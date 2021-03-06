﻿using Microsoft.WindowsAzure.Storage.Blob;
//-----------------------------------------------------------------------
// <copyright >
//    Copyright 2014 Ken Faulkner
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//      http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Iceberg
{
    class IcebergCore
    {
        const string SigUrl = "sigurl";
        const string SigCount = "sigcount";
        const string LatestVersion = "latest";

        /// <summary>
        /// Upload/update container/blob in Azure storage. 
        /// Will make a copy of the previous version uploaded then attempt to update it 
        /// using the BlobSync library.
        /// </summary>
        /// <param name="filePath"></param>
        /// <param name="versionsToKeep"></param>
        public void UpdateCloudBlob( string filePath, string containerName, string blobName, int numberOfVersionsToKeep)
        {
            var latestVersion = 0;

            var sw = new Stopwatch();
            sw.Start();

            // check that uploaded file and local file aren't already identical (MD5)
            var fileMD5 = GetFileMD5(filePath);
            var blobMD5 = AzureHelper.GetBlobMD5(containerName, blobName);
            long bytesUploaded = 0;

            if (fileMD5 != blobMD5)
            {
                // Check if blob already exists
                if (AzureHelper.DoesBlobExist(containerName, blobName))
                {

                    // can update it.
                    // if blob exists, get name of latest version
                    var cloudClient = AzureHelper.GetCloudBlobClient();
                    var baseReferenceBlob = cloudClient.GetBlobReferenceFromServer(new Uri(AzureHelper.GenerateUrl(containerName, blobName)));

                    // signature info from existing blob.
                    latestVersion = GetLatestVersionNumber(baseReferenceBlob);
                    var sigCount = Convert.ToInt32(baseReferenceBlob.Metadata[SigCount]);
                    var sigUrl = baseReferenceBlob.Metadata[SigUrl];
                    latestVersion++;
                    var renamedBlobName = string.Format("{0}.v{1}", baseReferenceBlob.Name, latestVersion);

                    // copy existing blob to new name (latestVersion+1)
                    // will need to wait for async blob copy to complete.
                    CopyBlobToNewVersion(cloudClient, baseReferenceBlob, renamedBlobName);
                    MonitorASyncBlobCopy(containerName, renamedBlobName, 120); // 2 min timeout?

                    // copy existing sig to new name
                    var sigReferenceBlob = cloudClient.GetBlobReferenceFromServer(new Uri(AzureHelper.GenerateUrl(containerName, sigUrl)));
                    var newSigReferenceBlobName = string.Format("{0}.{1}.sig", renamedBlobName, sigCount);
                    CopyBlobToNewVersion(cloudClient, sigReferenceBlob, newSigReferenceBlobName);
                    MonitorASyncBlobCopy(containerName, newSigReferenceBlobName, 120); // 2 min timeout?

                    // modify "renamed blob" to have correct sig metadata.
                    UpdateSignatureDetails(containerName, renamedBlobName, newSigReferenceBlobName, sigCount);

                }

                // use blobsync to update this new latest.
                var blobSyncClient = new BlobSync.AzureOps();
                bytesUploaded = blobSyncClient.UploadFile(containerName, blobName, filePath);

                // set latest version metadata.
                UpdateLatestVersionNumber(containerName, blobName, latestVersion);
            }

            sw.Stop();

            Console.WriteLine("Uploaded {0} bytes, took {1}ms", bytesUploaded, sw.ElapsedMilliseconds);
        }

        private string GetFileMD5(string localFilePath)
        {
            var md5Hash = MD5.Create();
            byte[] hashByteArray;
            using (var fs = new FileStream(localFilePath, FileMode.Open))
            {
                hashByteArray = md5Hash.ComputeHash(fs);
            }

            return Convert.ToBase64String(hashByteArray);
        }


        private void UpdateSignatureDetails(string containerName, string newBlobName, string newSigReferenceBlobName, int sigCount)
        {
            var cloudClient = AzureHelper.GetCloudBlobClient();
            var newBlobUrl = AzureHelper.GenerateUrl( containerName, newBlobName);
            var newBlob = cloudClient.GetBlobReferenceFromServer(new Uri(newBlobUrl));

            newBlob.FetchAttributes();
            newBlob.Metadata[SigCount] = sigCount.ToString();
            newBlob.Metadata[SigUrl] = newSigReferenceBlobName;

            newBlob.SetMetadata();

        }

        public static void MonitorASyncBlobCopy(string containerName, string blobName, int timeoutSeconds)
        {
            var cloudClient = AzureHelper.GetCloudBlobClient();

            var container = cloudClient.GetContainerReference(containerName);

            var operationTime = DateTime.UtcNow;
            var completedCopy = false;
            var operationExpireTime = DateTime.UtcNow.AddSeconds(timeoutSeconds);
            while (!completedCopy && operationTime < operationExpireTime)
            {
                // use entire name as prefix? Valid? FIXME
                var blobList = container.ListBlobs(blobName);
                var filteredBlobList = (from b in blobList where (((ICloudBlob)b).CopyState != null) && (((ICloudBlob)b).CopyState.Status == CopyStatus.Pending) select b.Uri.AbsolutePath).ToList<string>();

                // has any, then hasn't completed/aborted yet.
                if (!filteredBlobList.Any())
                {
                    completedCopy = true;
                }
                else
                {
                    // ugly ugly...  should somehow make this properly async.
                    // but will do for now.
                    Thread.Sleep(1000);
                }
                operationTime = DateTime.UtcNow;
            }

            if (!completedCopy)
            {
                // expired... throw exception
                throw new TimeoutException("Copying operation taking too long");
            }
        }

        /// <summary>
        /// Finds latest version of blob. Copies it to new version.
        /// returns new blob name
        /// </summary>
        /// <param name="referenceBlob"></param>
        private string CopyBlobToNewVersion(CloudBlobClient blobClient, ICloudBlob referenceBlob, string newBlobName)
        {
            var newBlobUrl = AzureHelper.GenerateUrl( referenceBlob.Container.Name, newBlobName);
            var existingBlobUri = referenceBlob.Uri;
            var container = blobClient.GetContainerReference(referenceBlob.Container.Name);
            var newBlob = container.GetBlockBlobReference(newBlobName);

            //var newBlob = blobClient.GetBlobReferenceFromServer( new Uri( newBlobUrl));
            newBlob.StartCopyFromBlob(existingBlobUri);
            return newBlobName;
        }

        public void UpdateLocalFile( string containerName, string blobName, string localFilePath)
        {
            
        }

        // Updates the blob version in the original blob, and returns the 
        private void UpdateLatestVersionNumber( string containerName, string blobName, int versionNo)
        {
            var cloudClient = AzureHelper.GetCloudBlobClient();

            var referenceBlob = cloudClient.GetBlobReferenceFromServer(new Uri(AzureHelper.GenerateUrl(containerName, blobName)));
            referenceBlob.FetchAttributes();
            referenceBlob.Metadata[LatestVersion] = versionNo.ToString();
            referenceBlob.SetMetadata();
        }

        private int GetLatestVersionNumber(ICloudBlob referenceBlob)
        {
            referenceBlob.FetchAttributes();
            var latestVersion = 0; // 0 == doesn't exist.

            if (referenceBlob.Metadata.ContainsKey(LatestVersion))
            {
                latestVersion = Convert.ToInt32(referenceBlob.Metadata[LatestVersion]);
            }

            return latestVersion;
        }

        internal List<CloudBlockBlob> GetListOfBlobs( string containerName, string blobName)
        {
            var cloudClient = AzureHelper.GetCloudBlobClient();
            var baseReferenceBlob = cloudClient.GetBlobReferenceFromServer(new Uri(AzureHelper.GenerateUrl(containerName, blobName)));

            var container = cloudClient.GetContainerReference(containerName);
            var blobList = container.ListBlobs(blobName);

            // convert to CloudBlockBlob list.
            // get blobs without signatures..
            var blockBlobList = blobList.Select(b => b as CloudBlockBlob).ToList();
            
            return blockBlobList;
        }

        internal void ListBlobs(string containerName, string blobName)
        {
            var blobList = GetListOfBlobs(containerName, blobName);

            // convert to CloudBlockBlob list.
            // get blobs without signatures..
            var blockBlobList = blobList.Where(bb => !bb.Name.EndsWith(".sig")).ToList();

            // get signature list.
            var existingSigNames = blobList.Where(bb => bb.Name.EndsWith(".sig")).Select( b => b.Name).ToList();

            var blobsToList = new List<string>();

            // loop through list of blobs that include blobName.v<number> AND AND AND has signature file.
            foreach (var blob in blockBlobList)
            {
                try
                {
                    // get attributes of blob.
                    blob.FetchAttributes();
                    var sigName = blob.Metadata[SigUrl];

                    if (existingSigNames.Contains( sigName))
                    {
                        blobsToList.Add(blob.Name + " " + blob.Properties.LastModified.ToString());
                    }
                }
                catch(Exception )
                {
                    // ugly swallow... but possibly no matching metadata.
                }
            }

            foreach(var b in blobsToList)
            {
                Console.WriteLine(b);
            }
        }

        internal void DownloadCloudBlob(string filePath, string containerName, string blobName)
        {
            // use blobsync to update this new latest.
            var blobSyncClient = new BlobSync.AzureOps();
            var bytesDownloaded = blobSyncClient.DownloadBlob(containerName, blobName, filePath);
            Console.WriteLine("Downloaded {0} bytes", bytesDownloaded);
        }

        internal void Prune(string containerName, string blobName, int numVersionsToKeep)
        {
            var blobList = GetListOfBlobs(containerName, blobName);
            var cloudBlobClient = AzureHelper.GetCloudBlobClient();
            var container = cloudBlobClient.GetContainerReference( containerName);

            // filter out blob with exact correct blobName
            var latestBlob = blobList.Where(b => b.Name == blobName).FirstOrDefault();
            blobList.Remove(latestBlob);

            // apart from blob with exactly "blobName" name, we want to delete all but the 
            // most recent "numVersionsToKeep". (and sigs).
            // loads of conversions here....   should I just go against "Latest" metadata? (means pulling that in though).
            var blockBlobList = blobList.Where(bb => !bb.Name.EndsWith(".sig")).OrderByDescending(b => Convert.ToInt32(b.Name.Split('.').Last().Substring(1)));
            foreach( var blob in blockBlobList.Skip(numVersionsToKeep))
            {
                blob.FetchAttributes();
                var sigBlobName = blob.Metadata[SigUrl];

                var sigBlob = container.GetBlobReferenceFromServer(sigBlobName);

                // is async... is good.
                sigBlob.DeleteAsync();
                blob.DeleteAsync();
            }
        }
    }
}
