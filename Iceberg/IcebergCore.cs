using Microsoft.WindowsAzure.Storage.Blob;
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
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Iceberg
{
    class IcebergCore
    {
        static string VersionKey = "VERSION";

        /// <summary>
        /// Upload/update container/blob in Azure storage. 
        /// Will make a copy of the previous version uploaded then attempt to update it 
        /// using the BlobSync library.
        /// </summary>
        /// <param name="filePath"></param>
        /// <param name="versionsToKeep"></param>
        public void UpdateCloudBlob( string filePath, string containerName, string blobName, int numberOfVersionsToKeep)
        {
            // Check if blob already exists
            if (AzureHelper.DoesBlobExist( containerName, blobName))
            {
                // can update it.
                // if blob exists, get name of latest version
                var cloudClient = AzureHelper.GetCloudBlobClient();
                var referenceBlob = cloudClient.GetBlobReferenceFromServer( new Uri( AzureHelper.GenerateUrl(containerName, blobName)));

                // gets name for new blob.
                // will need to wait for async blob copy to complete.
                var newBlobName = CopyBlobToNewVersion(cloudClient, referenceBlob);

                // wait until complete
                MonitorASyncBlobCopy(containerName, newBlobName, 120); // 2 min timeout?

                // use blobsync to update this new latest.
                var blobSyncClient = new BlobSync.AzureOps();
                blobSyncClient.UploadFile(containerName, newBlobName, filePath);

                // determine how many versions of the blob exist, prune them down to "numberOfVersionsToKeep"

            }
            else
            {
                // doesnt exist, nothing tricky to do just upload the sucker.

            }
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
                var filteredBlobList = (from b in blobList where (((ICloudBlob)b).CopyState != null) && (((ICloudBlob)b).CopyState.Status ==                        CopyStatus.Pending) select b.Uri.AbsolutePath).ToList<string>();

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

            if (operationTime > operationExpireTime)
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
        private string CopyBlobToNewVersion(CloudBlobClient blobClient, ICloudBlob referenceBlob)
        {
            int latestVersionNumber = GetLatestVersionNumber(referenceBlob);
            var nextVersion = latestVersionNumber + 1;

            var existingBlobUri = referenceBlob.Uri;
            var newBlobName = string.Format("{0}.{1}",referenceBlob.Name, nextVersion);
            var newBlobUrl = AzureHelper.GenerateUrl( referenceBlob.Container.Name, newBlobName);
            var newBlob = blobClient.GetBlobReferenceFromServer( new Uri( newBlobUrl));
            newBlob.StartCopyFromBlob(existingBlobUri);

            return newBlobName;
        }

        public void UpdateLocalFile( string containerName, string blobName, string localFilePath)
        {
            
        }

        // Updates the blob version in the original blob, and returns the 
        private void UpdateLatestVersionNumber( ICloudBlob referenceBlob, int versionNo)
        {
            referenceBlob.FetchAttributes();
            referenceBlob.Metadata[VersionKey] = versionNo.ToString();
            referenceBlob.SetMetadata();
        }

        private int GetLatestVersionNumber(ICloudBlob referenceBlob)
        {
            referenceBlob.FetchAttributes();
            var latestVersion = 0; // 0 == doesn't exist.

            if (referenceBlob.Metadata.ContainsKey(VersionKey))
            {
                latestVersion = Convert.ToInt32(referenceBlob.Metadata[VersionKey] );
            }

            return latestVersion;
        }
    }
}
