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

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Iceberg
{
    static class AzureHelper
    {
        const string AzureBaseUrl = "blob.core.windows.net";
        static CloudBlobClient BlobClient { get; set; }
        static string azureAccountName;
        static string azureAccountKey;
        static bool isDev = false;

        // get all the configs.
        static AzureHelper()
        {
            // if we dont have keys, we're dev.
            try
            {
                azureAccountName = ConfigurationManager.AppSettings["AzureAccountName"];
                azureAccountKey = ConfigurationManager.AppSettings["AzureAccountKey"];
            }
            catch(Exception)
            {
                isDev = true;
            }
        }

        public static bool DoesBlobExist(string container, string blobName)
        {
            var exists = false;
            try
            {
                var client = GetCloudBlobClient();
                var url = GenerateUrl(container, blobName);
                var blob = client.GetBlobReferenceFromServer(new Uri(url));
               
                if (blob != null)
                    exists = true;
            }
            catch (Exception)
            {

            }

            return exists;
        }

        internal static string GenerateUrl(string containerName, string blobName)
        {
            return string.Format("https://{0}.{1}/{2}/{3}", azureAccountName, AzureBaseUrl, containerName, blobName);
        }

        public static CloudBlobClient GetCloudBlobClient()
        {
            if (BlobClient == null)
            {
                if (isDev)
                {
                    CloudStorageAccount storageAccount = CloudStorageAccount.DevelopmentStorageAccount;
                    BlobClient = storageAccount.CreateCloudBlobClient();
                }
                else
                {
                    var credentials = new Microsoft.WindowsAzure.Storage.Auth.StorageCredentials(azureAccountName, azureAccountKey);
                    CloudStorageAccount azureStorageAccount = new CloudStorageAccount(credentials, true);
                    BlobClient = azureStorageAccount.CreateCloudBlobClient();
                }
            }

            return BlobClient;
        }

    }
}
