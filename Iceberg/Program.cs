﻿//-----------------------------------------------------------------------
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
using System.Threading.Tasks;

namespace Iceberg
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                Console.WriteLine("iceberg list <container> <blobname> : Lists blob versions for this blob");
                Console.WriteLine("iceberg upload <local path> <container> <blobname> : Uploads/updates blob ");
                Console.WriteLine("iceberg download <local path> <container> <blobname> : Downloads blob. Can be version specific");
                Console.WriteLine("iceberg prune <container> <blobname> <number of versions to keep>: Removes unwanted backups");

                return;
            }

            var core = new IcebergCore();

            switch( args[0])
            {
                case "list":
                    core.ListBlobs(args[1], args[2], args[3]);
                    break;
                case "upload":
                    core.UpdateCloudBlob(args[1], args[2], args[3], 1);
                    break;
                case "download":
                    core.DownloadCloudBlob(args[1], args[2], args[3]);
                    break;

                default:
                    Console.WriteLine("Invalid arguments");
                    break;
            }
        }
    }
}
