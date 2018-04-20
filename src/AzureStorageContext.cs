﻿using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;

using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;

namespace Korzh.WindowsAzure.Storage
{

    /// <summary>
    /// Represents 
    /// </summary>
    public class AzureStorageContext
	{
		internal readonly CloudStorageAccount Account;

        public AzureStorageContext(string connectionString) {
			Account = CloudStorageAccount.Parse(connectionString);
		}

		public AzureStorageContext(string name, string key, bool isHttps = true) {
			var credentials = new StorageCredentials(name, key);

			Account = new CloudStorageAccount(credentials, isHttps);
		}

        private CloudBlobClient _blobClient;

        public CloudBlobClient GetBlobClient() {
            if (_blobClient == null) {
                _blobClient = Account.CreateCloudBlobClient();
            }
            return _blobClient;
        }


        private CloudTableClient _tableClient;

        public CloudTableClient GetTableClient() {
            if (_tableClient == null) {
                _tableClient = Account.CreateCloudTableClient();
            }
            return _tableClient;
        }
    }

    public class DefaultAzureStorageContext  : AzureStorageContext
    {
        public DefaultAzureStorageContext(string connectionString) : base(connectionString) { }
    }

    public class AzureStorageOptions {
        public string ConnectionString { get; set; } = "UseDevelopmentStorage=true";
    }


}
