using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;


namespace Korzh.WindowsAzure.Storage
{

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
	}

    public class DefaultAzureStorageContext  : AzureStorageContext
    {
        public DefaultAzureStorageContext(string connectionString) : base(connectionString) { }
    }

    public class AzureStorageOptions {
        public string ConnectionString { get; set; } = "UseDevelopmentStorage=true";
    }


}
