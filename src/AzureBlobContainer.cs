using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Korzh.WindowsAzure.Storage {

    public class AzureBlobContainer {
        protected CloudBlobClient Client { get; private set; }

        public CloudBlobContainer Container { get; private set; }

        private string _containerName;

        public AzureBlobContainer(AzureStorageContext context, string containerName) {
            this._containerName = containerName;
            InitilizeInternal(context);
        }

        private void InitilizeInternal(AzureStorageContext context) {
            Client = context.GetBlobClient();

            Container = Client.GetContainerReference(_containerName);
            Container.CreateIfNotExistsAsync().Wait();
        }

        public Task CreateContainerIfNotExistsAsync() {
            return Container.CreateIfNotExistsAsync();
        }

        public async Task<IEnumerable<IListBlobItem>> ListAllBlobs() {
            BlobContinuationToken token = null;
            var result = new List<IListBlobItem>();
            do {
                BlobResultSegment resultSegment = await Container.ListBlobsSegmentedAsync(token);
                token = resultSegment.ContinuationToken;
                result.AddRange(resultSegment.Results);
            }
            while (token != null);
            return result;
        }

        public async Task<string> DownloadStringAsync(string blockName) {
            CloudBlockBlob blockBlob = Container.GetBlockBlobReference(blockName);
            if (await blockBlob.ExistsAsync())
                return await blockBlob.DownloadTextAsync();
            return null;
        }

        public Task UploadStringAsync(string blockName, string data, string contentType = "text/plain") {
            CloudBlockBlob blockBlob = Container.GetBlockBlobReference(blockName);
            blockBlob.Properties.ContentType = contentType;
            return blockBlob.UploadTextAsync(data);
        }
        

        public async Task DeleteAsync(string name) {
            CloudBlockBlob blockBlob = Container.GetBlockBlobReference(name);  
            if (await blockBlob.ExistsAsync())          
                await  blockBlob.DeleteAsync();
        }

        public async Task DownloadToStreamAsync(string blockName, Stream stream) {
            CloudBlockBlob blockBlob = Container.GetBlockBlobReference(blockName);
            if (await blockBlob.ExistsAsync()) {
                await blockBlob.DownloadToStreamAsync(stream);
            }
        }

        public Task UploadFromStreamAsync(string blockName, Stream stream, string contentType = null) {
            CloudBlockBlob blockBlob = Container.GetBlockBlobReference(blockName);
            if (contentType != null) {
                blockBlob.Properties.ContentType = contentType;
            }
            return blockBlob.UploadFromStreamAsync(stream);
        }

        public async Task<byte[]> DownloadBytesAsync(string blockName) {
            CloudBlockBlob blockBlob = Container.GetBlockBlobReference(blockName);
            if (await blockBlob.ExistsAsync()) {
                using (var ms = new MemoryStream()) {
                    await blockBlob.DownloadToStreamAsync(ms);
                    ms.Position = 0;
                    return ms.ToArray();
                }
            }
            return null;
        }

        public Task UploadBytesAsync(string blockName, byte[] data, string contentType = null) {
            CloudBlockBlob blockBlob = Container.GetBlockBlobReference(blockName);
            if (contentType != null) {
                blockBlob.Properties.ContentType = contentType;
            }
            return blockBlob.UploadFromByteArrayAsync(data, 0, data.Length);
        }

        public async Task<Stream> OpenReadStreamAsync(string blockName) {
            CloudBlockBlob blockBlob = Container.GetBlockBlobReference(blockName);
            return await blockBlob.OpenReadAsync();
        }
    }
}
