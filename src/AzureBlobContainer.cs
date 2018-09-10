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
        }

        public Task CreateContainerIfNotExistsAsync() {
            return Container.CreateIfNotExistsAsync();
        }

        protected async Task CheckContainerAsync() {
            if (Container == null) {
                throw new InvalidOperationException("Container is null (possibly deleted?)");
            }

            if (!(await Container.ExistsAsync())) {
                await Container.CreateIfNotExistsAsync();
                Container = Client.GetContainerReference(_containerName);
            }
        }

        public async Task<IEnumerable<IListBlobItem>> ListAllBlobs() {
            await CheckContainerAsync();
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

        public async Task<string> DownloadStringAsync(string blobName) {
            await CheckContainerAsync();
            CloudBlockBlob blockBlob = Container.GetBlockBlobReference(blobName);
            if (await blockBlob.ExistsAsync())
                return await blockBlob.DownloadTextAsync();
            return null;
        }

        public async Task UploadStringAsync(string blobName, string data, string contentType = "text/plain") {
            await CheckContainerAsync();
            CloudBlockBlob blockBlob = Container.GetBlockBlobReference(blobName);
            blockBlob.Properties.ContentType = contentType;
            await blockBlob.UploadTextAsync(data);
        }
        

        public async Task<bool> DeleteBlobAsync(string blobName) {
            await CheckContainerAsync();
            CloudBlockBlob blockBlob = Container.GetBlockBlobReference(blobName);
            return await blockBlob.DeleteIfExistsAsync();
        }

        public async Task<bool> DeleteContainerAsync() {
            await CheckContainerAsync();
            var result = await Container.DeleteIfExistsAsync();
            if (result) {
                Container = null;
            }
            return result;
        }

        public async Task DownloadToStreamAsync(string blockName, Stream stream) {
            await CheckContainerAsync();
            CloudBlockBlob blockBlob = Container.GetBlockBlobReference(blockName);
            if (await blockBlob.ExistsAsync()) {
                await blockBlob.DownloadToStreamAsync(stream);
            }
        }

        public async Task UploadFromStreamAsync(string blobName, Stream stream, string contentType = null) {
            await CheckContainerAsync();
            CloudBlockBlob blockBlob = Container.GetBlockBlobReference(blobName);
            if (contentType != null) {
                blockBlob.Properties.ContentType = contentType;
            }
            await blockBlob.UploadFromStreamAsync(stream);
        }

        public async Task<byte[]> DownloadBytesAsync(string blobName) {
            await CheckContainerAsync();
            CloudBlockBlob blockBlob = Container.GetBlockBlobReference(blobName);
            if (await blockBlob.ExistsAsync()) {
                using (var ms = new MemoryStream()) {
                    await blockBlob.DownloadToStreamAsync(ms);
                    ms.Position = 0;
                    return ms.ToArray();
                }
            }
            return null;
        }

        public async Task UploadBytesAsync(string blobName, byte[] data, string contentType = null) {
            await CheckContainerAsync();
            CloudBlockBlob blockBlob = Container.GetBlockBlobReference(blobName);
            if (contentType != null) {
                blockBlob.Properties.ContentType = contentType;
            }
            await blockBlob.UploadFromByteArrayAsync(data, 0, data.Length);
        }

        public async Task<Stream> OpenReadStreamAsync(string blobName) {
            await CheckContainerAsync();
            CloudBlockBlob blockBlob = Container.GetBlockBlobReference(blobName);
            return await blockBlob.OpenReadAsync();
        }

        public async Task<Stream> OpenWriteStreamAsync(string blobName) {
            await CheckContainerAsync();
            CloudBlockBlob blockBlob = Container.GetBlockBlobReference(blobName);
            return await blockBlob.OpenWriteAsync();
        }

        public bool ContainerExists {
            get {
                return Container.ExistsAsync().Result;
            }
        }

        public bool BlobExists(string blobName) {
            var blockBlob = Container.GetBlockBlobReference(blobName);
            return blockBlob.ExistsAsync().Result;
        }
    }
}
