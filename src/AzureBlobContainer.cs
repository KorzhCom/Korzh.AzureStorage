﻿using Microsoft.WindowsAzure.Storage.Blob;
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

        protected void CheckContainer() {
            if (Container == null) {
                throw new InvalidOperationException("Container is null (possibly deleted?)");
            }
        }

        public async Task<IEnumerable<IListBlobItem>> ListAllBlobs() {
            CheckContainer();
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
            CheckContainer();
            CloudBlockBlob blockBlob = Container.GetBlockBlobReference(blobName);
            if (await blockBlob.ExistsAsync())
                return await blockBlob.DownloadTextAsync();
            return null;
        }

        public Task UploadStringAsync(string blobName, string data, string contentType = "text/plain") {
            CheckContainer();
            CloudBlockBlob blockBlob = Container.GetBlockBlobReference(blobName);
            blockBlob.Properties.ContentType = contentType;
            return blockBlob.UploadTextAsync(data);
        }
        

        public async Task<bool> DeleteBlobAsync(string blobName) {
            CheckContainer();
            CloudBlockBlob blockBlob = Container.GetBlockBlobReference(blobName);
            return await blockBlob.DeleteIfExistsAsync();
        }

        public async Task<bool> DeleteContainerAsync() {
            CheckContainer();
            var result = await Container.DeleteIfExistsAsync();
            if (result) {
                Container = null;
            }
            return result;
        }

        public async Task DownloadToStreamAsync(string blockName, Stream stream) {
            CheckContainer();
            CloudBlockBlob blockBlob = Container.GetBlockBlobReference(blockName);
            if (await blockBlob.ExistsAsync()) {
                await blockBlob.DownloadToStreamAsync(stream);
            }
        }

        public Task UploadFromStreamAsync(string blobName, Stream stream, string contentType = null) {
            CheckContainer();
            CloudBlockBlob blockBlob = Container.GetBlockBlobReference(blobName);
            if (contentType != null) {
                blockBlob.Properties.ContentType = contentType;
            }
            return blockBlob.UploadFromStreamAsync(stream);
        }

        public async Task<byte[]> DownloadBytesAsync(string blobName) {
            CheckContainer();
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

        public Task UploadBytesAsync(string blobName, byte[] data, string contentType = null) {
            CheckContainer();
            CloudBlockBlob blockBlob = Container.GetBlockBlobReference(blobName);
            if (contentType != null) {
                blockBlob.Properties.ContentType = contentType;
            }
            return blockBlob.UploadFromByteArrayAsync(data, 0, data.Length);
        }

        public async Task<Stream> OpenReadStreamAsync(string blobName) {
            CheckContainer();
            CloudBlockBlob blockBlob = Container.GetBlockBlobReference(blobName);
            return await blockBlob.OpenReadAsync();
        }

        public async Task<Stream> OpenWriteStreamAsync(string blobName) {
            CheckContainer();
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
