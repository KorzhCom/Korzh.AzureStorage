﻿using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Korzh.WindowsAzure.Storage {

    public class BlobStorageService : AzureStorageContext {
        protected CloudBlobClient Client { get; private set; }

        public CloudBlobContainer Container { get; private set; }

        private string containerName;

        public BlobStorageService(string connectionString, string containerName)
            : base(connectionString) 
        {
            this.containerName = containerName;
            InitilizeInternal();
        }

        private void InitilizeInternal() {
            Client = Account.CreateCloudBlobClient();

            Container = Client.GetContainerReference(containerName);
            Container.CreateIfNotExistsAsync();
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

        public Task UploadStringAsync(string blockName, string data) {
            CloudBlockBlob blockBlob = Container.GetBlockBlobReference(blockName);               
            return blockBlob.UploadTextAsync(data);
        }
        

        public async Task DeleteAsync(string name) {
            CloudBlockBlob blockBlob = Container.GetBlockBlobReference(name);  
            if (await blockBlob.ExistsAsync())          
                await  blockBlob.DeleteAsync();
        }

        public async Task DownloadToStreamAsync(string blockName, Stream stream) {
            CloudBlockBlob blockBlob = Container.GetBlockBlobReference(blockName);
            if (await blockBlob.ExistsAsync())            
                await blockBlob.DownloadToStreamAsync(stream);                                     
        }

        public Task UploadFromStreamAsync(string blockName, Stream stream) {
            CloudBlockBlob blockBlob = Container.GetBlockBlobReference(blockName);
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

        public Task UploadBytesAsync(string blockName, byte[] data) {
            CloudBlockBlob blockBlob = Container.GetBlockBlobReference(blockName);
            return blockBlob.UploadFromByteArrayAsync(data, 0, data.Length);
        }

        public async Task<Stream> OpenReadStreamAsync(string blockName) {
            CloudBlockBlob blockBlob = Container.GetBlockBlobReference(blockName);
            return await blockBlob.OpenReadAsync();
        }
    }
}
