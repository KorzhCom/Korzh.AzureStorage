using System.IO;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using Korzh.WindowsAzure.Storage;


namespace Korzh.AzureStorage.Tests
{
    [TestClass]
    public class BlobTests
    {
        [TestMethod]
        public async Task Create_Container_Put_File_Read_Back()
        {
            var context = new DefaultAzureStorageContext("UseDevelopmentStorage=true");

            var blobContainer = new AzureBlobContainer(context, "test-container");

            string srcFileName = "easy-query256.png";
            await blobContainer.UploadFromStreamAsync(srcFileName, GetResourceAsStream(srcFileName));

            string destFileName = "__fileFromBlob.png";
            using (var fileFromBlob = new FileStream(destFileName, FileMode.Create)) {
                await blobContainer.DownloadToStreamAsync(srcFileName, fileFromBlob);
                fileFromBlob.Flush();
            }

            Assert.IsTrue(File.Exists(destFileName));

            await blobContainer.DeleteContainerAsync();
        }


        private static Stream GetResourceAsStream(string resourceFileName) {
            string fullName = "Korzh.AzureStorage.Tests.Resources." + resourceFileName;

            Assembly a = typeof(BlobTests).GetTypeInfo().Assembly;
            var resources = new List<string>(a.GetManifestResourceNames());
            if (resources.Contains(fullName))
                return a.GetManifestResourceStream(fullName);
            else
                return null;
        }
    }
}
