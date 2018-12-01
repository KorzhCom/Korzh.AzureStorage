# Korzh.AzureStorage
Useful .NET Core extensions for more convenient working with Azure Storage (table and blobs) in ASP.NET Core project.

This package introduces the concept of AzureContext with a similar meaning as DbContext for Entity Framework.

Each object of AzureContext class (and its dependencies) reprsents one connection to some Azure Storage account. Once configured the program's start it can be then injected via DI to any other class (controller, service, etc) in your your project.

For example, here is how to add azure context to the dependency injection container in your Startup class:

```
        public void ConfigureServices(IServiceCollection services) {
           
            services.AddAzureStorageContext<FilesAzureStorageContext>(options => {
                options.ConnectionString = Configuration.GetConnectionString("FilesAzureStorage");
            });
            .    .    .    .    .    .
        }
```

Then, the context can be injected in any service:

```
    public class SomeService : ISomeService {

        private readonly AzureTable<SomeStorageEntity>  _storageTable;
        
        public SomeService(AzureStorageContext azureContext) {
            _storageTable = new AzureTable<SomeStorageEntity>(azureContext, "SomeTable");
        }
        
        .    .    .    .    .   
```
