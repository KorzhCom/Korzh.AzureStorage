using System;
using Korzh.WindowsAzure.Storage;

namespace Microsoft.Extensions.DependencyInjection
{
    public static class AzureStorageExtensions
    {
        public static void AddAzureStorageContext(this IServiceCollection services, Action<AzureStorageOptions> setupAction) {
            if (setupAction == null) {
                throw new ArgumentNullException(nameof(setupAction));
            }
            services.AddSingleton<DefaultAzureStorageContext>((serviceProvider) => {
                AzureStorageOptions options = new AzureStorageOptions();
                setupAction(options);
                return new DefaultAzureStorageContext(options.ConnectionString);
            });
        }


        public static void AddAzureStorageContext<TAzureStorageContext>(this IServiceCollection services, Action<AzureStorageOptions> setupAction)
                    where TAzureStorageContext : AzureStorageContext
        {
            if (setupAction == null) {
                throw new ArgumentNullException(nameof(setupAction));
            }

            services.AddSingleton<TAzureStorageContext>((serviceProvider) => {
                AzureStorageOptions options = new AzureStorageOptions();
                setupAction(options);
                return (TAzureStorageContext)Activator.CreateInstance(typeof(TAzureStorageContext), new object[] { options.ConnectionString });
            });
    }

    }
}
