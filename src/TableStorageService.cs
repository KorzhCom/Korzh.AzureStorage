using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using System.Text;

namespace Korzh.WindowsAzure.Storage
{
	public class TableStorageService<T>  where T : ITableEntity, new()
	{
		public const string PartitionKey = "PartitionKey";
		public const string RowKey = "RowKey";
		public const byte MaxOperationsInBatch = 100;
		public const byte MaxFiltersConditions = 15;

		protected CloudTableClient Client { get; private set; }


		public CloudTable Table { get; private set; }

		protected string TableName { get; private set; }

        public TableStorageService(AzureStorageContext context) 
            : this(context, typeof(T).Name + "Table") 
        {
        }


        public TableStorageService(AzureStorageContext context, string tableName)  {
            TableName = tableName;
            InitilizeInternal(context);
        }



        private void InitilizeInternal(AzureStorageContext context) {
			Client = context.Account.CreateCloudTableClient();
			Table = Client.GetTableReference(TableName);
            CreateTableIfNotExistsAsync().Wait();
        }

		private string GenerateFilterCondition(string key, object value) {
			if (value is string) {
				return TableQuery.GenerateFilterCondition(key, QueryComparisons.Equal, (string)value);
			}

			if (value is Guid) {
				return TableQuery.GenerateFilterConditionForGuid(key, QueryComparisons.Equal, (Guid)value);
			}

			if (value is byte || value is short || value is int) {
				return TableQuery.GenerateFilterConditionForInt(key, QueryComparisons.Equal, (int)value);
			}

			if (value is long) {
				return TableQuery.GenerateFilterConditionForLong(key, QueryComparisons.Equal, (long)value);
			}

			if (value is bool) {
				return TableQuery.GenerateFilterConditionForBool(key, QueryComparisons.Equal, (bool)value);
			}

			if (value is DateTimeOffset) {
				return TableQuery.GenerateFilterConditionForDate(key, QueryComparisons.Equal, (DateTimeOffset)value);
			}

			if (value is DateTime) {
				return TableQuery.GenerateFilterConditionForDate(key, QueryComparisons.Equal, new DateTimeOffset((DateTime)value));
			}

			if (value is float || value is double) {
				return TableQuery.GenerateFilterConditionForDouble(key, QueryComparisons.Equal, (double)value);
			}

			throw new NotSupportedException();
		}

		public Task CreateTableIfNotExistsAsync() {
            return Table.CreateIfNotExistsAsync();
		}

        public async Task<T> GetEntityByFilterAsync(string filter) {
            var query = new TableQuery<T>().Where(filter).Take(1);
            var items = await Table.ExecuteQuerySegmentedAsync(query,null);
            return items.FirstOrDefault();
        }

		public async Task<T> GetEntityByKeysAsync(string partitionKey, string rowKey) {
			var operation = TableOperation.Retrieve<T>(partitionKey, rowKey);           
			return (T)(await Table.ExecuteAsync(operation)).Result;
		}

		public Task<IEnumerable<T>> GetEntitiesByPartitionKeyAsync(string partitionKey) {
			return ListEntitiesByFilterAsync(new List<KeyValuePair<string, object>> {
				new KeyValuePair<string, object>(PartitionKey, partitionKey)
			});
		}

		public async Task<IEnumerable<T>> ListEntitiesAsync(string filter = null) {
			return await GetEntitiesByFilterAsync(filter);
		}

		public Task<IEnumerable<T>> ListEntitiesByFilterAsync(IList<KeyValuePair<string, object>> filters, IList<string> columns = null, int? rowsLimit = null) {
			if (filters != null && filters.Count > 0) {
				var filterString = GenerateFilterCondition(filters[0].Key, filters[0].Value);

				for (int i = 1; i < filters.Count; i++) {
					var filterNew = GenerateFilterCondition(filters[i].Key, filters[i].Value);

					filterString = TableQuery.CombineFilters(filterString, TableOperators.And, filterNew);
				}

				return GetEntitiesByFilterAsync(filterString, columns, rowsLimit);
			}

			return GetEntitiesByFilterAsync(string.Empty, columns, rowsLimit);
		}

        public Task<IEnumerable<T>> ListEntitiesByFilterAsync(IDictionary<string, object> filters, IList<string> columns = null, int? rowsLimit = null) {
            if (filters != null && filters.Count > 0) {
                StringBuilder filterStr = new StringBuilder();

                foreach (var entry in filters) {
                    if (filterStr.Length > 0) {
                        filterStr.Append(" " + TableOperators.And + " ");
                    }
                    filterStr.Append(GenerateFilterCondition(entry.Key, entry.Value));
                }

                return GetEntitiesByFilterAsync(filterStr.ToString(), columns, rowsLimit);
            }

            return GetEntitiesByFilterAsync(string.Empty, columns, rowsLimit);
        }

        public async Task<IEnumerable<T>> GetEntitiesByFilterAsync(string filters = null, IList<string> columns = null, int? rowsLimit = null) {
			var query = new TableQuery<T>();

			if (!string.IsNullOrEmpty(filters)) {
				query = query.Where(filters);
			}

			if (columns != null && columns.Any()) {
				query = query.Select(columns);
			}

			if (rowsLimit.HasValue) {
				query = query.Take(rowsLimit.Value);
			}

            TableContinuationToken token = null;
            var results = new List<T>();
            do {
                var seg = await Table.ExecuteQuerySegmentedAsync(query, token);
                token = seg.ContinuationToken;
                results.AddRange(seg.Results);
            }
            while (token != null);
            return results; 
		}

		public async Task<T> InsertEntityAsync(T entity) {
			var operation = TableOperation.Insert(entity);

			return (T)(await Table.ExecuteAsync(operation)).Result;
		}

		public async Task<T> InsertOrUpdateEntityAsync(T entity) {
			var operation = TableOperation.InsertOrReplace(entity);

			return (T)(await Table.ExecuteAsync(operation)).Result;
		}

        public Task ReplaceAsync(T entity) { 
            entity.ETag = "*";
            var operation = TableOperation.Replace(entity);
            return Table.ExecuteAsync(operation);
        }

        public async Task<T> InsertOrMergeEntityAsync(T entity) {
			var operation = TableOperation.InsertOrMerge(entity);

			return (T)(await Table.ExecuteAsync(operation)).Result;
		}

		public async Task<T> ReplaceEntityAsync(T entity) {
			var operation = TableOperation.Replace(entity);

			return (T)(await Table.ExecuteAsync(operation)).Result;
		}

		public async Task<T> MergeEntityAsync(T entity) {
			var operation = TableOperation.Merge(entity);

			return (T)(await Table.ExecuteAsync(operation)).Result;
		}

		public Task DeleteEntityAsync(T entity) {
            entity.ETag = "*";
			var operation = TableOperation.Delete(entity);

			return Table.ExecuteAsync(operation);
		}
	}
}
