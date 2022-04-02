using Microsoft.Data.SqlClient;
using Pluralize.NET.Core;
using System.Data;
using System.Data.Common;
using System.Reflection;

namespace GenericAdoNet
{
    public abstract class GenericSqlClientRepo
    {
        protected DbConnection _connection;
        protected DbTransaction _transaction;
        Pluralizer _pluralizer = new Pluralizer();

        public GenericSqlClientRepo(string connectionString)
        {
            _connection = new SqlConnection(connectionString);
        }

        public GenericSqlClientRepo(DbConnection connection)
        {
            _connection = connection;
        }

        public DbConnection GetConnection()
        {
            return _connection;
        }

        public async Task CreateTransactionAsync(Func<DbConnection, DbTransaction> configure = null, CancellationToken token = default)
        {
            await _connection.OpenAsync(token);

            if (configure is not null)
            {
                _transaction = configure(_connection);
            } else
            {
                _transaction = await _connection.BeginTransactionAsync(token);
            }
        }

        public async Task CloseTransactionAsync(TransactionAction action = TransactionAction.Commit)
        {
            if (_connection is null || _connection.State == ConnectionState.Closed) return;

            switch (action)
            {
                case TransactionAction.Commit:
                    _transaction.Commit();
                    break;
                case TransactionAction.Rollback:
                    _transaction.Rollback();
                    break;
                default:
                    break;
            }

            await _connection.CloseAsync();
            await _connection.DisposeAsync();
        }

        public async Task<TEntity> GetAsync<TEntity, TKey>(string tableName, string columnName, TKey value, Action<SqlCommand> configure = null) 
            where TEntity : class 
            where TKey : IEquatable<TKey>
        {
            var command = new SqlCommand();
            command.Connection = (SqlConnection)_connection;

            var query = GetQuery<TEntity>();
            query += $" WHERE {columnName} = @id";

            command.CommandText = query;

            if (_connection.State != ConnectionState.Open) await _connection.OpenAsync();

            if (configure is not null) configure(command);

            using var reader = await command.ExecuteReaderAsync();

            var entity = FillEntity<TEntity>(reader);

            return entity;
        }

        string GetQuery<TEntity>()
            where TEntity : class
        {
            var entityType = typeof(TEntity);
            var table =  _pluralizer.Pluralize(entityType.Name);
            var columns = entityType.GetProperties().Select(prop => $"[{prop.Name}]").ToList();
            var columnsForQuery = string.Join(",", columns);
            var query = $"SELECT {columnsForQuery} FROM [{table}]";

            return query;
        }

        public async Task<TEntity> ExecuteQuerySingleAsync<TEntity>(string query, SqlParameter[] parameters, Action<SqlCommand> configure = null)
            where TEntity : class
        {
            var command = new SqlCommand();
            command.CommandType = CommandType.Text;
            command.CommandText = query;
            command.Connection = (SqlConnection)_connection;
            command.Parameters.AddRange(parameters);

            if (_transaction is not null) command.Transaction = (SqlTransaction)_transaction;

            if(_connection.State != ConnectionState.Open) await _connection.OpenAsync();

            if (configure is not null) configure(command);

            return await ExecuteQueryAsync<TEntity>(command);
        }

        async Task<TEntity> ExecuteQueryAsync<TEntity>(SqlCommand command)
        {
            using var reader = await command.ExecuteReaderAsync();

            var entity = FillEntity<TEntity>(reader);

            return entity;
        }

        TEntity FillEntity<TEntity>(SqlDataReader reader)
        {
            var entityProperties = typeof(TEntity).GetProperties();
            var availableColumnsInReader = reader.GetColumnSchema().Select(col => col.ColumnName).ToList();

            var entity = Activator.CreateInstance<TEntity>();

            foreach (var prop in entityProperties)
            {
                var columnName = prop.Name;

                if (!availableColumnsInReader.Contains(columnName)) continue;

                var value = reader[columnName];

                if (value == DBNull.Value)
                {
                    if (!IsNullable(prop.PropertyType)) throw new InvalidCastException($"Property {columnName} is not nullable.");
                    break;
                }

                prop.SetValue(entity, value);
            }

            return entity;
        }

        bool IsNullable(Type type) => type == typeof(Nullable<>) || type == typeof(string);
    }
}