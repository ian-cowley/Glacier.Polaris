using System;
using System.Data.Common;

namespace Glacier.Polaris.IO
{
    /// <summary>
    /// Identifies the SQL dialect / provider used when constructing a <see cref="SqlReader"/>.
    /// </summary>
    public enum SqlDialect
    {
        /// <summary>
        /// Microsoft SQL Server via <c>Microsoft.Data.SqlClient</c>.
        /// </summary>
        SqlServer,

        /// <summary>
        /// PostgreSQL via <c>Npgsql</c>.
        /// </summary>
        PostgreSql,

        /// <summary>
        /// Any ADO.NET-compatible provider supplied directly as a <see cref="DbConnection"/>.
        /// Use this for SQLite, Oracle, MySQL, etc.
        /// </summary>
        Generic
    }

    internal static class SqlDialectFactory
    {
        /// <summary>
        /// Creates an open <see cref="DbConnection"/> for the given dialect and connection string.
        /// Returns <c>null</c> for <see cref="SqlDialect.Generic"/> — the caller must supply its own connection.
        /// </summary>
        public static DbConnection Create(SqlDialect dialect, string connectionString) => dialect switch
        {
            SqlDialect.SqlServer  => CreateSqlServer(connectionString),
            SqlDialect.PostgreSql => CreatePostgres(connectionString),
            _ => throw new NotSupportedException(
                    $"Use the DbConnection overload for dialect '{dialect}'.")
        };

        private static DbConnection CreateSqlServer(string cs)
        {
            var conn = new Microsoft.Data.SqlClient.SqlConnection(cs);
            return conn;
        }

        private static DbConnection CreatePostgres(string cs)
        {
            var conn = new Npgsql.NpgsqlConnection(cs);
            return conn;
        }

        /// <summary>
        /// Wraps a query with a column projection clause appropriate for the dialect.
        /// When <paramref name="columns"/> is null/empty the original query is returned unchanged.
        /// </summary>
        public static string ApplyProjection(SqlDialect dialect, string query, string[]? columns)
        {
            if (columns == null || columns.Length == 0) return query;
            string quote = dialect == SqlDialect.SqlServer ? "[{0}]" : "\"{0}\"";
            string cols = string.Join(", ", Array.ConvertAll(columns, c => string.Format(quote, c)));
            return $"SELECT {cols} FROM ({query}) __ag_sub";
        }

        /// <summary>
        /// Wraps a query with a TOP/LIMIT clause appropriate for the dialect.
        /// </summary>
        public static string ApplyLimit(SqlDialect dialect, string query, int? limit)
        {
            if (limit == null) return query;
            return dialect switch
            {
                SqlDialect.SqlServer  => $"SELECT TOP {limit} * FROM ({query}) __ag_sub",
                SqlDialect.PostgreSql => $"{query} LIMIT {limit}",
                _                     => $"{query} LIMIT {limit}"
            };
        }
    }
}
