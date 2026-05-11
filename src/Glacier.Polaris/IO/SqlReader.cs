using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.IO
{
    /// <summary>
    /// Streams SQL query results into <see cref="DataFrame"/> chunks using any ADO.NET provider.
    /// <para>
    /// Supports MSSQL (<see cref="SqlDialect.SqlServer"/>), PostgreSQL (<see cref="SqlDialect.PostgreSql"/>),
    /// and any other ADO.NET provider via the <see cref="DbConnection"/> overload
    /// (<see cref="SqlDialect.Generic"/>).
    /// </para>
    /// <para>
    /// Column projection and row limits are pushed into the SQL query text before execution,
    /// matching Polars' predicate/projection pushdown philosophy.
    /// </para>
    /// </summary>
    public sealed class SqlReader
    {
        private readonly string? _connectionString;
        private readonly DbConnection? _externalConnection;
        private readonly SqlDialect _dialect;
        private readonly string _query;
        private readonly string[]? _columns;
        private readonly int? _limit;
        private readonly int _chunkSize;

        // ── Connection-string constructors ───────────────────────────────────

        /// <summary>
        /// Read from a SQL Server or PostgreSQL connection string.
        /// </summary>
        public SqlReader(
            SqlDialect dialect,
            string connectionString,
            string query,
            string[]? columns = null,
            int? limit = null,
            int chunkSize = 10_000)
        {
            if (dialect == SqlDialect.Generic)
                throw new ArgumentException(
                    "Use the DbConnection overload for SqlDialect.Generic.", nameof(dialect));

            _dialect = dialect;
            _connectionString = connectionString;
            _query = query;
            _columns = columns;
            _limit = limit;
            _chunkSize = chunkSize;
        }

        // ── DbConnection constructor (Generic / SQLite / Oracle / …) ─────────

        /// <summary>
        /// Read using a caller-supplied open or closed <see cref="DbConnection"/>.
        /// The reader will open the connection if it is not already open, but will
        /// never close or dispose it — that responsibility stays with the caller.
        /// </summary>
        public SqlReader(
            DbConnection connection,
            string query,
            string[]? columns = null,
            int? limit = null,
            int chunkSize = 10_000)
        {
            _dialect = SqlDialect.Generic;
            _externalConnection = connection;
            _query = query;
            _columns = columns;
            _limit = limit;
            _chunkSize = chunkSize;
        }

        // ── Public API ───────────────────────────────────────────────────────

        public async IAsyncEnumerable<DataFrame> ReadAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation]
            CancellationToken cancellationToken = default)
        {
            DbConnection conn = _externalConnection ?? SqlDialectFactory.Create(_dialect, _connectionString!);
            bool ownsConnection = _externalConnection == null;

            try
            {
                if (conn.State != ConnectionState.Open)
                    await conn.OpenAsync(cancellationToken);

                string sql = BuildSql();

                await using var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                cmd.CommandTimeout = 120;

                await using var reader = await cmd.ExecuteReaderAsync(
                    CommandBehavior.SequentialAccess, cancellationToken);

                await foreach (var chunk in StreamChunksAsync(reader, cancellationToken))
                    yield return chunk;
            }
            finally
            {
                if (ownsConnection)
                    await conn.DisposeAsync();
            }
        }

        // ── Internal helpers ─────────────────────────────────────────────────

        private string BuildSql()
        {
            string sql = _query.TrimEnd(';', ' ', '\r', '\n');

            // Apply column projection before limit so both can nest correctly.
            sql = SqlDialectFactory.ApplyProjection(_dialect, sql, _columns);
            sql = SqlDialectFactory.ApplyLimit(_dialect, sql, _limit);

            return sql;
        }

        private async IAsyncEnumerable<DataFrame> StreamChunksAsync(
            DbDataReader reader,
            [System.Runtime.CompilerServices.EnumeratorCancellation]
            CancellationToken cancellationToken)
        {
            // Discover schema on first read.
            var schema = await reader.GetSchemaTableAsync(cancellationToken);
            int fieldCount = reader.FieldCount;

            // Build typed column accumulators.
            IColumnAccumulator[] accumulators = BuildAccumulators(reader, fieldCount);

            int rowsInChunk = 0;

            while (await reader.ReadAsync(cancellationToken))
            {
                for (int i = 0; i < fieldCount; i++)
                    accumulators[i].Append(reader, i);

                rowsInChunk++;

                if (rowsInChunk >= _chunkSize)
                {
                    yield return Flush(accumulators, rowsInChunk);
                    foreach (var acc in accumulators) acc.Reset();
                    rowsInChunk = 0;
                }
            }

            if (rowsInChunk > 0)
                yield return Flush(accumulators, rowsInChunk);
        }

        private static DataFrame Flush(IColumnAccumulator[] accumulators, int rowCount)
        {
            var series = new List<ISeries>(accumulators.Length);
            foreach (var acc in accumulators)
                series.Add(acc.BuildSeries(rowCount));
            return new DataFrame(series);
        }

        private static IColumnAccumulator[] BuildAccumulators(DbDataReader reader, int fieldCount)
        {
            var result = new IColumnAccumulator[fieldCount];
            for (int i = 0; i < fieldCount; i++)
            {
                string name = reader.GetName(i);
                Type clrType = reader.GetFieldType(i);
                result[i] = CreateAccumulator(name, clrType);
            }
            return result;
        }

        private static IColumnAccumulator CreateAccumulator(string name, Type clrType)
        {
            if (clrType == typeof(int)    || clrType == typeof(short) || clrType == typeof(byte))
                return new Int32Accumulator(name);
            if (clrType == typeof(long))
                return new Int64Accumulator(name);
            if (clrType == typeof(double) || clrType == typeof(float))
                return new Float64Accumulator(name);
            if (clrType == typeof(bool))
                return new BoolAccumulator(name);
            if (clrType == typeof(DateTime))
                return new DatetimeAccumulator(name);
            // Fallback: stringify everything else (including decimal, Guid, etc.)
            return new StringAccumulator(name);
        }

        // ── Accumulator interface & implementations ──────────────────────────

        private interface IColumnAccumulator
        {
            void Append(DbDataReader reader, int ordinal);
            ISeries BuildSeries(int rowCount);
            void Reset();
        }

        private sealed class Int32Accumulator : IColumnAccumulator
        {
            private readonly string _name;
            private readonly List<int> _values = new();
            private readonly List<bool> _nulls  = new();

            public Int32Accumulator(string name) => _name = name;

            public void Append(DbDataReader reader, int ordinal)
            {
                bool isNull = reader.IsDBNull(ordinal);
                _nulls.Add(isNull);
                _values.Add(isNull ? 0 : Convert.ToInt32(reader.GetValue(ordinal)));
            }

            public ISeries BuildSeries(int rowCount)
            {
                var s = new Int32Series(_name, rowCount);
                var span = s.Memory.Span;
                for (int i = 0; i < rowCount; i++)
                {
                    span[i] = _values[i];
                    if (_nulls[i]) s.ValidityMask.SetNull(i);
                }
                return s;
            }

            public void Reset() { _values.Clear(); _nulls.Clear(); }
        }

        private sealed class Int64Accumulator : IColumnAccumulator
        {
            private readonly string _name;
            private readonly List<long> _values = new();
            private readonly List<bool> _nulls   = new();

            public Int64Accumulator(string name) => _name = name;

            public void Append(DbDataReader reader, int ordinal)
            {
                bool isNull = reader.IsDBNull(ordinal);
                _nulls.Add(isNull);
                _values.Add(isNull ? 0L : reader.GetInt64(ordinal));
            }

            public ISeries BuildSeries(int rowCount)
            {
                var s = new Int64Series(_name, rowCount);
                var span = s.Memory.Span;
                for (int i = 0; i < rowCount; i++)
                {
                    span[i] = _values[i];
                    if (_nulls[i]) s.ValidityMask.SetNull(i);
                }
                return s;
            }

            public void Reset() { _values.Clear(); _nulls.Clear(); }
        }

        private sealed class Float64Accumulator : IColumnAccumulator
        {
            private readonly string _name;
            private readonly List<double> _values = new();
            private readonly List<bool>   _nulls  = new();

            public Float64Accumulator(string name) => _name = name;

            public void Append(DbDataReader reader, int ordinal)
            {
                bool isNull = reader.IsDBNull(ordinal);
                _nulls.Add(isNull);
                _values.Add(isNull ? 0d : Convert.ToDouble(reader.GetValue(ordinal)));
            }

            public ISeries BuildSeries(int rowCount)
            {
                var s = new Float64Series(_name, rowCount);
                var span = s.Memory.Span;
                for (int i = 0; i < rowCount; i++)
                {
                    span[i] = _values[i];
                    if (_nulls[i]) s.ValidityMask.SetNull(i);
                }
                return s;
            }

            public void Reset() { _values.Clear(); _nulls.Clear(); }
        }

        private sealed class BoolAccumulator : IColumnAccumulator
        {
            private readonly string _name;
            private readonly List<bool> _values = new();
            private readonly List<bool> _nulls  = new();

            public BoolAccumulator(string name) => _name = name;

            public void Append(DbDataReader reader, int ordinal)
            {
                bool isNull = reader.IsDBNull(ordinal);
                _nulls.Add(isNull);
                _values.Add(!isNull && reader.GetBoolean(ordinal));
            }

            public ISeries BuildSeries(int rowCount)
            {
                var s = new BooleanSeries(_name, rowCount);
                var span = s.Memory.Span;
                for (int i = 0; i < rowCount; i++)
                {
                    span[i] = _values[i];
                    if (_nulls[i]) s.ValidityMask.SetNull(i);
                }
                return s;
            }

            public void Reset() { _values.Clear(); _nulls.Clear(); }
        }

        private sealed class DatetimeAccumulator : IColumnAccumulator
        {
            private static readonly DateTime Epoch =
                new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            private readonly string _name;
            private readonly List<long> _values = new();
            private readonly List<bool> _nulls  = new();

            public DatetimeAccumulator(string name) => _name = name;

            public void Append(DbDataReader reader, int ordinal)
            {
                bool isNull = reader.IsDBNull(ordinal);
                _nulls.Add(isNull);
                if (!isNull)
                {
                    var dt = reader.GetDateTime(ordinal);
                    var utc = dt.Kind == DateTimeKind.Unspecified
                        ? DateTime.SpecifyKind(dt, DateTimeKind.Utc)
                        : dt.ToUniversalTime();
                    _values.Add((long)(utc - Epoch).TotalMicroseconds());
                }
                else _values.Add(0L);
            }

            public ISeries BuildSeries(int rowCount)
            {
                var s = new DatetimeSeries(_name, rowCount);
                var span = s.Memory.Span;
                for (int i = 0; i < rowCount; i++)
                {
                    span[i] = _values[i];
                    if (_nulls[i]) s.ValidityMask.SetNull(i);
                }
                return s;
            }

            public void Reset() { _values.Clear(); _nulls.Clear(); }
        }

        private sealed class StringAccumulator : IColumnAccumulator
        {
            private readonly string _name;
            private readonly List<string?> _values = new();

            public StringAccumulator(string name) => _name = name;

            public void Append(DbDataReader reader, int ordinal)
            {
                _values.Add(reader.IsDBNull(ordinal)
                    ? null
                    : Convert.ToString(reader.GetValue(ordinal)));
            }

            public ISeries BuildSeries(int rowCount)
            {
                int totalBytes = 0;
                foreach (var v in _values)
                    if (v != null) totalBytes += Encoding.UTF8.GetByteCount(v);

                var s = new Utf8StringSeries(_name, rowCount, totalBytes);
                var offsets = s.Offsets.Span;
                var data    = s.DataBytes.Span;
                int cur = 0;
                for (int i = 0; i < rowCount; i++)
                {
                    offsets[i] = cur;
                    if (_values[i] == null)
                    {
                        s.ValidityMask.SetNull(i);
                    }
                    else
                    {
                        byte[] bytes = Encoding.UTF8.GetBytes(_values[i]!);
                        bytes.CopyTo(data.Slice(cur));
                        cur += bytes.Length;
                    }
                }
                offsets[rowCount] = cur;
                return s;
            }

            public void Reset() => _values.Clear();
        }
    }

    // ── TimeSpan extension for microseconds (pre-.NET 7 compat) ─────────────

    internal static class TimeSpanExtensions
    {
        public static double TotalMicroseconds(this TimeSpan ts) =>
            ts.Ticks / 10.0;
    }
}
