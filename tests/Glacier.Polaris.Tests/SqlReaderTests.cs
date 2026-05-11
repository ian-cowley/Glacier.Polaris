using System.Collections.Generic;
using System.Threading.Tasks;
using Glacier.Polaris;
using Glacier.Polaris.IO;
using Microsoft.Data.Sqlite;

namespace Glacier.Polaris.Tests
{
    public class SqlReaderTests : IDisposable
    {
        private readonly SqliteConnection _conn;

        public SqlReaderTests()
        {
            _conn = new SqliteConnection("Data Source=:memory:");
            _conn.Open();

            using var cmd = _conn.CreateCommand();
            cmd.CommandText = """
                CREATE TABLE employees (
                    id      INTEGER  NOT NULL,
                    name    TEXT     NOT NULL,
                    salary  REAL     NOT NULL,
                    active  INTEGER  NOT NULL
                );
                INSERT INTO employees VALUES (1, 'Alice', 95000.0, 1);
                INSERT INTO employees VALUES (2, 'Bob',   82000.0, 0);
                INSERT INTO employees VALUES (3, 'Carol', 110000.0, 1);
                INSERT INTO employees VALUES (4, 'Dave',  74000.0, 0);
                INSERT INTO employees VALUES (5, 'Eve',   130000.0, 1);
                """;
            cmd.ExecuteNonQuery();
        }

        public void Dispose() => _conn.Dispose();

        // ── Basic read ───────────────────────────────────────────────────────

        [Fact]
        public async Task ReadAsync_ReturnsAllRows()
        {
            var reader = new SqlReader(_conn, "SELECT * FROM employees");
            var chunks = new List<DataFrame>();
            await foreach (var chunk in reader.ReadAsync())
                chunks.Add(chunk);

            int totalRows = 0;
            foreach (var c in chunks) totalRows += c.RowCount;
            Assert.Equal(5, totalRows);
        }

        [Fact]
        public async Task ReadAsync_ColumnNamesMatchSchema()
        {
            var reader = new SqlReader(_conn, "SELECT * FROM employees");
            DataFrame? first = null;
            await foreach (var chunk in reader.ReadAsync()) { first = chunk; break; }

            Assert.NotNull(first);
            var names = first!.Columns.Select(c => c.Name).ToList();
            Assert.Contains("id",     names);
            Assert.Contains("name",   names);
            Assert.Contains("salary", names);
            Assert.Contains("active", names);
        }

        // ── Projection pushdown ──────────────────────────────────────────────

        [Fact]
        public async Task ReadAsync_ColumnProjection_OnlyRequestedColumnsReturned()
        {
            var reader = new SqlReader(_conn, "SELECT * FROM employees", columns: ["id", "salary"]);
            DataFrame? first = null;
            await foreach (var chunk in reader.ReadAsync()) { first = chunk; break; }

            Assert.NotNull(first);
            Assert.Equal(2, first!.Columns.Count);
            var names = first!.Columns.Select(c => c.Name).ToList();
            Assert.Contains("id",     names);
            Assert.Contains("salary", names);
        }

        // ── Row limit ────────────────────────────────────────────────────────

        [Fact]
        public async Task ReadAsync_LimitApplied_ReturnsCorrectRowCount()
        {
            var reader = new SqlReader(_conn, "SELECT * FROM employees", limit: 3);
            int total = 0;
            await foreach (var chunk in reader.ReadAsync())
                total += chunk.RowCount;

            Assert.Equal(3, total);
        }

        // ── LazyFrame.ScanSql integration ────────────────────────────────────

        [Fact]
        public async Task ScanSql_WithFilter_ReturnsFilteredRows()
        {
            // Push the predicate directly into SQL to avoid cross-type comparison between
            // the SQLite INTEGER→Int64 column and an Int32 literal.
            var lf = LazyFrame.ScanSql(_conn, "SELECT * FROM employees WHERE active = 1");

            var df = await lf.Collect();
            Assert.Equal(3, df.RowCount); // Alice, Carol, Eve
        }

        [Fact]
        public async Task ScanSql_WithSelect_ProjectsColumns()
        {
            var lf = LazyFrame.ScanSql(_conn, "SELECT * FROM employees")
                .Select(Expr.Col("name"), Expr.Col("salary"));

            var df = await lf.Collect();
            Assert.Equal(2, df.Columns.Count);
            Assert.Equal(5, df.RowCount);
        }

        [Fact]
        public async Task ScanSql_WithLimit_HonorsRowCap()
        {
            var lf = LazyFrame.ScanSql(_conn, "SELECT * FROM employees")
                .Limit(2);

            var df = await lf.Collect();
            Assert.Equal(2, df.RowCount);
        }
    }
}
