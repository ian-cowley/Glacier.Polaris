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

        // ── SqlDialect escaping, ORDER BY & limit/projection ordering ────────

        [Fact]
        public void SqlDialect_ApplyProjection_EscapesIdentifiersProperly()
        {
            string mssql = SqlDialectFactory.ApplyProjection(
                SqlDialect.SqlServer,
                "SELECT 1",
                ["col]name", "a] FROM t; --"]);
            Assert.Equal("SELECT [col]]name], [a]] FROM t; --] FROM (SELECT 1\n) __ag_sub", mssql);

            string pg = SqlDialectFactory.ApplyProjection(
                SqlDialect.PostgreSql,
                "SELECT 1",
                ["col\"name", "a\" FROM t; --"]);
            Assert.Equal("SELECT \"col\"\"name\", \"a\"\" FROM t; --\" FROM (SELECT 1\n) __ag_sub", pg);

            string generic = SqlDialectFactory.ApplyProjection(
                SqlDialect.Generic,
                "SELECT 1",
                ["a\"b"]);
            Assert.Equal("SELECT \"a\"\"b\" FROM (SELECT 1\n) __ag_sub", generic);
        }

        [Theory]
        [InlineData("")]
        [InlineData("   ")]
        [InlineData("col\0name")]
        public void SqlDialect_ApplyProjection_RejectsInvalidIdentifiers(string badCol)
        {
            Assert.ThrowsAny<ArgumentException>(() =>
                SqlDialectFactory.ApplyProjection(SqlDialect.SqlServer, "SELECT 1", [badCol]));
            Assert.ThrowsAny<ArgumentException>(() =>
                SqlDialectFactory.ApplyProjection(SqlDialect.PostgreSql, "SELECT 1", [badCol]));
        }

        [Fact]
        public void SqlDialect_ApplyProjection_NullIdentifier_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() =>
                SqlDialectFactory.ApplyProjection(SqlDialect.Generic, "SELECT 1", [null!]));
        }

        [Fact]
        public void SqlDialect_SqlServer_OrderByAndLimit_EmitsValidTsql()
        {
            string limited = SqlDialectFactory.ApplyLimit(
                SqlDialect.SqlServer,
                "SELECT a, b FROM t ORDER BY b DESC",
                10);
            Assert.Equal("SELECT a, b FROM t ORDER BY b DESC\nOFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY", limited);

            // Inner parenthesized ORDER BY must NOT trigger outer OFFSET 0 ROWS
            string innerOrder = SqlDialectFactory.ApplyLimit(
                SqlDialect.SqlServer,
                "SELECT a FROM t WHERE x IN (SELECT TOP 1 y FROM u ORDER BY y)",
                5);
            Assert.Equal("SELECT TOP (5) * FROM (SELECT a FROM t WHERE x IN (SELECT TOP 1 y FROM u ORDER BY y)\n) __ag_sub", innerOrder);

            // String literal containing 'order by' must NOT trigger OFFSET 0 ROWS
            string strLiteral = SqlDialectFactory.ApplyLimit(
                SqlDialect.SqlServer,
                "SELECT * FROM t WHERE note = 'order by x'",
                5);
            Assert.Equal("SELECT TOP (5) * FROM (SELECT * FROM t WHERE note = 'order by x'\n) __ag_sub", strLiteral);

            // Line comment containing 'order by' must NOT trigger OFFSET 0 ROWS
            string comment = SqlDialectFactory.ApplyLimit(
                SqlDialect.SqlServer,
                "SELECT * FROM t -- order by x",
                5);
            Assert.Equal("SELECT TOP (5) * FROM (SELECT * FROM t -- order by x\n) __ag_sub", comment);

            // Already has OFFSET/FETCH -> wrap with TOP
            string alreadyOffset = SqlDialectFactory.ApplyLimit(
                SqlDialect.SqlServer,
                "SELECT a FROM t ORDER BY a OFFSET 5 ROWS FETCH NEXT 20 ROWS ONLY",
                10);
            Assert.Equal("SELECT TOP (10) * FROM (SELECT a FROM t ORDER BY a OFFSET 5 ROWS FETCH NEXT 20 ROWS ONLY\n) __ag_sub", alreadyOffset);

            // Projection over top-level ORDER BY appends OFFSET 0 ROWS inside derived table
            string projectedOrder = SqlDialectFactory.ApplyProjection(
                SqlDialect.SqlServer,
                "SELECT a, b FROM t ORDER BY b DESC",
                ["a"]);
            Assert.Equal("SELECT [a] FROM (SELECT a, b FROM t ORDER BY b DESC\nOFFSET 0 ROWS) __ag_sub", projectedOrder);
        }

        [Fact]
        public void SqlDialect_ApplyLimit_NegativeAndDoubleLimitGuards()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                SqlDialectFactory.ApplyLimit(SqlDialect.SqlServer, "SELECT 1", -1));
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                SqlDialectFactory.ApplyLimit(SqlDialect.Generic, "SELECT 1", -5));

            string pgDouble = SqlDialectFactory.ApplyLimit(
                SqlDialect.PostgreSql,
                "SELECT * FROM t LIMIT 20",
                5);
            Assert.Equal("SELECT * FROM (SELECT * FROM t LIMIT 20\n) __ag_sub LIMIT 5", pgDouble);
        }

        [Fact]
        public void SqlDialect_TrailingCommentAndAliasKeywords_HandledSafely()
        {
            // Trailing line comments must not swallow LIMIT, OFFSET/FETCH, or closing derived-table parenthesis
            string tsqlCommentLimit = SqlDialectFactory.ApplyLimit(
                SqlDialect.SqlServer,
                "SELECT a FROM t ORDER BY a -- trailing comment",
                5);
            Assert.Equal("SELECT a FROM t ORDER BY a -- trailing comment\nOFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY", tsqlCommentLimit);

            string pgCommentLimit = SqlDialectFactory.ApplyLimit(
                SqlDialect.PostgreSql,
                "SELECT a FROM t ORDER BY a -- trailing comment",
                5);
            Assert.Equal("SELECT a FROM t ORDER BY a -- trailing comment\nLIMIT 5", pgCommentLimit);

            string genericCommentProj = SqlDialectFactory.ApplyProjection(
                SqlDialect.Generic,
                "SELECT a FROM t -- trailing comment",
                ["a"]);
            Assert.Equal("SELECT \"a\" FROM (SELECT a FROM t -- trailing comment\n) __ag_sub", genericCommentProj);

            // Alias/column keywords must NOT suppress T-SQL OFFSET 0 ROWS or falsely trigger subquery wrapping
            string aliasOffset = SqlDialectFactory.ApplyLimit(
                SqlDialect.SqlServer,
                "SELECT a AS offset FROM t ORDER BY a",
                5);
            Assert.Equal("SELECT a AS offset FROM t ORDER BY a\nOFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY", aliasOffset);

            string bareTopCol = SqlDialectFactory.ApplyLimit(
                SqlDialect.SqlServer,
                "SELECT top FROM t ORDER BY a",
                5);
            Assert.Equal("SELECT top FROM t ORDER BY a\nOFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY", bareTopCol);

            string aliasOffsetProj = SqlDialectFactory.ApplyProjection(
                SqlDialect.SqlServer,
                "SELECT a AS offset FROM t ORDER BY a",
                ["offset"]);
            Assert.Equal("SELECT [offset] FROM (SELECT a AS offset FROM t ORDER BY a\nOFFSET 0 ROWS) __ag_sub", aliasOffsetProj);

            string paramOffset = SqlDialectFactory.ApplyLimit(
                SqlDialect.SqlServer,
                "SELECT a FROM t WHERE x = @offset ORDER BY a",
                5);
            Assert.Equal("SELECT a FROM t WHERE x = @offset ORDER BY a\nOFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY", paramOffset);

            string aliasLimitPg = SqlDialectFactory.ApplyLimit(
                SqlDialect.PostgreSql,
                "SELECT a AS limit FROM t",
                5);
            Assert.Equal("SELECT a AS limit FROM t\nLIMIT 5", aliasLimitPg);

            string bareLimitGeneric = SqlDialectFactory.ApplyLimit(
                SqlDialect.Generic,
                "SELECT limit FROM t",
                5);
            Assert.Equal("SELECT limit FROM t\nLIMIT 5", bareLimitGeneric);

            string aliasFetchPg = SqlDialectFactory.ApplyLimit(
                SqlDialect.PostgreSql,
                "SELECT a AS fetch FROM t",
                5);
            Assert.Equal("SELECT a AS fetch FROM t\nLIMIT 5", aliasFetchPg);
        }

        [Fact]
        public async Task ReadAsync_TrailingLineComment_WithLimitAndProjection_WorksOnSqlite()
        {
            var reader = new SqlReader(
                _conn,
                "SELECT id, name FROM employees ORDER BY id -- trailing comment",
                columns: ["id"],
                limit: 2);

            var chunks = new List<DataFrame>();
            await foreach (var c in reader.ReadAsync())
                chunks.Add(c);

            Assert.Single(chunks);
            var df = chunks[0];
            Assert.Single(df.Columns);
            Assert.Equal(2, df.RowCount);
            Assert.Equal(1L, df.GetColumn("id").Get(0));
            Assert.Equal(2L, df.GetColumn("id").Get(1));
        }

        [Fact]
        public async Task ReadAsync_QuotedColumnAndOrderByUnprojectedColumnWithLimit_WorksOnSqlite()
        {
            using (var setup = _conn.CreateCommand())
            {
                setup.CommandText = """
                    CREATE TABLE quoted_items ("col""q" TEXT NOT NULL, sort_key INTEGER NOT NULL);
                    INSERT INTO quoted_items VALUES ('low', 1), ('high', 100), ('mid', 50);
                    """;
                setup.ExecuteNonQuery();
            }

            // Order by unprojected sort_key DESC with limit 2 and quoted column projection
            var reader = new SqlReader(
                _conn,
                "SELECT \"col\"\"q\", sort_key FROM quoted_items ORDER BY sort_key DESC",
                columns: ["col\"q"],
                limit: 2);

            var chunks = new List<DataFrame>();
            await foreach (var c in reader.ReadAsync())
                chunks.Add(c);

            Assert.Single(chunks);
            var df = chunks[0];
            Assert.Single(df.Columns);
            Assert.Equal(2, df.RowCount);
            Assert.Equal("high", df.GetColumn("col\"q").Get(0));
            Assert.Equal("mid", df.GetColumn("col\"q").Get(1));
        }

        [Fact]
        public async Task ReadAsync_TrailingSemicolonBeforeComments_WithLimitAndProjection_WorksOnSqlite()
        {
            string[] queries =
            [
                "SELECT id, name FROM employees ORDER BY id; -- trailing comment",
                "SELECT id, name FROM employees ORDER BY id; /* block comment */",
                "SELECT id, name FROM employees ORDER BY id ; ; -- multi-semi\n/* trailing */"
            ];

            foreach (string query in queries)
            {
                // Limit alone
                var readerLimitOnly = new SqlReader(_conn, query, limit: 2);
                var chunksLimitOnly = new List<DataFrame>();
                await foreach (var c in readerLimitOnly.ReadAsync())
                    chunksLimitOnly.Add(c);

                Assert.Single(chunksLimitOnly);
                Assert.Equal(2, chunksLimitOnly[0].RowCount);
                Assert.Equal(1L, chunksLimitOnly[0].GetColumn("id").Get(0));
                Assert.Equal(2L, chunksLimitOnly[0].GetColumn("id").Get(1));

                // Columns + Limit
                var readerProjAndLimit = new SqlReader(_conn, query, columns: ["id"], limit: 2);
                var chunksProjAndLimit = new List<DataFrame>();
                await foreach (var c in readerProjAndLimit.ReadAsync())
                    chunksProjAndLimit.Add(c);

                Assert.Single(chunksProjAndLimit);
                Assert.Single(chunksProjAndLimit[0].Columns);
                Assert.Equal(2, chunksProjAndLimit[0].RowCount);
                Assert.Equal(1L, chunksProjAndLimit[0].GetColumn("id").Get(0));
                Assert.Equal(2L, chunksProjAndLimit[0].GetColumn("id").Get(1));
            }
        }

        [Fact]
        public void SqlDialect_StripTrailingSemicolons_And_SelectFetchFirstAlias_HandledSafely()
        {
            // Trailing semicolons stripped across comments, while semicolons inside literals/comments/multi-statement are preserved
            Assert.Equal(
                "SELECT a FROM t ORDER BY a -- comment",
                SqlDialectFactory.StripTrailingSemicolons("SELECT a FROM t ORDER BY a; -- comment"));
            Assert.Equal(
                "SELECT a FROM t /* c */",
                SqlDialectFactory.StripTrailingSemicolons("SELECT a FROM t; /* c */  "));
            Assert.Equal(
                "SELECT ';' FROM t -- ;",
                SqlDialectFactory.StripTrailingSemicolons("SELECT ';' FROM t -- ;"));
            Assert.Equal(
                "SELECT a FROM t; SELECT 1",
                SqlDialectFactory.StripTrailingSemicolons("SELECT a FROM t; SELECT 1"));

            // T-SQL stripped query + ApplyLimit
            string strippedTsql = SqlDialectFactory.StripTrailingSemicolons("SELECT a FROM t ORDER BY a; -- comment");
            Assert.Equal(
                "SELECT a FROM t ORDER BY a -- comment\nOFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY",
                SqlDialectFactory.ApplyLimit(SqlDialect.SqlServer, strippedTsql, 5));

            // Alias without AS in SELECT projection list must NOT falsely trigger subquery wrapping
            Assert.Equal(
                "SELECT fetch first FROM t\nLIMIT 5",
                SqlDialectFactory.ApplyLimit(SqlDialect.PostgreSql, "SELECT fetch first FROM t", 5));
            Assert.Equal(
                "SELECT fetch first FROM t\nLIMIT 5",
                SqlDialectFactory.ApplyLimit(SqlDialect.Generic, "SELECT fetch first FROM t", 5));
            Assert.Equal(
                "SELECT a, fetch next FROM t\nLIMIT 5",
                SqlDialectFactory.ApplyLimit(SqlDialect.PostgreSql, "SELECT a, fetch next FROM t", 5));
            Assert.Equal(
                "SELECT limit all FROM t\nLIMIT 5",
                SqlDialectFactory.ApplyLimit(SqlDialect.PostgreSql, "SELECT limit all FROM t", 5));
            Assert.Equal(
                "SELECT a FROM fetch first WHERE x = 1\nLIMIT 5",
                SqlDialectFactory.ApplyLimit(SqlDialect.PostgreSql, "SELECT a FROM fetch first WHERE x = 1", 5));

            // Real FETCH FIRST / LIMIT ALL clauses still wrap cleanly
            Assert.Equal(
                "SELECT * FROM (SELECT a FROM t FETCH FIRST 5 ROWS ONLY\n) __ag_sub LIMIT 2",
                SqlDialectFactory.ApplyLimit(SqlDialect.PostgreSql, "SELECT a FROM t FETCH FIRST 5 ROWS ONLY", 2));
            Assert.Equal(
                "SELECT * FROM (SELECT 1 FETCH FIRST ROW ONLY\n) __ag_sub LIMIT 2",
                SqlDialectFactory.ApplyLimit(SqlDialect.PostgreSql, "SELECT 1 FETCH FIRST ROW ONLY", 2));
            Assert.Equal(
                "SELECT * FROM (SELECT 1 LIMIT ALL\n) __ag_sub LIMIT 2",
                SqlDialectFactory.ApplyLimit(SqlDialect.PostgreSql, "SELECT 1 LIMIT ALL", 2));
        }
    }
}
