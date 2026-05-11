using System;
using System.Threading.Tasks;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Xunit;

namespace Glacier.Polaris.Tests
{
    public class ListTests
    {
        #region Helpers

        /// <summary>Build a ListSeries{Int32} from row-wise jagged arrays.</summary>
        private static ListSeries MakeInt32List(string name, int[][] rows)
        {
            int totalValues = 0;
            foreach (var row in rows) totalValues += row.Length;
            var offsets = new int[rows.Length + 1];
            var values = new int[totalValues];
            int pos = 0;
            for (int i = 0; i < rows.Length; i++)
            {
                offsets[i] = pos;
                foreach (var v in rows[i]) values[pos++] = v;
            }
            offsets[rows.Length] = pos;
            return new ListSeries(name, new Int32Series("off", offsets), new Int32Series("val", values));
        }

        /// <summary>Build a ListSeries{Float64} from row-wise nullable doubles.</summary>
        private static ListSeries MakeFloat64List(string name, double?[][] rows)
        {
            int totalValues = 0;
            foreach (var row in rows) totalValues += row.Length;
            var offsets = new int[rows.Length + 1];
            var fvals = new Float64Series("val", totalValues);
            int pos = 0;
            for (int i = 0; i < rows.Length; i++)
            {
                offsets[i] = pos;
                foreach (var v in rows[i])
                {
                    if (v.HasValue) fvals[pos] = v.Value;
                    else fvals.ValidityMask.SetNull(pos);
                    pos++;
                }
            }
            offsets[rows.Length] = pos;
            return new ListSeries(name, new Int32Series("off", offsets), fvals);
        }

        /// <summary>Build a ListSeries{Utf8String} from row-wise string arrays.</summary>
        private static ListSeries MakeStringList(string name, string?[][] rows)
        {
            int totalValues = 0;
            foreach (var row in rows) totalValues += row.Length;
            var offsets = new int[rows.Length + 1];
            var allStrings = new string?[totalValues];
            int pos = 0;
            for (int i = 0; i < rows.Length; i++)
            {
                offsets[i] = pos;
                foreach (var v in rows[i]) allStrings[pos++] = v;
            }
            offsets[rows.Length] = pos;
            return new ListSeries(name, new Int32Series("off", offsets), Utf8StringSeries.FromStrings("val", allStrings));
        }

        #endregion

        // ──────────────────────────────────────────────
        // List.Sum / List.Mean
        // ──────────────────────────────────────────────

        [Fact]
        public async Task TestListSumMean_Int32()
        {
            // rows: [10,20,30], [40,50], [60,70], []
            var list = MakeInt32List("lst", new[] {
                new[] { 10, 20, 30 },
                new[] { 40, 50 },
                new[] { 60, 70 },
                Array.Empty<int>()
            });
            var df = new DataFrame(new ISeries[] { list });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("lst").List().Sum().Alias("s"),
                    Expr.Col("lst").List().Mean().Alias("m")
                )
                .Collect();

            var s = result.GetColumn("s") as Float64Series;
            var m = result.GetColumn("m") as Float64Series;
            Assert.NotNull(s);
            Assert.NotNull(m);

            Assert.Equal(60, s.Memory.Span[0]);  // 10+20+30
            Assert.Equal(90, s.Memory.Span[1]);  // 40+50
            Assert.Equal(130, s.Memory.Span[2]); // 60+70
            Assert.True(s.ValidityMask.IsNull(3)); // empty → null

            Assert.Equal(20.0, m.Memory.Span[0]);
            Assert.Equal(45.0, m.Memory.Span[1]);
            Assert.Equal(65.0, m.Memory.Span[2]);
            Assert.True(m.ValidityMask.IsNull(3));
        }

        [Fact]
        public async Task TestListSumMean_Float64_WithNulls()
        {
            // rows: [1.5, null, 3.0], [10.0], [null, null], []
            var list = MakeFloat64List("lst", new double?[][] {
                new double?[] { 1.5, null, 3.0 },
                new double?[] { 10.0 },
                new double?[] { null, null },
                Array.Empty<double?>()
            });
            var df = new DataFrame(new ISeries[] { list });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("lst").List().Sum().Alias("s"),
                    Expr.Col("lst").List().Mean().Alias("m")
                )
                .Collect();

            var s = result.GetColumn("s") as Float64Series;
            var m = result.GetColumn("m") as Float64Series;
            Assert.NotNull(s); Assert.NotNull(m);

            // Row 0: sum = 4.5 (1.5+3.0, null skipped), mean = 2.25 (4.5/2)
            Assert.Equal(4.5, s.Memory.Span[0], 6);
            Assert.Equal(2.25, m.Memory.Span[0], 6);
            // Row 1: sum = 10.0, mean = 10.0
            Assert.Equal(10.0, s.Memory.Span[1], 6);
            Assert.Equal(10.0, m.Memory.Span[1], 6);
            // Row 2: all null elements → null
            Assert.True(s.ValidityMask.IsNull(2));
            Assert.True(m.ValidityMask.IsNull(2));
            // Row 3: empty → null
            Assert.True(s.ValidityMask.IsNull(3));
            Assert.True(m.ValidityMask.IsNull(3));
        }

        // ──────────────────────────────────────────────
        // List.Min / List.Max
        // ──────────────────────────────────────────────

        [Fact]
        public async Task TestListMinMax_Int32()
        {
            var list = MakeInt32List("lst", new[] {
                new[] { 30, 10, 20 },
                new[] { 5, 99 },
                new[] { -1, -5, 0 },
                Array.Empty<int>()
            });
            var df = new DataFrame(new ISeries[] { list });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("lst").List().Min().Alias("mn"),
                    Expr.Col("lst").List().Max().Alias("mx")
                )
                .Collect();

            var mn = result.GetColumn("mn") as Int32Series;
            var mx = result.GetColumn("mx") as Int32Series;
            Assert.NotNull(mn); Assert.NotNull(mx);

            Assert.Equal(10, mn.Memory.Span[0]); Assert.Equal(30, mx.Memory.Span[0]);
            Assert.Equal(5, mn.Memory.Span[1]);  Assert.Equal(99, mx.Memory.Span[1]);
            Assert.Equal(-5, mn.Memory.Span[2]); Assert.Equal(0, mx.Memory.Span[2]);
            Assert.True(mn.ValidityMask.IsNull(3));
            Assert.True(mx.ValidityMask.IsNull(3));
        }

        [Fact]
        public async Task TestListMinMax_Float64()
        {
            var list = MakeFloat64List("lst", new double?[][] {
                new double?[] { 3.0, 1.0, 2.0 },
                new double?[] { -5.0, 10.0, null },
                new double?[] { null, null },
                Array.Empty<double?>()
            });
            var df = new DataFrame(new ISeries[] { list });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("lst").List().Min().Alias("mn"),
                    Expr.Col("lst").List().Max().Alias("mx")
                )
                .Collect();

            var mn = result.GetColumn("mn") as Float64Series;
            var mx = result.GetColumn("mx") as Float64Series;
            Assert.NotNull(mn); Assert.NotNull(mx);

            Assert.Equal(1.0, mn.Memory.Span[0]); Assert.Equal(3.0, mx.Memory.Span[0]);
            Assert.Equal(-5.0, mn.Memory.Span[1]); Assert.Equal(10.0, mx.Memory.Span[1]);
            Assert.True(mn.ValidityMask.IsNull(2));
            Assert.True(mx.ValidityMask.IsNull(2));
            Assert.True(mn.ValidityMask.IsNull(3));
            Assert.True(mx.ValidityMask.IsNull(3));
        }

        // ──────────────────────────────────────────────
        // List.Lengths
        // ──────────────────────────────────────────────

        [Fact]
        public async Task TestListLengths()
        {
            var list = MakeInt32List("lst", new[] {
                new[] { 1, 2, 3 },
                new[] { 4, 5 },
                Array.Empty<int>(),
                new[] { 6 }
            });
            var df = new DataFrame(new ISeries[] { list });

            var result = await df.Lazy()
                .Select(Expr.Col("lst").List().Lengths().Alias("len"))
                .Collect();

            var len = result.GetColumn("len") as Int32Series;
            Assert.NotNull(len);
            Assert.Equal(3, len.Memory.Span[0]);
            Assert.Equal(2, len.Memory.Span[1]);
            Assert.Equal(0, len.Memory.Span[2]);
            Assert.Equal(1, len.Memory.Span[3]);
        }

        // ──────────────────────────────────────────────
        // List.Get (positive & negative indexing)
        // ──────────────────────────────────────────────

        [Fact]
        public async Task TestListGet()
        {
            var list = MakeInt32List("lst", new[] {
                new[] { 10, 20, 30 },
                new[] { 40, 50 },
                Array.Empty<int>(),
                new[] { 60 }
            });
            var df = new DataFrame(new ISeries[] { list });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("lst").List().Get(0).Alias("first"),
                    Expr.Col("lst").List().Get(-1).Alias("last"),
                    Expr.Col("lst").List().Get(5).Alias("ob")
                )
                .Collect();

            var first = result.GetColumn("first") as Int32Series;
            var last = result.GetColumn("last") as Int32Series;
            var ob = result.GetColumn("ob") as Int32Series;
            Assert.NotNull(first); Assert.NotNull(last); Assert.NotNull(ob);

            // Positive index
            Assert.Equal(10, first.Memory.Span[0]);
            Assert.Equal(40, first.Memory.Span[1]);
            Assert.True(first.ValidityMask.IsNull(2)); // empty → null
            Assert.Equal(60, first.Memory.Span[3]);

            // Negative index (wrap):  -1 → last element
            Assert.Equal(30, last.Memory.Span[0]);
            Assert.Equal(50, last.Memory.Span[1]);
            Assert.True(last.ValidityMask.IsNull(2));
            Assert.Equal(60, last.Memory.Span[3]);

            // Out of bounds → null
            Assert.True(ob.ValidityMask.IsNull(0));
            Assert.True(ob.ValidityMask.IsNull(1));
            Assert.True(ob.ValidityMask.IsNull(2));
            Assert.True(ob.ValidityMask.IsNull(3));
        }

        // ──────────────────────────────────────────────
        // List.Contains
        // ──────────────────────────────────────────────

        [Fact]
        public async Task TestListContains()
        {
            var list = MakeInt32List("lst", new[] {
                new[] { 10, 20, 30 },
                new[] { 40, 50 },
                Array.Empty<int>(),
                new[] { 10, 10 }
            });
            var df = new DataFrame(new ISeries[] { list });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("lst").List().Contains(10).Alias("has10"),
                    Expr.Col("lst").List().Contains(99).Alias("has99")
                )
                .Collect();

            var has10 = result.GetColumn("has10") as BooleanSeries;
            var has99 = result.GetColumn("has99") as BooleanSeries;
            Assert.NotNull(has10); Assert.NotNull(has99);

            Assert.True(has10.Memory.Span[0]);  // [10,20,30] contains 10
            Assert.False(has10.Memory.Span[1]); // [40,50] doesn't contain 10
            Assert.False(has10.Memory.Span[2]); // empty
            Assert.True(has10.Memory.Span[3]);   // [10,10] contains 10

            Assert.False(has99.Memory.Span[0]);
            Assert.False(has99.Memory.Span[1]);
            Assert.False(has99.Memory.Span[2]);
            Assert.False(has99.Memory.Span[3]);
        }

        // ──────────────────────────────────────────────
        // List.Join (string concat)
        // ──────────────────────────────────────────────

        [Fact]
        public async Task TestListJoin()
        {
            var list = MakeStringList("lst", new string?[][] {
                new[] { "a", "b", "c" },
                new[] { "x", "y" },
                Array.Empty<string>()
            });
            var df = new DataFrame(new ISeries[] { list });

            var result = await df.Lazy()
                .Select(Expr.Col("lst").List().Join("-").Alias("joined"))
                .Collect();

            var joined = result.GetColumn("joined") as Utf8StringSeries;
            Assert.NotNull(joined);
            Assert.Equal("a-b-c", joined.GetString(0));
            Assert.Equal("x-y", joined.GetString(1));
            Assert.Equal("", joined.GetString(2));
        }

        // ──────────────────────────────────────────────
        // List.Unique (dedup within each sublist)
        // ──────────────────────────────────────────────

        [Fact]
        public async Task TestListUnique()
        {
            var list = MakeInt32List("lst", new[] {
                new[] { 1, 2, 2, 3, 1 },
                new[] { 5, 5, 5 },
                new[] { 10 },
                Array.Empty<int>(),
                new[] { 1, 2, 3 }
            });
            var df = new DataFrame(new ISeries[] { list });

            var result = await df.Lazy()
                .Select(Expr.Col("lst").List().Unique().Alias("uniq"))
                .Collect();

            var uniq = result.GetColumn("uniq") as ListSeries;
            Assert.NotNull(uniq);

            // Row 0: [1,2,3] (deduped)
            var row0 = (object?[])uniq.Get(0)!;
            Assert.Equal(3, row0.Length);
            Assert.Equal(1, row0[0]); Assert.Equal(2, row0[1]); Assert.Equal(3, row0[2]);

            // Row 1: [5] (all same)
            var row1 = (object?[])uniq.Get(1)!;
            Assert.Single(row1);
            Assert.Equal(5, row1[0]);

            // Row 2: [10] (single element, unchanged)
            var row2 = (object?[])uniq.Get(2)!;
            Assert.Single(row2);
            Assert.Equal(10, row2[0]);

            // Row 3: [] (empty → empty)
            var row3 = (object?[])uniq.Get(3)!;
            Assert.Empty(row3);
        }

        // ──────────────────────────────────────────────
        // List via split -> chained expression
        // ──────────────────────────────────────────────

        [Fact]
        public async Task TestListSumViaSplitChain()
        {
            // Split strings then compute list sum.
            // Note: "".Split("|") returns [""] (not []), so use "0" for the zero-sum case.
            var strings = Utf8StringSeries.FromStrings("nums", new[] { "10|20|30", "40|50", "0", "60" });

            var df = new DataFrame(new ISeries[] { strings });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("nums").Str().Split("|").List().Sum().Alias("total")
                )
                .Collect();

            var total = result.GetColumn("total") as Float64Series;
            Assert.NotNull(total);

            Assert.Equal(60.0, total.Memory.Span[0]); // 10+20+30
            Assert.Equal(90.0, total.Memory.Span[1]); // 40+50
            Assert.Equal(0.0, total.Memory.Span[2]);  // "0" → ["0"] → 0

            Assert.Equal(60.0, total.Memory.Span[3]);  // "60"
        }

        [Fact]
        public async Task TestListLengthsViaSplitChain()
        {
            var strings = Utf8StringSeries.FromStrings("words", new[] { "a,b,c", "x,y", "", "z" });
            var df = new DataFrame(new ISeries[] { strings });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("words").Str().Split(",").List().Lengths().Alias("cnt")
                )
                .Collect();

            var cnt = result.GetColumn("cnt") as Int32Series;
            Assert.NotNull(cnt);
            Assert.Equal(3, cnt.Memory.Span[0]);
            Assert.Equal(2, cnt.Memory.Span[1]);
            Assert.Equal(1, cnt.Memory.Span[2]); // "".Split(",") → [""], length 1

            Assert.Equal(1, cnt.Memory.Span[3]);
        }

        // ──────────────────────────────────────────────
        // Null propagation: null row in list column
        // ──────────────────────────────────────────────

        [Fact]
        public async Task TestListNullRowPropagation()
        {
            // Create a ListSeries with a null validity mask for row 1
            var offsets = new Int32Series("off", new int[] { 0, 2, 4, 6 });
            var values = new Int32Series("val", new int[] { 1, 2, 3, 4, 5, 6 });
            var list = new ListSeries("lst", offsets, values);
            list.ValidityMask.SetNull(1); // row 1 is null

            var df = new DataFrame(new ISeries[] { list });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("lst").List().Sum().Alias("s"),
                    Expr.Col("lst").List().Mean().Alias("m"),
                    Expr.Col("lst").List().Min().Alias("mn"),
                    Expr.Col("lst").List().Max().Alias("mx"),
                    Expr.Col("lst").List().Lengths().Alias("len")
                )
                .Collect();

            // Row 0: valid, Row 1: null → all null
            var s = result.GetColumn("s") as Float64Series;
            var m = result.GetColumn("m") as Float64Series;
            var mn = result.GetColumn("mn") as Int32Series;
            var mx = result.GetColumn("mx") as Int32Series;
            var len = result.GetColumn("len") as Int32Series;
            Assert.NotNull(s); Assert.NotNull(m); Assert.NotNull(mn); Assert.NotNull(mx); Assert.NotNull(len);

            // Row 0 values
            Assert.Equal(3.0, s.Memory.Span[0]); // 1+2
            Assert.Equal(1.5, m.Memory.Span[0]); // 3/2
            Assert.Equal(1, mn.Memory.Span[0]);
            Assert.Equal(2, mx.Memory.Span[0]);
            Assert.Equal(2, len.Memory.Span[0]);

            // Row 1: null → all null
            Assert.True(s.ValidityMask.IsNull(1));
            Assert.True(m.ValidityMask.IsNull(1));
            Assert.True(mn.ValidityMask.IsNull(1));
            Assert.True(mx.ValidityMask.IsNull(1));
            Assert.False(len.ValidityMask.IsNull(1)); // Lengths doesn't use validity mask from list
        }
    }
}
