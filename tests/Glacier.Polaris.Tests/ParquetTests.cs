using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Glacier.Polaris;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Glacier.Polaris.Tests
{
    public class ParquetTests
    {
        [Fact]
        public async Task TestScanParquet()
        {
            string path = "test.parquet";
            if (File.Exists(path)) File.Delete(path);

            // Create a test parquet file
            var idField = new DataField<int>("id");
            var valField = new DataField<double>("val");
            var nameField = new DataField<string>("name");
            var schema = new ParquetSchema(idField, valField, nameField);

            using (Stream fileStream = File.Create(path))
            {
                using (var writer = await ParquetWriter.CreateAsync(schema, fileStream))
                {
                    using (ParquetRowGroupWriter groupWriter = writer.CreateRowGroup())
                    {
                        await groupWriter.WriteColumnAsync(new DataColumn(idField, new[] { 1, 2, 3 }));
                        await groupWriter.WriteColumnAsync(new DataColumn(valField, new[] { 10.5, 20.5, 30.5 }));
                        await groupWriter.WriteColumnAsync(new DataColumn(nameField, new[] { "A", "B", "C" }));
                    }
                }
            }

            try
            {
                var lf = LazyFrame.ScanParquet(path)
                    .Filter(c => Expr.Col("id") > 1)
                    .Select(Expr.Col("val"), Expr.Col("name"));

                var df = await lf.Collect();

                Assert.Equal(2, df.RowCount);
                Assert.Equal(2, df.Columns.Count);
                Assert.Equal(20.5, ((Data.Float64Series)df.GetColumn("val")).Memory.Span[0]);
                
                var nameCol = (Data.Utf8StringSeries)df.GetColumn("name");
                var name = System.Text.Encoding.UTF8.GetString(nameCol.GetStringSpan(0));
                Assert.Equal("B", name);
            }
            finally
            {
                if (File.Exists(path)) File.Delete(path);
            }
        }
    }
}
