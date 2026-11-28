using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Formats.Arrow;

namespace Glacier.Polaris.IO
{
    internal static class IpcWriter
    {
        public static void Write(DataFrame df, string filePath)
        {
            using var fs = new FileStream(filePath, FileMode.Create, FileAccess.Write);
            df.ToArrowIpc(fs);
        }

        public static void WriteStream(DataFrame df, string filePath)
        {
            using var fs = new FileStream(filePath, FileMode.Create, FileAccess.Write);
            df.ToArrowIpc(fs);
        }

        internal static async Task WriteStreamAsync(IAsyncEnumerable<DataFrame> batches, string filePath, CancellationToken ct = default)
        {
            await using var fs = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None, 65536, FileOptions.SequentialScan);
            using var writer = new ArrowStreamWriter(fs, leaveOpen: true);
            bool schemaWritten = false;

            await foreach (var df in batches.WithCancellation(ct).ConfigureAwait(false))
            {
                if (df.Columns.Count == 0)
                {
                    continue;
                }

                var batch = df.ToGlacierRecordBatch();
                if (!schemaWritten)
                {
                    writer.WriteSchema(batch.Schema);
                    schemaWritten = true;
                }

                if (df.RowCount > 0)
                {
                    writer.WriteRecordBatch(batch);
                }
            }

            if (!schemaWritten)
            {
                writer.WriteSchema(new ArrowSchema(Array.Empty<ArrowField>()));
            }

            writer.WriteEndOfStream();
        }
    }
}
