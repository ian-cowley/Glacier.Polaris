using System;
using System.IO;
using Apache.Arrow.Ipc;

namespace Glacier.Polaris.IO
{
    internal static class IpcWriter
    {
        /// <summary>
        /// Writes the DataFrame to Apache Arrow IPC file format.
        /// Matching Polars' DataFrame.write_ipc() behavior.
        /// </summary>
        public static void Write(DataFrame df, string filePath)
        {
            using var fs = new FileStream(filePath, FileMode.Create, FileAccess.Write);
            var recordBatch = df.ToArrowRecordBatch();

            // Use ArrowFileWriter for the IPC file format (with footer/metadata).
            // This produces a valid .arrow file that can be read by PyArrow, Polars, etc.
            using var writer = new ArrowFileWriter(fs, recordBatch.Schema);
            writer.WriteRecordBatch(recordBatch);
            writer.WriteEnd();
        }

        /// <summary>
        /// Writes the DataFrame to Apache Arrow IPC stream format (no footer).
        /// The stream variant is a lower-overhead continuous stream.
        /// </summary>
        public static void WriteStream(DataFrame df, string filePath)
        {
            using var fs = new FileStream(filePath, FileMode.Create, FileAccess.Write);
            var recordBatch = df.ToArrowRecordBatch();

            using var writer = new ArrowStreamWriter(fs, recordBatch.Schema);
            writer.WriteRecordBatch(recordBatch);
        }
    }
}
