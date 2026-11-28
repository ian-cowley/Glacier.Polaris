using System;
using System.Buffers;
using System.IO;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;

namespace Glacier.Polaris.IO
{
    public sealed class CsvReader
    {
        private readonly string _filePath;
        private readonly string[]? _columns;
        private readonly int? _nRows;
        private long _totalRowsRead;

        public static string[] PeekHeaders(string filePath)
        {
            using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            int bufSize = 4096;
            byte[] buffer = new byte[bufSize];
            int totalRead = 0;

            while (true)
            {
                if (totalRead == buffer.Length)
                {
                    Array.Resize(ref buffer, buffer.Length * 2);
                }

                int read = stream.Read(buffer, totalRead, buffer.Length - totalRead);
                totalRead += read;
                bool isEof = read == 0;

                int start = (totalRead >= 3 && buffer[0] == 0xEF && buffer[1] == 0xBB && buffer[2] == 0xBF) ? 3 : 0;
                var seq = new ReadOnlySequence<byte>(buffer, start, totalRead - start);
                if (CsvParser.TryReadLine(ref seq, out ReadOnlySequence<byte> headerLine, isFinalBlock: isEof))
                {
                    return CsvParser.ParseHeader(headerLine.FirstSpan).ToArray();
                }

                if (isEof)
                {
                    return Array.Empty<string>();
                }
            }
        }

        public CsvReader(string filePath, string[]? columns = null, int? nRows = null)
        {
            _filePath = filePath;
            _columns = columns;
            _nRows = nRows;
        }

        public async IAsyncEnumerable<DataFrame> ReadAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            // Configure Pipeline with Backpressure
            var pipeOptions = new PipeOptions(
                pauseWriterThreshold: 32 * 1024 * 1024,  // 32MB
                resumeWriterThreshold: 16 * 1024 * 1024, // 16MB
                useSynchronizationContext: false
            );

            var pipe = new Pipe(pipeOptions);

            // Producer Task: Read from FileStream and push to PipeWriter
            var producerTask = FillPipeAsync(_filePath, pipe.Writer, cancellationToken);

            // Consumer Task: Parse sequences and yield DataFrames
            await foreach (var df in ConsumePipeAsync(pipe.Reader, cancellationToken))
            {
                yield return df;
            }

            await producerTask;
        }

        private async Task FillPipeAsync(string filePath, PipeWriter writer, CancellationToken cancellationToken)
        {
            const int minimumBufferSize = 65536; // 64KB chunks

            try
            {
                await using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.Asynchronous | FileOptions.SequentialScan);

                while (!cancellationToken.IsCancellationRequested)
                {
                    // Allocate memory from the PipeWriter
                    Memory<byte> memory = writer.GetMemory(minimumBufferSize);
                    
                    int bytesRead = await stream.ReadAsync(memory, cancellationToken);
                    if (bytesRead == 0)
                    {
                        break; // EOF
                    }

                    // Tell the PipeWriter how much was read
                    writer.Advance(bytesRead);

                    // Make the data available to the PipeReader
                    FlushResult result = await writer.FlushAsync(cancellationToken);

                    // If the reader is completed, we stop writing
                    if (result.IsCompleted)
                    {
                        break;
                    }
                }
            }
            catch (Exception ex)
            {
                writer.Complete(ex);
                throw;
            }

            // Signal that we're done producing
            writer.Complete();
        }

        private async IAsyncEnumerable<DataFrame> ConsumePipeAsync(PipeReader reader, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            bool bomChecked = false;
            bool headerParsed = false;
            List<string> allHeaders = null!;
            bool schemaInferred = false;
            Type[] allTypes = null!;
            IColumnBuilder?[] builders = null!;

            while (!cancellationToken.IsCancellationRequested)
            {
                ReadResult result = await reader.ReadAsync(cancellationToken);
                ReadOnlySequence<byte> buffer = result.Buffer;

                if (!bomChecked && buffer.Length > 0)
                {
                    if (!TryConsumeBom(ref buffer, result.IsCompleted))
                    {
                        reader.AdvanceTo(buffer.Start, buffer.End);
                        continue;
                    }
                    bomChecked = true;
                }

                if (!headerParsed && buffer.Length > 0)
                {
                    if (CsvParser.TryReadLine(ref buffer, out ReadOnlySequence<byte> line, result.IsCompleted))
                    {
                        allHeaders = ParseHeaderFromSequence(line);
                        headerParsed = true;
                    }
                }

                if (headerParsed && !schemaInferred && buffer.Length > 0)
                {
                    // Peek the next line to infer schema
                    var tempBuffer = buffer;
                    if (CsvParser.TryReadLine(ref tempBuffer, out ReadOnlySequence<byte> line, result.IsCompleted))
                    {
                        var firstRowTokens = ParseHeaderFromSequence(line);

                        allTypes = new Type[allHeaders.Count];
                        for (int i = 0; i < allHeaders.Count; i++)
                        {
                            string token = i < firstRowTokens.Count ? firstRowTokens[i] : "";
                            if (int.TryParse(token, out _)) allTypes[i] = typeof(int);
                            else if (double.TryParse(token, System.Globalization.CultureInfo.InvariantCulture, out _)) allTypes[i] = typeof(double);
                            else allTypes[i] = typeof(string);
                        }
                        schemaInferred = true;
                        builders = CreateBuilders(allHeaders, allTypes, _columns);
                    }
                }

                if (schemaInferred && buffer.Length > 0)
                {
                    int rowCount = 0;
                    while (CsvParser.TryReadLine(ref buffer, out ReadOnlySequence<byte> line, result.IsCompleted))
                    {
                        ParseRowFromSequence(line, builders);
                        rowCount++;
                        _totalRowsRead++;

                        if (_nRows.HasValue && _totalRowsRead >= _nRows.Value)
                        {
                            break;
                        }
                    }

                    if (rowCount > 0 || result.IsCompleted)
                    {
                        // Build DataFrame for this chunk
                        var activeBuilders = builders.Where(b => b != null).ToArray();
                        var seriesList = new ISeries[activeBuilders.Length];
                        for (int i = 0; i < activeBuilders.Length; i++)
                        {
                            seriesList[i] = activeBuilders[i]!.Build();
                        }
                        
                        var df = new DataFrame(seriesList);
                        if (df.Columns.Count > 0 && df.Columns[0].Length > 0)
                        {
                            yield return df;
                        }

                        // Reset builders for next chunk
                        foreach (var builder in builders) builder?.Dispose();
                        builders = CreateBuilders(allHeaders, allTypes, _columns);
                    }
                }

                reader.AdvanceTo(buffer.Start, buffer.End);

                if (result.IsCompleted || (_nRows.HasValue && _totalRowsRead >= _nRows.Value))
                {
                    break;
                }
            }

            if (builders != null)
            {
                foreach (var builder in builders) builder?.Dispose();
            }
        }

        private static bool TryConsumeBom(ref ReadOnlySequence<byte> buffer, bool isCompleted)
        {
            if (buffer.Length >= 3)
            {
                Span<byte> bom = stackalloc byte[3];
                buffer.Slice(0, 3).CopyTo(bom);
                if (bom[0] == 0xEF && bom[1] == 0xBB && bom[2] == 0xBF)
                {
                    buffer = buffer.Slice(3);
                }
                return true;
            }
            return isCompleted;
        }

        private static List<string> ParseHeaderFromSequence(ReadOnlySequence<byte> sequence)
        {
            if (sequence.IsSingleSegment)
                return CsvParser.ParseHeader(sequence.FirstSpan);

            int length = checked((int)sequence.Length);
            byte[] rented = ArrayPool<byte>.Shared.Rent(length);
            try
            {
                sequence.CopyTo(rented);
                return CsvParser.ParseHeader(rented.AsSpan(0, length));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }

        private static void ParseRowFromSequence(ReadOnlySequence<byte> sequence, IColumnBuilder?[] builders)
        {
            if (sequence.IsSingleSegment)
            {
                CsvParser.ParseRow(sequence.FirstSpan, builders);
                return;
            }

            int length = checked((int)sequence.Length);
            byte[] rented = ArrayPool<byte>.Shared.Rent(length);
            try
            {
                sequence.CopyTo(rented);
                CsvParser.ParseRow(rented.AsSpan(0, length), builders);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }

        private IColumnBuilder?[] CreateBuilders(List<string> headers, Type[] types, string[]? columns)
        {
            var builders = new IColumnBuilder?[headers.Count];
            for (int i = 0; i < headers.Count; i++)
            {
                if (columns != null && !columns.Contains(headers[i]))
                {
                    builders[i] = null;
                    continue;
                }

                if (types[i] == typeof(int)) builders[i] = new Int32ColumnBuilder(headers[i]);
                else if (types[i] == typeof(double)) builders[i] = new Float64ColumnBuilder(headers[i]);
                else builders[i] = new Utf8StringColumnBuilder(headers[i]);
            }
            return builders;
        }
    }
}
