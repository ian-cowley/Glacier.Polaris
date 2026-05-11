using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.IO
{
    /// <summary>
    /// High-performance JSON/NDJSON reader using System.IO.Pipelines and Utf8JsonReader.
    /// This reader yields DataFrames in chunks, minimizing heap allocations.
    /// </summary>
    public sealed class JsonReader
    {
        private readonly string _filePath;
        private readonly int _chunkSize;

        public JsonReader(string filePath, int chunkSize = 10000)
        {
            _filePath = filePath;
            _chunkSize = chunkSize;
        }

        public async IAsyncEnumerable<DataFrame> ReadNdJsonAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            using var stream = File.OpenRead(_filePath);
            var reader = PipeReader.Create(stream);

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    ReadResult result = await reader.ReadAsync(cancellationToken);
                    ReadOnlySequence<byte> buffer = result.Buffer;

                    if (buffer.Length > 0)
                    {
                        var df = ParseNdJsonBuffer(ref buffer);
                        if (df != null)
                        {
                            yield return df;
                        }
                    }

                    reader.AdvanceTo(buffer.Start, buffer.End);

                    if (result.IsCompleted) break;
                }
            }
            finally
            {
                await reader.CompleteAsync();
            }
        }

        private DataFrame ParseNdJsonBuffer(ref ReadOnlySequence<byte> buffer)
        {
            var series = new Int32Series("Value", _chunkSize);
            var destSpan = series.Memory.Span;
            int rowCount = 0;

            var sequenceReader = new SequenceReader<byte>(buffer);

            // NDJSON: each line is a JSON object. We look for newlines.
            while (sequenceReader.TryReadTo(out ReadOnlySequence<byte> line, (byte)'\n'))
            {
                if (rowCount >= _chunkSize) break;

                var jsonReader = new Utf8JsonReader(line);
                
                // Extremely simplified: we assume each object has a single "val" property
                while (jsonReader.Read())
                {
                    if (jsonReader.TokenType == JsonTokenType.PropertyName && jsonReader.ValueTextEquals("val"))
                    {
                        if (jsonReader.Read() && jsonReader.TokenType == JsonTokenType.Number)
                        {
                            destSpan[rowCount++] = jsonReader.GetInt32();
                        }
                    }
                }
            }

            // Update buffer to reflecting how much we consumed
            buffer = buffer.Slice(sequenceReader.Position);

            return rowCount > 0 ? new DataFrame(new[] { series }) : null!;
        }
    }
}
