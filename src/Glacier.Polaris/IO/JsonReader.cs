using System;
using System.Buffers;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Pipelines;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.IO
{
    /// <summary>
    /// Streaming multi-column JSON and NDJSON reader built on <see cref="PipeReader"/> and <see cref="Utf8JsonReader"/>.
    /// Supports both newline-delimited JSON objects (with or without a trailing newline) and top-level JSON object arrays,
    /// automatic numeric/string type promotion, null masks, exact row-count slicing, and optional column projection.
    /// </summary>
    public sealed class JsonReader
    {
        private readonly string _filePath;
        private readonly int _chunkSize;
        private readonly string[]? _columns;
        private readonly HashSet<string>? _projectionSet;

        public JsonReader(string filePath, int chunkSize = 10000, string[]? columns = null)
        {
            if (string.IsNullOrEmpty(filePath))
                throw new ArgumentException("File path must not be null or empty.", nameof(filePath));
            if (chunkSize <= 0)
                throw new ArgumentOutOfRangeException(nameof(chunkSize), "Chunk size must be positive.");

            _filePath = filePath;
            _chunkSize = chunkSize;
            _columns = columns;
            if (columns != null)
            {
                _projectionSet = new HashSet<string>(columns, StringComparer.Ordinal);
            }
        }

        /// <summary>
        /// Peeks the first JSON object(s) in an NDJSON or JSON-array file to discover column names.
        /// </summary>
        public static string[] PeekHeaders(string filePath)
        {
            using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            int maxProbeBytes = (int)Math.Min(fs.Length, 256 * 1024);
            if (maxProbeBytes == 0) return Array.Empty<string>();

            byte[] buffer = new byte[maxProbeBytes];
            int totalRead = 0;
            while (totalRead < maxProbeBytes)
            {
                int n = fs.Read(buffer, totalRead, maxProbeBytes - totalRead);
                if (n == 0) break;
                totalRead += n;
            }

            ReadOnlySpan<byte> span = buffer.AsSpan(0, totalRead);
            if (span.Length >= 3 && span[0] == 0xEF && span[1] == 0xBB && span[2] == 0xBF)
            {
                span = span.Slice(3);
            }
            span = TrimLeadingAsciiWhitespace(span);
            if (span.IsEmpty) return Array.Empty<string>();

            var headers = new List<string>();
            var seen = new HashSet<string>(StringComparer.Ordinal);

            if (span[0] == (byte)'[')
            {
                var reader = new Utf8JsonReader(span, isFinalBlock: totalRead == fs.Length, state: default);
                int objectsVisited = 0;
                try
                {
                    while (reader.Read())
                    {
                        if (reader.TokenType == JsonTokenType.PropertyName && reader.CurrentDepth == 2)
                        {
                            string prop = reader.GetString()!;
                            if (seen.Add(prop)) headers.Add(prop);
                        }
                        else if (reader.TokenType == JsonTokenType.EndObject && reader.CurrentDepth == 1)
                        {
                            objectsVisited++;
                            if (objectsVisited >= 32) break;
                        }
                    }
                }
                catch (JsonException)
                {
                    // Partial probe buffer on large JSON array is expected
                }
            }
            else
            {
                int objectsVisited = 0;
                while (!span.IsEmpty && objectsVisited < 32)
                {
                    int nl = span.IndexOf((byte)'\n');
                    ReadOnlySpan<byte> line;
                    if (nl >= 0)
                    {
                        line = span.Slice(0, nl);
                        span = span.Slice(nl + 1);
                    }
                    else
                    {
                        if (totalRead < fs.Length) break;
                        line = span;
                        span = ReadOnlySpan<byte>.Empty;
                    }

                    line = TrimAsciiWhitespace(line);
                    if (line.IsEmpty) continue;

                    var reader = new Utf8JsonReader(line, isFinalBlock: true, state: default);
                    while (reader.Read())
                    {
                        if (reader.TokenType == JsonTokenType.PropertyName && reader.CurrentDepth == 1)
                        {
                            string prop = reader.GetString()!;
                            if (seen.Add(prop)) headers.Add(prop);
                        }
                    }
                    objectsVisited++;
                }
            }

            return headers.ToArray();
        }

        /// <summary>
        /// Reads the entire JSON or NDJSON file eagerly into a single <see cref="DataFrame"/>.
        /// </summary>
        public static DataFrame Read(string filePath, string[]? columns = null)
        {
            byte[] bytes = File.ReadAllBytes(filePath);
            ReadOnlySpan<byte> span = bytes;
            if (span.Length >= 3 && span[0] == 0xEF && span[1] == 0xBB && span[2] == 0xBF)
            {
                span = span.Slice(3);
            }
            span = TrimLeadingAsciiWhitespace(span);

            HashSet<string>? projSet = columns != null ? new HashSet<string>(columns, StringComparer.Ordinal) : null;
            var batch = new ChunkAccumulator(256, columns, projSet, null);
            if (span.IsEmpty)
            {
                return batch.RowCount == 0 && columns == null ? new DataFrame() : batch.Build();
            }

            var rowScratch = new List<ParsedCell>(16);
            if (span[0] == (byte)'[')
            {
                var reader = new Utf8JsonReader(span, isFinalBlock: true, state: default);
                if (!reader.Read() || reader.TokenType != JsonTokenType.StartArray)
                    throw new JsonException("Expected top-level JSON array.");

                while (reader.Read())
                {
                    if (reader.TokenType == JsonTokenType.EndArray && reader.CurrentDepth == 0)
                        break;
                    if (reader.TokenType != JsonTokenType.StartObject)
                        throw new JsonException($"Expected JSON object inside array, found {reader.TokenType}.");

                    rowScratch.Clear();
                    if (!TryParseObjectFields(ref reader, targetDepth: 1, projSet, rowScratch))
                        throw new JsonException("Incomplete JSON object in array.");
                    batch.AppendRow(rowScratch);
                }
            }
            else
            {
                while (!span.IsEmpty)
                {
                    int nl = span.IndexOf((byte)'\n');
                    ReadOnlySpan<byte> line;
                    if (nl >= 0)
                    {
                        line = span.Slice(0, nl);
                        span = span.Slice(nl + 1);
                    }
                    else
                    {
                        line = span;
                        span = ReadOnlySpan<byte>.Empty;
                    }

                    line = TrimAsciiWhitespace(line);
                    if (line.IsEmpty) continue;

                    var reader = new Utf8JsonReader(line, isFinalBlock: true, state: default);
                    if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
                        throw new JsonException("Expected JSON object on NDJSON line.");

                    rowScratch.Clear();
                    if (!TryParseObjectFields(ref reader, targetDepth: 0, projSet, rowScratch))
                        throw new JsonException("Incomplete JSON object on NDJSON line.");
                    batch.AppendRow(rowScratch);
                }
            }

            if (batch.RowCount == 0 && columns == null)
                return new DataFrame();
            return batch.Build();
        }

        public IAsyncEnumerable<DataFrame> ReadAsync(CancellationToken cancellationToken = default)
            => ReadNdJsonAsync(cancellationToken);

        public async IAsyncEnumerable<DataFrame> ReadNdJsonAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await using var stream = new FileStream(
                _filePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite,
                bufferSize: 65536,
                FileOptions.Asynchronous | FileOptions.SequentialScan);

            var reader = PipeReader.Create(stream, new StreamPipeReaderOptions(bufferSize: 65536));
            var knownColumnKinds = new Dictionary<string, ColumnKind>(StringComparer.Ordinal);
            var rowScratch = new List<ParsedCell>(16);

            bool bomChecked = false;
            bool formatDetected = false;
            bool isJsonArray = false;
            bool arrayStarted = false;
            bool arrayCompleted = false;
            JsonReaderState jsonState = default;

            var currentBatch = new ChunkAccumulator(Math.Min(_chunkSize, 256), _columns, _projectionSet, knownColumnKinds);
            bool yieldedAny = false;
            byte[] bom = new byte[3];

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    ReadResult result = await reader.ReadAsync(cancellationToken);
                    ReadOnlySequence<byte> buffer = result.Buffer;

                    if (!bomChecked && buffer.Length > 0)
                    {
                        if (buffer.Length >= 3)
                        {
                            buffer.Slice(0, 3).CopyTo(bom);
                            if (bom[0] == 0xEF && bom[1] == 0xBB && bom[2] == 0xBF)
                            {
                                buffer = buffer.Slice(3);
                            }
                            bomChecked = true;
                        }
                        else if (result.IsCompleted)
                        {
                            bomChecked = true;
                        }
                        else
                        {
                            reader.AdvanceTo(buffer.Start, buffer.End);
                            continue;
                        }
                    }

                    if (!formatDetected && buffer.Length > 0)
                    {
                        if (TrySkipWhitespace(ref buffer, out byte firstByte))
                        {
                            isJsonArray = firstByte == (byte)'[';
                            formatDetected = true;
                        }
                        else if (!result.IsCompleted)
                        {
                            reader.AdvanceTo(buffer.End, buffer.End);
                            continue;
                        }
                    }

                    if (formatDetected && buffer.Length > 0)
                    {
                        if (isJsonArray)
                        {
                            while (!arrayCompleted)
                            {
                                bool chunkFull = ParseJsonArrayFromSequence(
                                    ref buffer,
                                    result.IsCompleted,
                                    ref jsonState,
                                    ref arrayStarted,
                                    ref arrayCompleted,
                                    currentBatch,
                                    _chunkSize,
                                    _projectionSet,
                                    rowScratch);

                                if (chunkFull && currentBatch.RowCount > 0)
                                {
                                    yieldedAny = true;
                                    yield return currentBatch.Build();
                                    currentBatch = new ChunkAccumulator(Math.Min(_chunkSize, 256), _columns, _projectionSet, knownColumnKinds);
                                    if (buffer.IsEmpty) break;
                                }
                                else
                                {
                                    break;
                                }
                            }
                        }
                        else
                        {
                            while (!buffer.IsEmpty)
                            {
                                bool chunkFull = ParseNdJsonFromSequence(
                                    ref buffer,
                                    result.IsCompleted,
                                    currentBatch,
                                    _chunkSize,
                                    _projectionSet,
                                    rowScratch);

                                if (chunkFull && currentBatch.RowCount > 0)
                                {
                                    yieldedAny = true;
                                    yield return currentBatch.Build();
                                    currentBatch = new ChunkAccumulator(Math.Min(_chunkSize, 256), _columns, _projectionSet, knownColumnKinds);
                                }
                                else
                                {
                                    break;
                                }
                            }
                        }
                    }

                    if (result.IsCompleted)
                    {
                        reader.AdvanceTo(buffer.End);
                        break;
                    }

                    reader.AdvanceTo(buffer.Start, buffer.End);
                }

                if (currentBatch.RowCount > 0)
                {
                    yieldedAny = true;
                    yield return currentBatch.Build();
                }
                else if (!yieldedAny && _columns != null && _columns.Length > 0)
                {
                    yield return currentBatch.Build();
                }
            }
            finally
            {
                await reader.CompleteAsync();
            }
        }

        private static bool ParseNdJsonFromSequence(
            ref ReadOnlySequence<byte> buffer,
            bool isFinalBlock,
            ChunkAccumulator batch,
            int chunkSize,
            HashSet<string>? projSet,
            List<ParsedCell> rowScratch)
        {
            var seqReader = new SequenceReader<byte>(buffer);
            while (batch.RowCount < chunkSize)
            {
                if (seqReader.TryReadTo(out ReadOnlySequence<byte> lineSeq, (byte)'\n'))
                {
                    ParseSingleNdJsonLine(lineSeq, projSet, batch, rowScratch);
                }
                else
                {
                    if (isFinalBlock && seqReader.Remaining > 0)
                    {
                        var tailSeq = buffer.Slice(seqReader.Position);
                        seqReader.AdvanceToEnd();
                        ParseSingleNdJsonLine(tailSeq, projSet, batch, rowScratch);
                    }
                    break;
                }
            }

            buffer = buffer.Slice(seqReader.Position);
            return batch.RowCount >= chunkSize;
        }

        private static void ParseSingleNdJsonLine(
            ReadOnlySequence<byte> lineSeq,
            HashSet<string>? projSet,
            ChunkAccumulator batch,
            List<ParsedCell> rowScratch)
        {
            if (lineSeq.IsSingleSegment)
            {
                ReadOnlySpan<byte> line = TrimAsciiWhitespace(lineSeq.FirstSpan);
                if (line.IsEmpty) return;

                var reader = new Utf8JsonReader(line, isFinalBlock: true, state: default);
                if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
                    throw new JsonException("Expected JSON object on NDJSON line.");

                rowScratch.Clear();
                if (!TryParseObjectFields(ref reader, targetDepth: 0, projSet, rowScratch))
                    throw new JsonException("Incomplete JSON object on NDJSON line.");
                batch.AppendRow(rowScratch);
            }
            else
            {
                int len = checked((int)lineSeq.Length);
                byte[] rented = ArrayPool<byte>.Shared.Rent(len);
                try
                {
                    lineSeq.CopyTo(rented);
                    ReadOnlySpan<byte> line = TrimAsciiWhitespace(rented.AsSpan(0, len));
                    if (line.IsEmpty) return;

                    var reader = new Utf8JsonReader(line, isFinalBlock: true, state: default);
                    if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
                        throw new JsonException("Expected JSON object on NDJSON line.");

                    rowScratch.Clear();
                    if (!TryParseObjectFields(ref reader, targetDepth: 0, projSet, rowScratch))
                        throw new JsonException("Incomplete JSON object on NDJSON line.");
                    batch.AppendRow(rowScratch);
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(rented);
                }
            }
        }

        private static bool ParseJsonArrayFromSequence(
            ref ReadOnlySequence<byte> buffer,
            bool isFinalBlock,
            ref JsonReaderState jsonState,
            ref bool arrayStarted,
            ref bool arrayCompleted,
            ChunkAccumulator batch,
            int chunkSize,
            HashSet<string>? projSet,
            List<ParsedCell> rowScratch)
        {
            var reader = new Utf8JsonReader(buffer, isFinalBlock, jsonState);

            if (!arrayStarted)
            {
                if (!reader.Read())
                {
                    jsonState = reader.CurrentState;
                    buffer = buffer.Slice(reader.BytesConsumed);
                    return false;
                }
                if (reader.TokenType != JsonTokenType.StartArray)
                    throw new JsonException("Expected top-level JSON array.");
                arrayStarted = true;
            }

            long committedBytes = reader.BytesConsumed;
            JsonReaderState committedState = reader.CurrentState;

            while (batch.RowCount < chunkSize)
            {
                if (!reader.Read())
                {
                    jsonState = committedState;
                    buffer = buffer.Slice(committedBytes);
                    return false;
                }

                if (reader.TokenType == JsonTokenType.EndArray && reader.CurrentDepth == 0)
                {
                    committedBytes = reader.BytesConsumed;
                    committedState = reader.CurrentState;
                    arrayCompleted = true;
                    break;
                }

                if (reader.TokenType != JsonTokenType.StartObject)
                    throw new JsonException($"Expected JSON object in array, found {reader.TokenType}.");

                rowScratch.Clear();
                if (!TryParseObjectFields(ref reader, targetDepth: 1, projSet, rowScratch))
                {
                    if (isFinalBlock)
                        throw new JsonException("Incomplete JSON object at end of JSON array.");
                    jsonState = committedState;
                    buffer = buffer.Slice(committedBytes);
                    return false;
                }

                batch.AppendRow(rowScratch);
                committedBytes = reader.BytesConsumed;
                committedState = reader.CurrentState;
            }

            jsonState = committedState;
            buffer = buffer.Slice(committedBytes);
            return batch.RowCount >= chunkSize;
        }

        private static bool TryParseObjectFields(
            ref Utf8JsonReader reader,
            int targetDepth,
            HashSet<string>? projSet,
            List<ParsedCell> output)
        {
            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.EndObject && reader.CurrentDepth == targetDepth)
                {
                    return true;
                }

                if (reader.TokenType != JsonTokenType.PropertyName)
                {
                    continue;
                }

                string propName = reader.GetString()!;
                bool include = projSet == null || projSet.Contains(propName);

                if (!reader.Read())
                {
                    return false;
                }

                if (!include)
                {
                    if (reader.TokenType == JsonTokenType.StartObject || reader.TokenType == JsonTokenType.StartArray)
                    {
                        if (!reader.TrySkip()) return false;
                    }
                    continue;
                }

                switch (reader.TokenType)
                {
                    case JsonTokenType.Null:
                        output.Add(ParsedCell.CreateNull(propName));
                        break;

                    case JsonTokenType.True:
                        output.Add(ParsedCell.CreateBool(propName, true));
                        break;

                    case JsonTokenType.False:
                        output.Add(ParsedCell.CreateBool(propName, false));
                        break;

                    case JsonTokenType.Number:
                        if (reader.TryGetInt32(out int iv))
                        {
                            output.Add(ParsedCell.CreateInt32(propName, iv));
                        }
                        else if (reader.TryGetInt64(out long lv))
                        {
                            output.Add(ParsedCell.CreateInt64(propName, lv));
                        }
                        else
                        {
                            output.Add(ParsedCell.CreateFloat64(propName, reader.GetDouble()));
                        }
                        break;

                    case JsonTokenType.String:
                        output.Add(ParsedCell.CreateString(propName, ExtractUtf8Bytes(ref reader)));
                        break;

                    case JsonTokenType.StartObject:
                    case JsonTokenType.StartArray:
                        if (!TryCaptureRawNestedJson(ref reader, out byte[] rawJson))
                            return false;
                        output.Add(ParsedCell.CreateString(propName, rawJson));
                        break;
                }
            }

            return false;
        }

        private static byte[] ExtractUtf8Bytes(ref Utf8JsonReader reader)
        {
            if (!reader.ValueIsEscaped && !reader.HasValueSequence)
            {
                return reader.ValueSpan.ToArray();
            }
            string s = reader.GetString() ?? string.Empty;
            return Encoding.UTF8.GetBytes(s);
        }

        private static bool TryCaptureRawNestedJson(ref Utf8JsonReader reader, out byte[] rawBytes)
        {
            using var ms = new MemoryStream();
            using (var writer = new Utf8JsonWriter(ms))
            {
                if (!TryCopyJsonElement(ref reader, writer))
                {
                    rawBytes = Array.Empty<byte>();
                    return false;
                }
            }
            rawBytes = ms.ToArray();
            return true;
        }

        private static bool TryCopyJsonElement(ref Utf8JsonReader reader, Utf8JsonWriter writer)
        {
            switch (reader.TokenType)
            {
                case JsonTokenType.StartObject:
                    writer.WriteStartObject();
                    while (true)
                    {
                        if (!reader.Read()) return false;
                        if (reader.TokenType == JsonTokenType.EndObject)
                        {
                            writer.WriteEndObject();
                            return true;
                        }
                        string prop = reader.GetString()!;
                        writer.WritePropertyName(prop);
                        if (!reader.Read()) return false;
                        if (!TryCopyJsonElement(ref reader, writer)) return false;
                    }

                case JsonTokenType.StartArray:
                    writer.WriteStartArray();
                    while (true)
                    {
                        if (!reader.Read()) return false;
                        if (reader.TokenType == JsonTokenType.EndArray)
                        {
                            writer.WriteEndArray();
                            return true;
                        }
                        if (!TryCopyJsonElement(ref reader, writer)) return false;
                    }

                case JsonTokenType.String:
                    writer.WriteStringValue(reader.GetString());
                    return true;
                case JsonTokenType.Number:
                    if (reader.TryGetInt64(out long l)) writer.WriteNumberValue(l);
                    else writer.WriteNumberValue(reader.GetDouble());
                    return true;
                case JsonTokenType.True:
                    writer.WriteBooleanValue(true);
                    return true;
                case JsonTokenType.False:
                    writer.WriteBooleanValue(false);
                    return true;
                case JsonTokenType.Null:
                    writer.WriteNullValue();
                    return true;
                default:
                    return false;
            }
        }

        private static bool TrySkipWhitespace(ref ReadOnlySequence<byte> buffer, out byte firstNonWhitespace)
        {
            var sr = new SequenceReader<byte>(buffer);
            while (sr.TryPeek(out byte b))
            {
                if (b == (byte)' ' || b == (byte)'\t' || b == (byte)'\r' || b == (byte)'\n')
                {
                    sr.Advance(1);
                    continue;
                }
                firstNonWhitespace = b;
                buffer = buffer.Slice(sr.Position);
                return true;
            }
            firstNonWhitespace = 0;
            buffer = buffer.Slice(sr.Position);
            return false;
        }

        private static ReadOnlySpan<byte> TrimLeadingAsciiWhitespace(ReadOnlySpan<byte> span)
        {
            int i = 0;
            while (i < span.Length && (span[i] == (byte)' ' || span[i] == (byte)'\t' || span[i] == (byte)'\r' || span[i] == (byte)'\n'))
            {
                i++;
            }
            return span.Slice(i);
        }

        private static ReadOnlySpan<byte> TrimAsciiWhitespace(ReadOnlySpan<byte> span)
        {
            int start = 0;
            while (start < span.Length && (span[start] == (byte)' ' || span[start] == (byte)'\t' || span[start] == (byte)'\r' || span[start] == (byte)'\n'))
            {
                start++;
            }
            int end = span.Length - 1;
            while (end >= start && (span[end] == (byte)' ' || span[end] == (byte)'\t' || span[end] == (byte)'\r' || span[end] == (byte)'\n'))
            {
                end--;
            }
            return start <= end ? span.Slice(start, end - start + 1) : ReadOnlySpan<byte>.Empty;
        }

        private enum ColumnKind : byte
        {
            Unknown = 0,
            Int32 = 1,
            Int64 = 2,
            Float64 = 3,
            Boolean = 4,
            String = 5
        }

        private readonly struct ParsedCell
        {
            public readonly string Name;
            public readonly ColumnKind Kind;
            public readonly long IntVal;
            public readonly double FloatVal;
            public readonly byte[]? BytesVal;

            private ParsedCell(string name, ColumnKind kind, long intVal, double floatVal, byte[]? bytesVal)
            {
                Name = name;
                Kind = kind;
                IntVal = intVal;
                FloatVal = floatVal;
                BytesVal = bytesVal;
            }

            public static ParsedCell CreateNull(string name) => new(name, ColumnKind.Unknown, 0, 0, null);
            public static ParsedCell CreateBool(string name, bool v) => new(name, ColumnKind.Boolean, v ? 1 : 0, 0, null);
            public static ParsedCell CreateInt32(string name, int v) => new(name, ColumnKind.Int32, v, 0, null);
            public static ParsedCell CreateInt64(string name, long v) => new(name, ColumnKind.Int64, v, 0, null);
            public static ParsedCell CreateFloat64(string name, double v) => new(name, ColumnKind.Float64, 0, v, null);
            public static ParsedCell CreateString(string name, byte[] utf8) => new(name, ColumnKind.String, 0, 0, utf8);
        }

        private sealed class ChunkAccumulator
        {
            private readonly List<ColumnAccumulator> _columns = new();
            private readonly Dictionary<string, int> _colIndex = new(StringComparer.Ordinal);
            private readonly HashSet<string>? _projectionSet;
            private readonly Dictionary<string, ColumnKind>? _sharedKinds;
            private readonly int _initialCap;
            public int RowCount { get; private set; }

            public ChunkAccumulator(
                int initialCapacity,
                string[]? projectedColumns,
                HashSet<string>? projectionSet,
                Dictionary<string, ColumnKind>? sharedKinds)
            {
                _initialCap = Math.Max(4, initialCapacity);
                _projectionSet = projectionSet;
                _sharedKinds = sharedKinds;

                if (projectedColumns != null)
                {
                    foreach (var colName in projectedColumns)
                    {
                        GetOrCreateColumn(colName);
                    }
                }
            }

            private ColumnAccumulator GetOrCreateColumn(string name)
            {
                if (_colIndex.TryGetValue(name, out int idx))
                {
                    return _columns[idx];
                }

                ColumnKind initialKind = ColumnKind.Unknown;
                if (_sharedKinds != null && _sharedKinds.TryGetValue(name, out var prevKind))
                {
                    initialKind = prevKind;
                }

                var col = new ColumnAccumulator(name, Math.Max(_initialCap, RowCount + 1), initialKind, RowCount);
                _colIndex[name] = _columns.Count;
                _columns.Add(col);
                return col;
            }

            public void AppendRow(List<ParsedCell> cells)
            {
                int row = RowCount;
                for (int i = 0; i < cells.Count; i++)
                {
                    var cell = cells[i];
                    if (_projectionSet != null && !_projectionSet.Contains(cell.Name))
                        continue;

                    var col = GetOrCreateColumn(cell.Name);
                    col.SetCell(row, in cell);
                }

                RowCount = row + 1;
                for (int c = 0; c < _columns.Count; c++)
                {
                    _columns[c].FinalizeRow(row);
                }
            }

            public DataFrame Build()
            {
                var seriesList = new List<ISeries>(_columns.Count);
                for (int i = 0; i < _columns.Count; i++)
                {
                    var col = _columns[i];
                    if (_sharedKinds != null && col.Kind != ColumnKind.Unknown)
                    {
                        _sharedKinds[col.Name] = col.Kind;
                    }
                    seriesList.Add(col.BuildSeries(RowCount));
                }
                return new DataFrame(seriesList);
            }
        }

        private sealed class ColumnAccumulator
        {
            public string Name { get; }
            public ColumnKind Kind { get; private set; }

            private int _capacity;
            private bool[] _isNull;
            private int _writtenUpToRow;

            private int[]? _i32;
            private long[]? _i64;
            private double[]? _f64;
            private bool[]? _bools;
            private byte[][]? _strings;

            public ColumnAccumulator(string name, int initialCapacity, ColumnKind initialKind, int leadingNullRows)
            {
                Name = name;
                Kind = initialKind;
                _capacity = Math.Max(4, initialCapacity);
                _isNull = new bool[_capacity];
                Array.Fill(_isNull, true, 0, leadingNullRows);
                _writtenUpToRow = leadingNullRows;

                AllocateKindStorage(initialKind);
            }

            private void EnsureCapacity(int row)
            {
                if (row < _capacity) return;
                int newCap = _capacity * 2;
                while (newCap <= row) newCap *= 2;

                Array.Resize(ref _isNull, newCap);
                if (_i32 != null) Array.Resize(ref _i32, newCap);
                if (_i64 != null) Array.Resize(ref _i64, newCap);
                if (_f64 != null) Array.Resize(ref _f64, newCap);
                if (_bools != null) Array.Resize(ref _bools, newCap);
                if (_strings != null) Array.Resize(ref _strings, newCap);
                _capacity = newCap;
            }

            private void AllocateKindStorage(ColumnKind kind)
            {
                switch (kind)
                {
                    case ColumnKind.Int32: _i32 ??= new int[_capacity]; break;
                    case ColumnKind.Int64: _i64 ??= new long[_capacity]; break;
                    case ColumnKind.Float64: _f64 ??= new double[_capacity]; break;
                    case ColumnKind.Boolean: _bools ??= new bool[_capacity]; break;
                    case ColumnKind.String: _strings ??= new byte[_capacity][]; break;
                }
            }

            private void PromoteToInt64(int upToRow)
            {
                _i64 = new long[_capacity];
                if (_i32 != null)
                {
                    for (int r = 0; r < upToRow; r++)
                    {
                        if (!_isNull[r]) _i64[r] = _i32[r];
                    }
                    _i32 = null;
                }
                Kind = ColumnKind.Int64;
            }

            private void PromoteToFloat64(int upToRow)
            {
                _f64 = new double[_capacity];
                if (_i32 != null)
                {
                    for (int r = 0; r < upToRow; r++)
                    {
                        if (!_isNull[r]) _f64[r] = _i32[r];
                    }
                    _i32 = null;
                }
                else if (_i64 != null)
                {
                    for (int r = 0; r < upToRow; r++)
                    {
                        if (!_isNull[r]) _f64[r] = _i64[r];
                    }
                    _i64 = null;
                }
                Kind = ColumnKind.Float64;
            }

            private void PromoteToString(int upToRow)
            {
                _strings = new byte[_capacity][];
                for (int r = 0; r < upToRow; r++)
                {
                    if (_isNull[r]) continue;
                    string s = Kind switch
                    {
                        ColumnKind.Int32 => _i32![r].ToString(CultureInfo.InvariantCulture),
                        ColumnKind.Int64 => _i64![r].ToString(CultureInfo.InvariantCulture),
                        ColumnKind.Float64 => _f64![r].ToString("G17", CultureInfo.InvariantCulture),
                        ColumnKind.Boolean => _bools![r] ? "true" : "false",
                        _ => string.Empty
                    };
                    _strings[r] = Encoding.UTF8.GetBytes(s);
                }
                _i32 = null;
                _i64 = null;
                _f64 = null;
                _bools = null;
                Kind = ColumnKind.String;
            }

            public void SetCell(int row, in ParsedCell cell)
            {
                EnsureCapacity(row);
                while (_writtenUpToRow < row)
                {
                    _isNull[_writtenUpToRow++] = true;
                }

                if (cell.Kind == ColumnKind.Unknown)
                {
                    _isNull[row] = true;
                    _writtenUpToRow = row + 1;
                    return;
                }

                _isNull[row] = false;

                if (Kind == ColumnKind.Unknown)
                {
                    Kind = cell.Kind;
                    AllocateKindStorage(Kind);
                }

                switch (cell.Kind)
                {
                    case ColumnKind.Int32:
                        if (Kind == ColumnKind.Int32) _i32![row] = (int)cell.IntVal;
                        else if (Kind == ColumnKind.Int64) _i64![row] = cell.IntVal;
                        else if (Kind == ColumnKind.Float64) _f64![row] = cell.IntVal;
                        else
                        {
                            if (Kind != ColumnKind.String) PromoteToString(row);
                            _strings![row] = Encoding.UTF8.GetBytes(((int)cell.IntVal).ToString(CultureInfo.InvariantCulture));
                        }
                        break;

                    case ColumnKind.Int64:
                        if (Kind == ColumnKind.Int32) PromoteToInt64(row);
                        if (Kind == ColumnKind.Int64) _i64![row] = cell.IntVal;
                        else if (Kind == ColumnKind.Float64) _f64![row] = cell.IntVal;
                        else
                        {
                            if (Kind != ColumnKind.String) PromoteToString(row);
                            _strings![row] = Encoding.UTF8.GetBytes(cell.IntVal.ToString(CultureInfo.InvariantCulture));
                        }
                        break;

                    case ColumnKind.Float64:
                        if (Kind == ColumnKind.Int32 || Kind == ColumnKind.Int64) PromoteToFloat64(row);
                        if (Kind == ColumnKind.Float64) _f64![row] = cell.FloatVal;
                        else
                        {
                            if (Kind != ColumnKind.String) PromoteToString(row);
                            _strings![row] = Encoding.UTF8.GetBytes(cell.FloatVal.ToString("G17", CultureInfo.InvariantCulture));
                        }
                        break;

                    case ColumnKind.Boolean:
                        if (Kind == ColumnKind.Boolean) _bools![row] = cell.IntVal != 0;
                        else
                        {
                            if (Kind != ColumnKind.String) PromoteToString(row);
                            _strings![row] = cell.IntVal != 0 ? "true"u8.ToArray() : "false"u8.ToArray();
                        }
                        break;

                    case ColumnKind.String:
                        if (Kind != ColumnKind.String) PromoteToString(row);
                        _strings![row] = cell.BytesVal ?? Array.Empty<byte>();
                        break;
                }

                _writtenUpToRow = row + 1;
            }

            public void FinalizeRow(int row)
            {
                if (_writtenUpToRow <= row)
                {
                    EnsureCapacity(row);
                    while (_writtenUpToRow <= row)
                    {
                        _isNull[_writtenUpToRow++] = true;
                    }
                }
            }

            public ISeries BuildSeries(int rowCount)
            {
                switch (Kind)
                {
                    case ColumnKind.Int32:
                    {
                        var s = new Int32Series(Name, rowCount);
                        if (rowCount > 0)
                        {
                            _i32.AsSpan(0, rowCount).CopyTo(s.Memory.Span);
                            ApplyNulls(s.ValidityMask, rowCount);
                        }
                        return s;
                    }
                    case ColumnKind.Int64:
                    {
                        var s = new Int64Series(Name, rowCount);
                        if (rowCount > 0)
                        {
                            _i64.AsSpan(0, rowCount).CopyTo(s.Memory.Span);
                            ApplyNulls(s.ValidityMask, rowCount);
                        }
                        return s;
                    }
                    case ColumnKind.Float64:
                    {
                        var s = new Float64Series(Name, rowCount);
                        if (rowCount > 0)
                        {
                            _f64.AsSpan(0, rowCount).CopyTo(s.Memory.Span);
                            ApplyNulls(s.ValidityMask, rowCount);
                        }
                        return s;
                    }
                    case ColumnKind.Boolean:
                    {
                        var s = new BooleanSeries(Name, rowCount);
                        if (rowCount > 0)
                        {
                            _bools.AsSpan(0, rowCount).CopyTo(s.Memory.Span);
                            ApplyNulls(s.ValidityMask, rowCount);
                        }
                        return s;
                    }
                    case ColumnKind.String:
                    {
                        int totalBytes = 0;
                        for (int r = 0; r < rowCount; r++)
                        {
                            if (!_isNull[r] && _strings![r] != null)
                                totalBytes += _strings[r].Length;
                        }

                        var s = new Utf8StringSeries(Name, rowCount, totalBytes);
                        var offsets = s.Offsets.Span;
                        var data = s.DataBytes.Span;
                        int offset = 0;
                        for (int r = 0; r < rowCount; r++)
                        {
                            offsets[r] = offset;
                            if (_isNull[r])
                            {
                                s.ValidityMask.SetNull(r);
                            }
                            else
                            {
                                byte[] b = _strings![r] ?? Array.Empty<byte>();
                                if (b.Length > 0)
                                {
                                    b.CopyTo(data.Slice(offset));
                                    offset += b.Length;
                                }
                            }
                        }
                        offsets[rowCount] = offset;
                        return s;
                    }
                    default:
                    {
                        // All values in this column were null across the chunk
                        var s = new Utf8StringSeries(Name, rowCount, 0);
                        s.Offsets.Span.Clear();
                        if (rowCount > 0) s.ValidityMask.SetAllNull();
                        return s;
                    }
                }
            }

            private void ApplyNulls(Memory.ValidityMask mask, int rowCount)
            {
                for (int r = 0; r < rowCount; r++)
                {
                    if (_isNull[r]) mask.SetNull(r);
                }
            }
        }
    }
}
