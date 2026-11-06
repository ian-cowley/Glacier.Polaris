using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    /// <summary>
    /// Out-of-core K-Way External Merge Sort engine.
    /// Manages memory budgets, spills sorted runs to temporary binary files,
    /// and performs K-Way tournament merges to stream sorted batches without OOM.
    /// </summary>
    public static class ExternalMergeSort
    {
        public static long DefaultSpillBudget { get; set; } = 256L * 1024 * 1024;

        public static async IAsyncEnumerable<DataFrame> SortAsync(
            IAsyncEnumerable<DataFrame> source,
            string[] columnNames,
            bool[] descending,
            long? budgetBytes = null,
            int outputBatchSize = 65536)
        {
            long effectiveBudget = budgetBytes ?? DefaultSpillBudget;
            var currentChunks = new List<DataFrame>();
            long currentBytes = 0;
            var spillFiles = new List<string>();

            try
            {
                await foreach (var chunk in source.ConfigureAwait(false))
                {
                    if (chunk.RowCount == 0) continue;

                    currentChunks.Add(chunk);
                    currentBytes += chunk.EstimatedSize();

                    if (currentBytes >= effectiveBudget)
                    {
                        var runDf = currentChunks.Count == 1 ? currentChunks[0] : DataFrame.Concat(currentChunks);
                        var sortedRun = SortSingleRun(runDf, columnNames, descending);
                        string spillPath = Path.Combine(Path.GetTempPath(), $"polaris_sort_run_{Guid.NewGuid():N}.tmp");
                        SpillRunWriter.WriteRun(sortedRun, spillPath);
                        spillFiles.Add(spillPath);

                        currentChunks.Clear();
                        currentBytes = 0;
                    }
                }

                if (spillFiles.Count == 0)
                {
                    if (currentChunks.Count == 0) yield break;

                    var fullDf = currentChunks.Count == 1 ? currentChunks[0] : DataFrame.Concat(currentChunks);
                    var sorted = SortSingleRun(fullDf, columnNames, descending);
                    yield return sorted;
                    yield break;
                }

                if (currentChunks.Count > 0)
                {
                    var runDf = currentChunks.Count == 1 ? currentChunks[0] : DataFrame.Concat(currentChunks);
                    var sortedRun = SortSingleRun(runDf, columnNames, descending);
                    string spillPath = Path.Combine(Path.GetTempPath(), $"polaris_sort_run_{Guid.NewGuid():N}.tmp");
                    SpillRunWriter.WriteRun(sortedRun, spillPath);
                    spillFiles.Add(spillPath);
                    currentChunks.Clear();
                }

                await foreach (var outDf in MergeSpillFilesAsync(spillFiles, columnNames, descending, outputBatchSize).ConfigureAwait(false))
                {
                    yield return outDf;
                }
            }
            finally
            {
                foreach (var path in spillFiles)
                {
                    if (File.Exists(path))
                    {
                        try { File.Delete(path); } catch { }
                    }
                }
            }
        }

        private static DataFrame SortSingleRun(DataFrame df, string[] columnNames, bool[] descending)
        {
            var indices = SortKernels.MultiColumnSort(df, columnNames, descending);
            var sortedCols = new List<ISeries>(df.Columns.Count);

            foreach (var col in df.Columns)
            {
                var newCol = UniqueKernels.CreateSeriesForTake(col, indices);
                col.Take(newCol, indices);
                sortedCols.Add(newCol);
            }

            return new DataFrame(sortedCols);
        }

        private static async IAsyncEnumerable<DataFrame> MergeSpillFilesAsync(
            List<string> spillFiles,
            string[] columnNames,
            bool[] descending,
            int batchSize)
        {
            var readers = new List<SpillRunReader>(spillFiles.Count);
            foreach (var file in spillFiles)
            {
                readers.Add(new SpillRunReader(file));
            }

            try
            {
                var comparer = new CursorComparer(readers, columnNames, descending);
                var queue = new PriorityQueue<RunCursor, RunCursor>(comparer);

                for (int i = 0; i < readers.Count; i++)
                {
                    if (readers[i].RowCount > 0)
                    {
                        var cursor = new RunCursor(i, 0);
                        queue.Enqueue(cursor, cursor);
                    }
                }

                if (queue.Count == 0) yield break;

                var templateCols = readers[0].Columns;
                var selectedRows = new List<RunCursor>(batchSize);

                while (queue.Count > 0)
                {
                    var minCursor = queue.Dequeue();
                    selectedRows.Add(minCursor);

                    int nextRow = minCursor.RowIndex + 1;
                    if (nextRow < readers[minCursor.RunIndex].RowCount)
                    {
                        var nextCursor = new RunCursor(minCursor.RunIndex, nextRow);
                        queue.Enqueue(nextCursor, nextCursor);
                    }

                    if (selectedRows.Count >= batchSize)
                    {
                        yield return MaterializeBatch(readers, selectedRows, templateCols);
                        selectedRows.Clear();
                    }
                }

                if (selectedRows.Count > 0)
                {
                    yield return MaterializeBatch(readers, selectedRows, templateCols);
                }
            }
            finally
            {
                foreach (var r in readers) r.Dispose();
            }

            await Task.CompletedTask;
        }

        private static DataFrame MaterializeBatch(
            List<SpillRunReader> readers,
            List<RunCursor> selectedRows,
            List<ISeries> templateCols)
        {
            int totalRows = selectedRows.Count;
            var resultCols = new List<ISeries>(templateCols.Count);

            for (int c = 0; c < templateCols.Count; c++)
            {
                var templateCol = templateCols[c];
                var name = templateCol.Name;

                if (templateCol is Utf8StringSeries)
                {
                    int totalBytes = 0;
                    for (int i = 0; i < totalRows; i++)
                    {
                        var cursor = selectedRows[i];
                        var srcCol = (Utf8StringSeries)readers[cursor.RunIndex].Columns[c];
                        if (!srcCol.ValidityMask.IsNull(cursor.RowIndex))
                            totalBytes += srcCol.GetStringSpan(cursor.RowIndex).Length;
                    }

                    var newCol = new Utf8StringSeries(name, totalRows, totalBytes);
                    for (int i = 0; i < totalRows; i++)
                    {
                        var cursor = selectedRows[i];
                        var srcCol = (Utf8StringSeries)readers[cursor.RunIndex].Columns[c];
                        srcCol.Take(newCol, cursor.RowIndex, i);
                    }
                    resultCols.Add(newCol);
                }
                else
                {
                    var newCol = UniqueKernels.CloneWithName(templateCol, name, totalRows);
                    for (int i = 0; i < totalRows; i++)
                    {
                        var cursor = selectedRows[i];
                        var srcCol = readers[cursor.RunIndex].Columns[c];
                        srcCol.Take(newCol, cursor.RowIndex, i);
                    }
                    resultCols.Add(newCol);
                }
            }

            return new DataFrame(resultCols);
        }

        private readonly struct RunCursor
        {
            public readonly int RunIndex;
            public readonly int RowIndex;

            public RunCursor(int runIndex, int rowIndex)
            {
                RunIndex = runIndex;
                RowIndex = rowIndex;
            }
        }

        private sealed class CursorComparer : IComparer<RunCursor>
        {
            private readonly List<SpillRunReader> _readers;
            private readonly string[] _sortCols;
            private readonly bool[] _descending;
            private readonly int[][] _colIndices;

            public CursorComparer(List<SpillRunReader> readers, string[] sortCols, bool[] descending)
            {
                _readers = readers;
                _sortCols = sortCols;
                _descending = descending;
                _colIndices = new int[readers.Count][];

                for (int r = 0; r < readers.Count; r++)
                {
                    _colIndices[r] = new int[sortCols.Length];
                    for (int s = 0; s < sortCols.Length; s++)
                    {
                        _colIndices[r][s] = readers[r].Columns.FindIndex(c => c.Name.Equals(sortCols[s], StringComparison.OrdinalIgnoreCase));
                    }
                }
            }

            public int Compare(RunCursor x, RunCursor y)
            {
                for (int s = 0; s < _sortCols.Length; s++)
                {
                    int colIdxX = _colIndices[x.RunIndex][s];
                    int colIdxY = _colIndices[y.RunIndex][s];
                    if (colIdxX < 0 || colIdxY < 0) continue;

                    var colX = _readers[x.RunIndex].Columns[colIdxX];
                    var colY = _readers[y.RunIndex].Columns[colIdxY];

                    bool nullX = colX.ValidityMask.IsNull(x.RowIndex);
                    bool nullY = colY.ValidityMask.IsNull(y.RowIndex);
                    if (nullX && nullY) continue;
                    if (nullX) return 1;
                    if (nullY) return -1;

                    int cmp = CompareNonNullValues(colX, x.RowIndex, colY, y.RowIndex);
                    if (cmp != 0)
                    {
                        return _descending[s] ? -cmp : cmp;
                    }
                }

                int runCmp = x.RunIndex.CompareTo(y.RunIndex);
                return runCmp != 0 ? runCmp : x.RowIndex.CompareTo(y.RowIndex);
            }

            private static int CompareNonNullValues(ISeries colX, int rowX, ISeries colY, int rowY)
            {
                if (colX is Int32Series i32X && colY is Int32Series i32Y)
                    return i32X.Memory.Span[rowX].CompareTo(i32Y.Memory.Span[rowY]);

                if (colX is Int64Series i64X && colY is Int64Series i64Y)
                    return i64X.Memory.Span[rowX].CompareTo(i64Y.Memory.Span[rowY]);

                if (colX is Float64Series f64X && colY is Float64Series f64Y)
                    return f64X.Memory.Span[rowX].CompareTo(f64Y.Memory.Span[rowY]);

                if (colX is Float32Series f32X && colY is Float32Series f32Y)
                    return f32X.Memory.Span[rowX].CompareTo(f32Y.Memory.Span[rowY]);

                if (colX is Utf8StringSeries strX && colY is Utf8StringSeries strY)
                    return strX.GetStringSpan(rowX).SequenceCompareTo(strY.GetStringSpan(rowY));

                var valX = colX.Get(rowX) as IComparable;
                var valY = colY.Get(rowY);
                return valX?.CompareTo(valY) ?? 0;
            }
        }

        private sealed class SpillRunReader : IDisposable
        {
            public List<ISeries> Columns { get; }
            public int RowCount { get; }

            public SpillRunReader(string filePath)
            {
                var df = SpillRunWriter.ReadRun(filePath);
                Columns = df.Columns.ToList();
                RowCount = df.RowCount;
            }

            public void Dispose()
            {
                foreach (var c in Columns)
                {
                    c.Dispose();
                }
            }
        }

        private enum SpillColumnTag : byte
        {
            Utf8String = 1,
            Int32 = 2,
            Int64 = 3,
            Float64 = 4,
            Float32 = 5,
            Boolean = 6,
            Int8 = 7,
            UInt8 = 8,
            Int16 = 9,
            UInt16 = 10,
            UInt32 = 11,
            UInt64 = 12,
            Date = 13,
            Datetime = 14,
            Decimal = 15,
            Duration = 16,
            Time = 17,
            Binary = 18,
            Null = 19,
            FallbackUtf8 = 255
        }

        private static class SpillRunWriter
        {
            private const uint Magic = 0x53504C53; // 'SPLS' v2 tagged format

            private static SpillColumnTag GetTag(ISeries col) => col switch
            {
                Utf8StringSeries => SpillColumnTag.Utf8String,
                Int32Series => SpillColumnTag.Int32,
                Int64Series => SpillColumnTag.Int64,
                Float64Series => SpillColumnTag.Float64,
                Float32Series => SpillColumnTag.Float32,
                BooleanSeries => SpillColumnTag.Boolean,
                Int8Series => SpillColumnTag.Int8,
                UInt8Series => SpillColumnTag.UInt8,
                Int16Series => SpillColumnTag.Int16,
                UInt16Series => SpillColumnTag.UInt16,
                UInt32Series => SpillColumnTag.UInt32,
                UInt64Series => SpillColumnTag.UInt64,
                DateSeries => SpillColumnTag.Date,
                DatetimeSeries => SpillColumnTag.Datetime,
                DecimalSeries => SpillColumnTag.Decimal,
                DurationSeries => SpillColumnTag.Duration,
                TimeSeries => SpillColumnTag.Time,
                BinarySeries => SpillColumnTag.Binary,
                NullSeries => SpillColumnTag.Null,
                _ => SpillColumnTag.FallbackUtf8
            };

            public static void WriteRun(DataFrame df, string filePath)
            {
                using var fs = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None, 65536);
                using var writer = new BinaryWriter(fs);

                writer.Write(Magic);
                writer.Write(df.RowCount);
                writer.Write(df.Columns.Count);

                foreach (var col in df.Columns)
                {
                    var tag = GetTag(col);
                    writer.Write(col.Name);
                    writer.Write((byte)tag);
                    writer.Write(col.Length);

                    writer.Write(col.ValidityMask.HasNulls);
                    if (col.ValidityMask.HasNulls)
                    {
                        int wordCount = col.ValidityMask.WordCount;
                        writer.Write(wordCount);
                        for (int w = 0; w < wordCount; w++)
                        {
                            writer.Write(col.ValidityMask.GetWord(w));
                        }
                    }

                    switch (col)
                    {
                        case Utf8StringSeries u8:
                        {
                            writer.Write(u8.DataBytes.Length);
                            writer.Write(u8.DataBytes.Span);
                            var offsets = MemoryMarshal.AsBytes(u8.Offsets.Span);
                            writer.Write(offsets.Length);
                            writer.Write(offsets);
                            break;
                        }
                        case BinarySeries bin:
                        {
                            writer.Write(bin.DataBytes.Length);
                            writer.Write(bin.DataBytes.Span);
                            var offsets = MemoryMarshal.AsBytes(bin.Offsets.Span);
                            writer.Write(offsets.Length);
                            writer.Write(offsets);
                            break;
                        }
                        case Int32Series i32:
                            writer.Write(MemoryMarshal.AsBytes(i32.Memory.Span));
                            break;
                        case Int64Series i64:
                            writer.Write(MemoryMarshal.AsBytes(i64.Memory.Span));
                            break;
                        case Float64Series f64:
                            writer.Write(MemoryMarshal.AsBytes(f64.Memory.Span));
                            break;
                        case Float32Series f32:
                            writer.Write(MemoryMarshal.AsBytes(f32.Memory.Span));
                            break;
                        case BooleanSeries b:
                            writer.Write(MemoryMarshal.AsBytes(b.Memory.Span));
                            break;
                        case Int8Series i8:
                            writer.Write(MemoryMarshal.AsBytes(i8.Memory.Span));
                            break;
                        case UInt8Series ui8:
                            writer.Write(MemoryMarshal.AsBytes(ui8.Memory.Span));
                            break;
                        case Int16Series i16:
                            writer.Write(MemoryMarshal.AsBytes(i16.Memory.Span));
                            break;
                        case UInt16Series ui16:
                            writer.Write(MemoryMarshal.AsBytes(ui16.Memory.Span));
                            break;
                        case UInt32Series ui32:
                            writer.Write(MemoryMarshal.AsBytes(ui32.Memory.Span));
                            break;
                        case UInt64Series ui64:
                            writer.Write(MemoryMarshal.AsBytes(ui64.Memory.Span));
                            break;
                        case DateSeries dt:
                            writer.Write(MemoryMarshal.AsBytes(dt.Memory.Span));
                            break;
                        case DatetimeSeries dtm:
                            writer.Write(MemoryMarshal.AsBytes(dtm.Memory.Span));
                            break;
                        case DecimalSeries dec:
                            writer.Write(MemoryMarshal.AsBytes(dec.Memory.Span));
                            break;
                        case DurationSeries dur:
                            writer.Write(MemoryMarshal.AsBytes(dur.Memory.Span));
                            break;
                        case TimeSeries tm:
                            writer.Write(MemoryMarshal.AsBytes(tm.Memory.Span));
                            break;
                        case NullSeries:
                            break;
                        default:
                        {
                            for (int i = 0; i < col.Length; i++)
                            {
                                var v = col.Get(i);
                                writer.Write(v?.ToString() ?? string.Empty);
                            }
                            break;
                        }
                    }
                }
            }

            public static DataFrame ReadRun(string filePath)
            {
                using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 65536);
                using var reader = new BinaryReader(fs);

                uint magic = reader.ReadUInt32();
                if (magic != Magic) throw new InvalidDataException("Invalid Polaris spill run magic header.");

                _ = reader.ReadInt32(); // rowCount
                int colCount = reader.ReadInt32();

                var cols = new List<ISeries>(colCount);

                for (int c = 0; c < colCount; c++)
                {
                    string name = reader.ReadString();
                    var tag = (SpillColumnTag)reader.ReadByte();
                    int length = reader.ReadInt32();

                    bool hasNulls = reader.ReadBoolean();
                    ulong[]? words = null;
                    if (hasNulls)
                    {
                        int wordCount = reader.ReadInt32();
                        words = new ulong[wordCount];
                        for (int w = 0; w < wordCount; w++)
                        {
                            words[w] = reader.ReadUInt64();
                        }
                    }

                    ISeries series;
                    switch (tag)
                    {
                        case SpillColumnTag.Utf8String:
                        {
                            int dataByteLength = reader.ReadInt32();
                            var dataBytes = reader.ReadBytes(dataByteLength);
                            int offsetByteLength = reader.ReadInt32();
                            var offsetBytes = reader.ReadBytes(offsetByteLength);

                            var u8 = new Utf8StringSeries(name, length, dataByteLength);
                            dataBytes.CopyTo(u8.DataBytes);
                            offsetBytes.CopyTo(MemoryMarshal.AsBytes(u8.Offsets.Span));
                            series = u8;
                            break;
                        }
                        case SpillColumnTag.Binary:
                        {
                            int dataByteLength = reader.ReadInt32();
                            var dataBytes = reader.ReadBytes(dataByteLength);
                            int offsetByteLength = reader.ReadInt32();
                            var offsetBytes = reader.ReadBytes(offsetByteLength);

                            var bin = new BinarySeries(name, length, dataByteLength);
                            dataBytes.CopyTo(bin.DataBytes);
                            offsetBytes.CopyTo(MemoryMarshal.AsBytes(bin.Offsets.Span));
                            series = bin;
                            break;
                        }
                        case SpillColumnTag.Int32:
                        {
                            var s = new Int32Series(name, length);
                            reader.BaseStream.ReadExactly(MemoryMarshal.AsBytes(s.Memory.Span));
                            series = s;
                            break;
                        }
                        case SpillColumnTag.Int64:
                        {
                            var s = new Int64Series(name, length);
                            reader.BaseStream.ReadExactly(MemoryMarshal.AsBytes(s.Memory.Span));
                            series = s;
                            break;
                        }
                        case SpillColumnTag.Float64:
                        {
                            var s = new Float64Series(name, length);
                            reader.BaseStream.ReadExactly(MemoryMarshal.AsBytes(s.Memory.Span));
                            series = s;
                            break;
                        }
                        case SpillColumnTag.Float32:
                        {
                            var s = new Float32Series(name, length);
                            reader.BaseStream.ReadExactly(MemoryMarshal.AsBytes(s.Memory.Span));
                            series = s;
                            break;
                        }
                        case SpillColumnTag.Boolean:
                        {
                            var s = new BooleanSeries(name, length);
                            reader.BaseStream.ReadExactly(MemoryMarshal.AsBytes(s.Memory.Span));
                            series = s;
                            break;
                        }
                        case SpillColumnTag.Int8:
                        {
                            var s = new Int8Series(name, length);
                            reader.BaseStream.ReadExactly(MemoryMarshal.AsBytes(s.Memory.Span));
                            series = s;
                            break;
                        }
                        case SpillColumnTag.UInt8:
                        {
                            var s = new UInt8Series(name, length);
                            reader.BaseStream.ReadExactly(MemoryMarshal.AsBytes(s.Memory.Span));
                            series = s;
                            break;
                        }
                        case SpillColumnTag.Int16:
                        {
                            var s = new Int16Series(name, length);
                            reader.BaseStream.ReadExactly(MemoryMarshal.AsBytes(s.Memory.Span));
                            series = s;
                            break;
                        }
                        case SpillColumnTag.UInt16:
                        {
                            var s = new UInt16Series(name, length);
                            reader.BaseStream.ReadExactly(MemoryMarshal.AsBytes(s.Memory.Span));
                            series = s;
                            break;
                        }
                        case SpillColumnTag.UInt32:
                        {
                            var s = new UInt32Series(name, length);
                            reader.BaseStream.ReadExactly(MemoryMarshal.AsBytes(s.Memory.Span));
                            series = s;
                            break;
                        }
                        case SpillColumnTag.UInt64:
                        {
                            var s = new UInt64Series(name, length);
                            reader.BaseStream.ReadExactly(MemoryMarshal.AsBytes(s.Memory.Span));
                            series = s;
                            break;
                        }
                        case SpillColumnTag.Date:
                        {
                            var s = new DateSeries(name, length);
                            reader.BaseStream.ReadExactly(MemoryMarshal.AsBytes(s.Memory.Span));
                            series = s;
                            break;
                        }
                        case SpillColumnTag.Datetime:
                        {
                            var s = new DatetimeSeries(name, length);
                            reader.BaseStream.ReadExactly(MemoryMarshal.AsBytes(s.Memory.Span));
                            series = s;
                            break;
                        }
                        case SpillColumnTag.Decimal:
                        {
                            var s = new DecimalSeries(name, length);
                            reader.BaseStream.ReadExactly(MemoryMarshal.AsBytes(s.Memory.Span));
                            series = s;
                            break;
                        }
                        case SpillColumnTag.Duration:
                        {
                            var s = new DurationSeries(name, length);
                            reader.BaseStream.ReadExactly(MemoryMarshal.AsBytes(s.Memory.Span));
                            series = s;
                            break;
                        }
                        case SpillColumnTag.Time:
                        {
                            var s = new TimeSeries(name, length);
                            reader.BaseStream.ReadExactly(MemoryMarshal.AsBytes(s.Memory.Span));
                            series = s;
                            break;
                        }
                        case SpillColumnTag.Null:
                        {
                            series = new NullSeries(name, length);
                            break;
                        }
                        default:
                        {
                            var strings = new string?[length];
                            for (int i = 0; i < length; i++) strings[i] = reader.ReadString();
                            series = Utf8StringSeries.FromStrings(name, strings);
                            break;
                        }
                    }

                    if (hasNulls && words != null)
                    {
                        for (int w = 0; w < Math.Min(words.Length, series.ValidityMask.WordCount); w++)
                        {
                            series.ValidityMask.SetWord(w, words[w]);
                        }
                    }

                    cols.Add(series);
                }

                return new DataFrame(cols);
            }
        }
    }
}
