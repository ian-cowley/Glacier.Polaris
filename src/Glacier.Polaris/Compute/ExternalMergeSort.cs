using System;
using System.Buffers;
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
        /// <summary>
        /// Default in-memory threshold before spilling runs to disk (256 MB).
        /// </summary>
        public static long DefaultSpillBudget { get; set; } = 256L * 1024 * 1024;

        /// <summary>
        /// Asynchronously sorts an IAsyncEnumerable of DataFrames using in-memory or external merge sort.
        /// </summary>
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

                // If no runs were spilled, execute purely in-memory
                if (spillFiles.Count == 0)
                {
                    if (currentChunks.Count == 0) yield break;

                    var fullDf = currentChunks.Count == 1 ? currentChunks[0] : DataFrame.Concat(currentChunks);
                    var sorted = SortSingleRun(fullDf, columnNames, descending);
                    yield return sorted;
                    yield break;
                }

                // If we did spill, spill the last remaining chunk if non-empty
                if (currentChunks.Count > 0)
                {
                    var runDf = currentChunks.Count == 1 ? currentChunks[0] : DataFrame.Concat(currentChunks);
                    var sortedRun = SortSingleRun(runDf, columnNames, descending);
                    string spillPath = Path.Combine(Path.GetTempPath(), $"polaris_sort_run_{Guid.NewGuid():N}.tmp");
                    SpillRunWriter.WriteRun(sortedRun, spillPath);
                    spillFiles.Add(spillPath);
                    currentChunks.Clear();
                }

                // K-Way Merge of all spill files
                await foreach (var outDf in MergeSpillFilesAsync(spillFiles, columnNames, descending, outputBatchSize).ConfigureAwait(false))
                {
                    yield return outDf;
                }
            }
            finally
            {
                // Clean up any remaining spill files
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
                ISeries newCol;
                if (col is Utf8StringSeries u8)
                {
                    int totalBytes = 0;
                    for (int i = 0; i < indices.Length; i++)
                    {
                        totalBytes += u8.GetStringSpan(indices[i]).Length;
                    }
                    newCol = new Utf8StringSeries(u8.Name, indices.Length, totalBytes);
                }
                else
                {
                    newCol = (ISeries)Activator.CreateInstance(col.GetType(), col.Name, indices.Length)!;
                }

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

                // Template schema from first reader
                var templateCols = readers[0].Columns;
                var colTypes = templateCols.Select(c => c.GetType()).ToArray();
                var colNames = templateCols.Select(c => c.Name).ToArray();

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
                        yield return MaterializeBatch(readers, selectedRows, colTypes, colNames);
                        selectedRows.Clear();
                    }
                }

                if (selectedRows.Count > 0)
                {
                    yield return MaterializeBatch(readers, selectedRows, colTypes, colNames);
                }
            }
            finally
            {
                foreach (var r in readers) r.Dispose();
            }
        }

        private static DataFrame MaterializeBatch(
            List<SpillRunReader> readers,
            List<RunCursor> selectedRows,
            Type[] colTypes,
            string[] colNames)
        {
            int totalRows = selectedRows.Count;
            var resultCols = new List<ISeries>(colNames.Length);

            for (int c = 0; c < colNames.Length; c++)
            {
                var name = colNames[c];
                var type = colTypes[c];

                if (type == typeof(Utf8StringSeries))
                {
                    int totalBytes = 0;
                    for (int i = 0; i < totalRows; i++)
                    {
                        var cursor = selectedRows[i];
                        var srcCol = (Utf8StringSeries)readers[cursor.RunIndex].Columns[c];
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
                    var newCol = (ISeries)Activator.CreateInstance(type, name, totalRows)!;
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
                    if (nullX) return 1;  // nulls last
                    if (nullY) return -1; // non-null first

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
            public int CurrentRow { get; private set; }
            public bool IsExhausted => CurrentRow >= RowCount;

            public SpillRunReader(string filePath)
            {
                var df = SpillRunWriter.ReadRun(filePath);
                Columns = df.Columns.ToList();
                RowCount = df.RowCount;
                CurrentRow = 0;
            }

            public void Advance()
            {
                CurrentRow++;
            }

            public void Dispose()
            {
                foreach (var c in Columns)
                {
                    if (c is IDisposable d) d.Dispose();
                }
            }
        }

        private static class SpillRunWriter
        {
            private const uint Magic = 0x53504C52; // 'RPLS'

            public static void WriteRun(DataFrame df, string filePath)
            {
                using var fs = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None, 65536);
                using var writer = new BinaryWriter(fs);

                writer.Write(Magic);
                writer.Write(df.RowCount);
                writer.Write(df.Columns.Count);

                foreach (var col in df.Columns)
                {
                    writer.Write(col.Name);
                    writer.Write(col.GetType().AssemblyQualifiedName!);
                    writer.Write(col.Length);

                    // ValidityMask
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

                    // Data payload
                    if (col is Utf8StringSeries u8)
                    {
                        writer.Write(u8.DataBytes.Length);
                        writer.Write(u8.DataBytes.Span);
                        var offsets = MemoryMarshal.AsBytes(u8.Offsets.Span);
                        writer.Write(offsets.Length);
                        writer.Write(offsets);
                    }
                    else if (col is Int32Series i32)
                    {
                        writer.Write(MemoryMarshal.AsBytes(i32.Memory.Span));
                    }
                    else if (col is Int64Series i64)
                    {
                        writer.Write(MemoryMarshal.AsBytes(i64.Memory.Span));
                    }
                    else if (col is Float64Series f64)
                    {
                        writer.Write(MemoryMarshal.AsBytes(f64.Memory.Span));
                    }
                    else if (col is Float32Series f32)
                    {
                        writer.Write(MemoryMarshal.AsBytes(f32.Memory.Span));
                    }
                    else if (col is BooleanSeries b)
                    {
                        writer.Write(MemoryMarshal.AsBytes(b.Memory.Span));
                    }
                    else if (col is Int8Series i8)
                    {
                        writer.Write(MemoryMarshal.AsBytes(i8.Memory.Span));
                    }
                    else if (col is UInt8Series u8col)
                    {
                        writer.Write(MemoryMarshal.AsBytes(u8col.Memory.Span));
                    }
                    else if (col is Int16Series i16)
                    {
                        writer.Write(MemoryMarshal.AsBytes(i16.Memory.Span));
                    }
                    else if (col is UInt16Series u16)
                    {
                        writer.Write(MemoryMarshal.AsBytes(u16.Memory.Span));
                    }
                    else if (col is UInt32Series u32)
                    {
                        writer.Write(MemoryMarshal.AsBytes(u32.Memory.Span));
                    }
                    else if (col is UInt64Series u64)
                    {
                        writer.Write(MemoryMarshal.AsBytes(u64.Memory.Span));
                    }
                    else if (col is DateSeries dt)
                    {
                        writer.Write(MemoryMarshal.AsBytes(dt.Memory.Span));
                    }
                    else if (col is DatetimeSeries dtm)
                    {
                        writer.Write(MemoryMarshal.AsBytes(dtm.Memory.Span));
                    }
                    else if (col is DecimalSeries dec)
                    {
                        writer.Write(MemoryMarshal.AsBytes(dec.Memory.Span));
                    }
                    else if (col is DurationSeries dur)
                    {
                        writer.Write(MemoryMarshal.AsBytes(dur.Memory.Span));
                    }
                    else
                    {
                        // Fallback object reflection or string
                        for (int i = 0; i < col.Length; i++)
                        {
                            var v = col.Get(i);
                            writer.Write(v?.ToString() ?? string.Empty);
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

                int rowCount = reader.ReadInt32();
                int colCount = reader.ReadInt32();

                var cols = new List<ISeries>(colCount);

                for (int c = 0; c < colCount; c++)
                {
                    string name = reader.ReadString();
                    string typeName = reader.ReadString();
                    int length = reader.ReadInt32();
                    Type seriesType = Type.GetType(typeName)!;

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

                    if (seriesType == typeof(Utf8StringSeries))
                    {
                        int dataByteLength = reader.ReadInt32();
                        var dataBytes = reader.ReadBytes(dataByteLength);
                        int offsetByteLength = reader.ReadInt32();
                        var offsetBytes = reader.ReadBytes(offsetByteLength);

                        var u8 = new Utf8StringSeries(name, length, dataByteLength);
                        dataBytes.CopyTo(u8.DataBytes);
                        var targetOffsets = MemoryMarshal.AsBytes(u8.Offsets.Span);
                        offsetBytes.CopyTo(targetOffsets);
                        series = u8;
                    }
                    else if (seriesType == typeof(Int32Series))
                    {
                        var s = new Int32Series(name, length);
                        var target = MemoryMarshal.AsBytes(s.Memory.Span);
                        reader.BaseStream.ReadExactly(target);
                        series = s;
                    }
                    else if (seriesType == typeof(Int64Series))
                    {
                        var s = new Int64Series(name, length);
                        var target = MemoryMarshal.AsBytes(s.Memory.Span);
                        reader.BaseStream.ReadExactly(target);
                        series = s;
                    }
                    else if (seriesType == typeof(Float64Series))
                    {
                        var s = new Float64Series(name, length);
                        var target = MemoryMarshal.AsBytes(s.Memory.Span);
                        reader.BaseStream.ReadExactly(target);
                        series = s;
                    }
                    else if (seriesType == typeof(Float32Series))
                    {
                        var s = new Float32Series(name, length);
                        var target = MemoryMarshal.AsBytes(s.Memory.Span);
                        reader.BaseStream.ReadExactly(target);
                        series = s;
                    }
                    else if (seriesType == typeof(BooleanSeries))
                    {
                        var s = new BooleanSeries(name, length);
                        var target = MemoryMarshal.AsBytes(s.Memory.Span);
                        reader.BaseStream.ReadExactly(target);
                        series = s;
                    }
                    else if (seriesType == typeof(Int8Series))
                    {
                        var s = new Int8Series(name, length);
                        var target = MemoryMarshal.AsBytes(s.Memory.Span);
                        reader.BaseStream.ReadExactly(target);
                        series = s;
                    }
                    else if (seriesType == typeof(UInt8Series))
                    {
                        var s = new UInt8Series(name, length);
                        var target = MemoryMarshal.AsBytes(s.Memory.Span);
                        reader.BaseStream.ReadExactly(target);
                        series = s;
                    }
                    else if (seriesType == typeof(Int16Series))
                    {
                        var s = new Int16Series(name, length);
                        var target = MemoryMarshal.AsBytes(s.Memory.Span);
                        reader.BaseStream.ReadExactly(target);
                        series = s;
                    }
                    else if (seriesType == typeof(UInt16Series))
                    {
                        var s = new UInt16Series(name, length);
                        var target = MemoryMarshal.AsBytes(s.Memory.Span);
                        reader.BaseStream.ReadExactly(target);
                        series = s;
                    }
                    else if (seriesType == typeof(UInt32Series))
                    {
                        var s = new UInt32Series(name, length);
                        var target = MemoryMarshal.AsBytes(s.Memory.Span);
                        reader.BaseStream.ReadExactly(target);
                        series = s;
                    }
                    else if (seriesType == typeof(UInt64Series))
                    {
                        var s = new UInt64Series(name, length);
                        var target = MemoryMarshal.AsBytes(s.Memory.Span);
                        reader.BaseStream.ReadExactly(target);
                        series = s;
                    }
                    else if (seriesType == typeof(DateSeries))
                    {
                        var s = new DateSeries(name, length);
                        var target = MemoryMarshal.AsBytes(s.Memory.Span);
                        reader.BaseStream.ReadExactly(target);
                        series = s;
                    }
                    else if (seriesType == typeof(DatetimeSeries))
                    {
                        var s = new DatetimeSeries(name, length);
                        var target = MemoryMarshal.AsBytes(s.Memory.Span);
                        reader.BaseStream.ReadExactly(target);
                        series = s;
                    }
                    else if (seriesType == typeof(DecimalSeries))
                    {
                        var s = new DecimalSeries(name, length);
                        var target = MemoryMarshal.AsBytes(s.Memory.Span);
                        reader.BaseStream.ReadExactly(target);
                        series = s;
                    }
                    else if (seriesType == typeof(DurationSeries))
                    {
                        var s = new DurationSeries(name, length);
                        var target = MemoryMarshal.AsBytes(s.Memory.Span);
                        reader.BaseStream.ReadExactly(target);
                        series = s;
                    }
                    else
                    {
                        series = (ISeries)Activator.CreateInstance(seriesType, name, length)!;
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
