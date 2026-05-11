using System;
using System.Collections.Generic;
using System.Linq;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    public static class TransposeKernels
    {
        public static DataFrame Transpose(DataFrame df, bool include_header = false, string header_name = "column", string[]? column_names = null)
        {
            if (df.Columns.Count == 0) return new DataFrame();
            int rowCount = df.RowCount;
            int colCount = df.Columns.Count;

            var newCols = new List<ISeries>();

            // Header column if requested
            if (include_header)
            {
                var headerNames = df.Columns.Select(c => c.Name).ToArray();
                newCols.Add(Utf8StringSeries.FromStrings(header_name, headerNames));
            }

            // New column names
            string[] newColNames;
            if (column_names != null)
            {
                newColNames = column_names;
            }
            else
            {
                newColNames = Enumerable.Range(0, rowCount).Select(i => $"column_{i}").ToArray();
            }

            // Data transposition
            // If any column is Float64, or if we have mixed numeric types, coerce to Float64
            bool anyFloat = df.Columns.Any(c => c is Float64Series);
            bool allNumeric = df.Columns.All(c => c is Int32Series || c is Float64Series || c is Int64Series || c is Int16Series || c is Int8Series);
            bool allInt = df.Columns.All(c => c is Int32Series);
            if (anyFloat)
            {
                for (int r = 0; r < rowCount; r++)
                {
                    var data = new double[colCount];
                    for (int c = 0; c < colCount; c++)
                    {
                        var val = df.Columns[c].Get(r);
                        data[c] = val != null ? Convert.ToDouble(val) : double.NaN;
                    }
                    newCols.Add(new Float64Series(newColNames[r], data));
                }
            }
            else if (allInt)
            {
                for (int r = 0; r < rowCount; r++)
                {
                    var data = new int[colCount];
                    for (int c = 0; c < colCount; c++)
                    {
                        var val = df.Columns[c].Get(r);
                        data[c] = val != null ? Convert.ToInt32(val) : 0;
                    }
                    newCols.Add(new Int32Series(newColNames[r], data));
                }
            }
            else if (allNumeric)
            {
                // Mixed numeric without float (e.g. Int32 and Int64) -> coerce to Float64 for safety or Int64
                // For now, let's stick to Float64 as common denominator
                for (int r = 0; r < rowCount; r++)
                {
                    var data = new double[colCount];
                    for (int c = 0; c < colCount; c++)
                    {
                        var val = df.Columns[c].Get(r);
                        data[c] = val != null ? Convert.ToDouble(val) : double.NaN;
                    }
                    newCols.Add(new Float64Series(newColNames[r], data));
                }
            }
            else
            {
                for (int r = 0; r < rowCount; r++)
                {
                    var data = new string[colCount];
                    for (int c = 0; c < colCount; c++)
                    {
                        data[c] = df.Columns[c].Get(r)?.ToString() ?? "";
                    }
                    newCols.Add(Utf8StringSeries.FromStrings(newColNames[r], data));
                }
            }

            return new DataFrame(newCols);
        }
    }
}
