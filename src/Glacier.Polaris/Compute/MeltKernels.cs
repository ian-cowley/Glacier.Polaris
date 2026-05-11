using System;
using System.Collections.Generic;
using System.Linq;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    public static class MeltKernels
    {
        public static DataFrame Melt(DataFrame df, string[] idVars, string[] valueVars, string varName, string valName)
        {
            int sourceRows = df.RowCount;
            int numValueVars = valueVars.Length;
            int resultRows = sourceRows * numValueVars;

            var resultCols = new List<ISeries>();

            // 1. Process ID Variables (Repeat sourceRows, numValueVars times)
            foreach (var idVar in idVars)
            {
                var col = df.GetColumn(idVar);
                resultCols.Add(RepeatColumn(col, numValueVars));
            }

            // 2. Create Variable Column (Column names of valueVars)
            resultCols.Add(CreateVariableColumn(valueVars, sourceRows, varName));

            // 3. Create Value Column (Stacking valueVars)
            resultCols.Add(StackValueColumns(df, valueVars, sourceRows, valName));

            return new DataFrame(resultCols);
        }

        private static ISeries RepeatColumn(ISeries source, int count)
        {
            int length = source.Length;
            int resultLength = length * count;
            
            int[] indices = new int[resultLength];
            for (int i = 0; i < count; i++)
            {
                for (int j = 0; j < length; j++)
                {
                    indices[i * length + j] = j;
                }
            }
            
            // Use existing Take kernel for efficient materialization
            return ComputeKernels.Take(source, indices);
        }

        private static ISeries CreateVariableColumn(string[] valueVars, int rowsPerVar, string name)
        {
            string[] data = new string[valueVars.Length * rowsPerVar];
            for (int i = 0; i < valueVars.Length; i++)
            {
                for (int j = 0; j < rowsPerVar; j++)
                {
                    data[i * rowsPerVar + j] = valueVars[i];
                }
            }
            return new Utf8StringSeries(name, data);
        }

        private static ISeries StackValueColumns(DataFrame df, string[] valueVars, int rowsPerVar, string name)
        {
            bool hasFloat = valueVars.Any(v => df.GetColumn(v) is Float64Series);
            
            if (hasFloat)
            {
                var result = new Float64Series(name, valueVars.Length * rowsPerVar);
                var span = result.Memory.Span;
                for (int i = 0; i < valueVars.Length; i++)
                {
                    var col = df.GetColumn(valueVars[i]);
                    if (col is Float64Series f64)
                    {
                        f64.Memory.Span.CopyTo(span.Slice(i * rowsPerVar));
                    }
                    else if (col is Int32Series i32)
                    {
                        var i32Span = i32.Memory.Span;
                        var dest = span.Slice(i * rowsPerVar);
                        for (int k = 0; k < rowsPerVar; k++) dest[k] = i32Span[k];
                    }
                }
                return result;
            }
            else
            {
                var result = new Int32Series(name, valueVars.Length * rowsPerVar);
                var span = result.Memory.Span;
                for (int i = 0; i < valueVars.Length; i++)
                {
                    var col = df.GetColumn(valueVars[i]) as Int32Series;
                    if (col != null)
                    {
                        col.Memory.Span.CopyTo(span.Slice(i * rowsPerVar));
                    }
                }
                return result;
            }
        }
    }
}
