using System;
using System.Collections.Generic;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    public static partial class GroupByKernels
    {
        public static DataFrame Aggregate(DataFrame source, string[] groupKeys, List<List<int>> groups, params (string col, string agg)[] aggregations)
        {
            int groupCount = groups.Count;
            var resultColumns = new List<ISeries>();

            // Add groupby key columns (first value from each group)
            foreach (var keyName in groupKeys)
            {
                var col = source.GetColumn(keyName);
                if (col is Int32Series i32Key)
                {
                    var keyCol = new Int32Series(keyName, groupCount);
                    for (int i = 0; i < groupCount; i++)
                        if (groups[i].Count > 0)
                            keyCol.Memory.Span[i] = i32Key.Memory.Span[groups[i][0]];
                    resultColumns.Add(keyCol);
                }
                else if (col is Utf8StringSeries u8Key)
                {
                    var keyCol = new Utf8StringSeries(keyName, groupCount);
                    for (int i = 0; i < groupCount; i++)
                        if (groups[i].Count > 0)
                            u8Key.Take(keyCol, groups[i][0], i);
                    resultColumns.Add(keyCol);
                }
                else if (col is Float64Series f64Key)
                {
                    var keyCol = new Float64Series(keyName, groupCount);
                    for (int i = 0; i < groupCount; i++)
                        if (groups[i].Count > 0)
                            f64Key.Take(keyCol, groups[i][0], i);
                    resultColumns.Add(keyCol);
                }
                else if (col is Int64Series i64Key)
                {
                    var keyCol = new Int64Series(keyName, groupCount);
                    for (int i = 0; i < groupCount; i++)
                        if (groups[i].Count > 0)
                            i64Key.Take(keyCol, groups[i][0], i);
                    resultColumns.Add(keyCol);
                }
            }

            // Delegate each aggregation to the ISeries Aggregate method for proper type handling
            foreach (var (colName, aggType) in aggregations)
            {
                var sourceCol = source.GetColumn(colName);
                var resultCol = Aggregate(sourceCol, groups, aggType);
                resultColumns.Add(resultCol);
            }

            return new DataFrame(resultColumns);
        }

        public static ISeries Aggregate(ISeries source, List<List<int>> groups, string aggType)
        {
            int groupCount = groups.Count;
            var resultCol = CreateResultColumn(source.Name, aggType, groupCount, source);
            var options = new System.Threading.Tasks.ParallelOptions { MaxDegreeOfParallelism = Math.Max(1, Environment.ProcessorCount - 1) };

            if (aggType == "sum" && source is Int32Series i32)
            {
                var dest = (Int32Series)resultCol;
                System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                {
                    int sum = 0;
                    foreach (int idx in groups[i]) sum += i32.Memory.Span[idx];
                    dest.Memory.Span[i] = sum;
                });
            }
            else if (aggType == "sum" && source is Float64Series f64)
            {
                var dest = (Float64Series)resultCol;
                System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                {
                    double sum = 0;
                    foreach (int idx in groups[i]) sum += f64.Memory.Span[idx];
                    dest.Memory.Span[i] = sum;
                });
            }
            else if (aggType == "count")
            {
                var dest = (Int32Series)resultCol;
                System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                {
                    dest.Memory.Span[i] = groups[i].Count;
                });
            }
            else if (aggType == "min")
            {
                if (resultCol is Int32Series i32Dest && source is Int32Series i32Source)
                {
                    System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                    {
                        int min = int.MaxValue;
                        foreach (int idx in groups[i]) if (i32Source.Memory.Span[idx] < min) min = i32Source.Memory.Span[idx];
                        i32Dest.Memory.Span[i] = min;
                    });
                }
                else
                {
                    var dest = (Float64Series)resultCol;
                    System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                    {
                        double min = double.MaxValue;
                        foreach (int idx in groups[i])
                        {
                            double val = GetValueAsDouble(source, idx);
                            if (val < min) min = val;
                        }
                        dest.Memory.Span[i] = min;
                    });
                }
            }
            else if (aggType == "max")
            {
                if (resultCol is Int32Series i32Dest && source is Int32Series i32Source)
                {
                    System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                    {
                        int max = int.MinValue;
                        foreach (int idx in groups[i]) if (i32Source.Memory.Span[idx] > max) max = i32Source.Memory.Span[idx];
                        i32Dest.Memory.Span[i] = max;
                    });
                }
                else
                {
                    var dest = (Float64Series)resultCol;
                    System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                    {
                        double max = double.MinValue;
                        foreach (int idx in groups[i])
                        {
                            double val = GetValueAsDouble(source, idx);
                            if (val > max) max = val;
                        }
                        dest.Memory.Span[i] = max;
                    });
                }
            }
            else if (aggType == "mean" || aggType == "avg")
            {
                var dest = (Float64Series)resultCol;
                System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                {
                    double sum = 0;
                    int count = 0;
                    foreach (int idx in groups[i])
                    {
                        double val = GetValueAsDouble(source, idx);
                        sum += val;
                        count++;
                    }
                    dest.Memory.Span[i] = count > 0 ? sum / count : 0;
                });
            }
            else if (aggType == "implode")
            {
                var offsets = new int[groupCount + 1];
                int totalVals = 0;
                for (int i = 0; i < groupCount; i++)
                {
                    offsets[i] = totalVals;
                    totalVals += groups[i].Count;
                }
                offsets[groupCount] = totalVals;

                var values = new Float64Series("values", totalVals);
                int pos = 0;
                for (int i = 0; i < groupCount; i++)
                {
                    foreach (int idx in groups[i])
                    {
                        values.Memory.Span[pos++] = GetValueAsDouble(source, idx);
                    }
                }

                var offsetsCol = new Int32Series(source.Name + "_offsets", offsets);
                var listSeries = new ListSeries(source.Name + "_implode", offsetsCol, values);
                return listSeries;
            }
            else if (aggType == "first")
            {
                var dest = (Float64Series)resultCol;
                System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                {
                    dest.Memory.Span[i] = groups[i].Count > 0 ? GetValueAsDouble(source, groups[i][0]) : 0;
                });
            }
            else if (aggType == "std")
            {
                var dest = (Float64Series)resultCol;
                System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                {
                    double sum = 0;
                    double sumSq = 0;
                    int cnt = 0;
                    foreach (int idx in groups[i])
                    {
                        double val = GetValueAsDouble(source, idx);
                        sum += val;
                        sumSq += val * val;
                        cnt++;
                    }
                    if (cnt > 1)
                    {
                        double variance = (sumSq - (sum * sum) / cnt) / (cnt - 1);
                        dest.Memory.Span[i] = Math.Sqrt(Math.Max(0, variance));
                    }
                    else
                        dest.Memory.Span[i] = 0;
                });
            }
            else if (aggType == "var")
            {
                var dest = (Float64Series)resultCol;
                System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                {
                    double sum = 0;
                    double sumSq = 0;
                    int cnt = 0;
                    foreach (int idx in groups[i])
                    {
                        double val = GetValueAsDouble(source, idx);
                        sum += val;
                        sumSq += val * val;
                        cnt++;
                    }
                    if (cnt > 1)
                    {
                        double variance = (sumSq - (sum * sum) / cnt) / (cnt - 1);
                        dest.Memory.Span[i] = Math.Max(0, variance);
                    }
                    else
                        dest.Memory.Span[i] = 0;
                });
            }
            else if (aggType == "median")
            {
                var dest = (Float64Series)resultCol;
                System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                {
                    var vals = new double[groups[i].Count];
                    int j = 0;
                    foreach (int idx in groups[i])
                        vals[j++] = GetValueAsDouble(source, idx);
                    Array.Sort(vals);
                    if (vals.Length % 2 == 0)
                        dest.Memory.Span[i] = (vals[vals.Length / 2 - 1] + vals[vals.Length / 2]) / 2.0;
                    else
                        dest.Memory.Span[i] = vals[vals.Length / 2];
                });
            }
            else if (aggType == "null_count")
            {
                var dest = (Int32Series)resultCol;
                System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                {
                    int nulls = 0;
                    foreach (int idx in groups[i])
                    {
                        if (source.ValidityMask.IsNull(idx))
                            nulls++;
                    }
                    dest.Memory.Span[i] = nulls;
                });
            }
            else if (aggType == "arg_min")
            {
                var dest = (Int32Series)resultCol;
                System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                {
                    double min = double.MaxValue;
                    int argMin = -1;
                    foreach (int idx in groups[i])
                    {
                        if (source.ValidityMask.IsValid(idx))
                        {
                            double val = GetValueAsDouble(source, idx);
                            if (val < min)
                            {
                                min = val;
                                argMin = idx;
                            }
                        }
                    }
                    dest.Memory.Span[i] = argMin >= 0 ? argMin : -1;
                });
            }
            else if (aggType == "arg_max")
            {
                var dest = (Int32Series)resultCol;
                System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                {
                    double max = double.MinValue;
                    int argMax = -1;
                    foreach (int idx in groups[i])
                    {
                        if (source.ValidityMask.IsValid(idx))
                        {
                            double val = GetValueAsDouble(source, idx);
                            if (val > max)
                            {
                                max = val;
                                argMax = idx;
                            }
                        }
                    }
                    dest.Memory.Span[i] = argMax >= 0 ? argMax : -1;
                });
            }
            else if (aggType == "nunique")
            {
                var dest = (Int32Series)resultCol;
                System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                {
                    var seen = new HashSet<double>();
                    foreach (int idx in groups[i])
                    {
                        double val = GetValueAsDouble(source, idx);
                        if (!seen.Contains(val)) seen.Add(val);
                    }
                    dest.Memory.Span[i] = seen.Count;
                });
            }
            else if (aggType == "all")
            {
                var dest = (BooleanSeries)resultCol;
                System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                {
                    bool allVal = true;
                    foreach (int idx in groups[i])
                    {
                        if (source.ValidityMask.IsValid(idx))
                        {
                            if (source is BooleanSeries bs)
                            {
                                if (!bs.Memory.Span[idx]) { allVal = false; break; }
                            }
                            else if (source is Int32Series i32s)
                            {
                                if (i32s.Memory.Span[idx] == 0) { allVal = false; break; }
                            }
                            else if (source is Float64Series f64s)
                            {
                                if (f64s.Memory.Span[idx] == 0.0 || double.IsNaN(f64s.Memory.Span[idx])) { allVal = false; break; }
                            }
                            else
                            {
                                var val = source.Get(idx);
                                if (val is bool b && !b) { allVal = false; break; }
                            }
                        }
                    }
                    dest.Memory.Span[i] = allVal;
                });
            }
            else if (aggType == "any")
            {
                var dest = (BooleanSeries)resultCol;
                System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                {
                    bool anyVal = false;
                    foreach (int idx in groups[i])
                    {
                        if (source.ValidityMask.IsValid(idx))
                        {
                            if (source is BooleanSeries bs)
                            {
                                if (bs.Memory.Span[idx]) { anyVal = true; break; }
                            }
                            else if (source is Int32Series i32s)
                            {
                                if (i32s.Memory.Span[idx] != 0) { anyVal = true; break; }
                            }
                            else if (source is Float64Series f64s)
                            {
                                if (f64s.Memory.Span[idx] != 0.0 && !double.IsNaN(f64s.Memory.Span[idx])) { anyVal = true; break; }
                            }
                            else
                            {
                                var val = source.Get(idx);
                                if (val is bool b && b) { anyVal = true; break; }
                            }
                        }
                    }
                    dest.Memory.Span[i] = anyVal;
                });
            }

            return resultCol;
        }

        internal static double GetValueAsDouble(ISeries s, int idx)
        {
            if (s is Int32Series i32) return i32.Memory.Span[idx];
            if (s is Float64Series f64) return f64.Memory.Span[idx];
            if (s is Int64Series i64) return i64.Memory.Span[idx];
            var val = s.Get(idx);
            return val != null ? Convert.ToDouble(val) : 0;
        }

        internal static ISeries CreateResultColumn(string name, string agg, int length, ISeries? source = null)
        {
            if (agg == "all" || agg == "any")
                return new BooleanSeries($"{name}_{agg}", length);
            if (agg == "sum")
            {
                if (source != null && (source is Float64Series || source is Int64Series))
                    return new Float64Series($"{name}_{agg}", length);
                return new Int32Series($"{name}_{agg}", length);
            }
            if (agg == "count" || agg == "nunique" || agg == "null_count" || agg == "arg_min" || agg == "arg_max")
                return new Int32Series($"{name}_{agg}", length);
            if (agg == "implode") return new ListSeries($"{name}_{agg}", new Int32Series($"{name}_offsets", length + 1), new Float64Series($"{name}_values", 0));
            if (source != null && (agg == "min" || agg == "max" || agg == "first"))
            {
                if (source is Int32Series) return new Int32Series($"{name}_{agg}", length);
                if (source is Int64Series) return new Int64Series($"{name}_{agg}", length);
            }
            return new Float64Series($"{name}_{agg}", length);
        }
    }
}
