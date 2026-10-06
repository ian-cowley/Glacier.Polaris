using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Glacier.Polaris.Data;

namespace Glacier.Polaris
{
    public partial class ExecutionEngine
    {
        private ISeries? EvaluateWindowAndAggOp(MethodCallExpression mce, DataFrame df, List<IDisposable> disposables)
        {
                if (mce.Method.Name == "Col")
                {
                    string colName = (string)((ConstantExpression)mce.Arguments[0]).Value!;
                    return df.GetColumn(colName);
                }
                else if (mce.Method.Name == "LitOp")
                {
                    var arg = mce.Arguments[0];
                    if (arg is UnaryExpression ue && ue.NodeType == ExpressionType.Convert) arg = ue.Operand;
                    var val = ((ConstantExpression)arg).Value;
                    return CreateLiteralSeries(val, df.RowCount);
                }
                // ── Binary ops (Bin namespace) ────────────────────────────────
                else if (mce.Method.Name == "Bin_LengthsOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.BinarySeries bin)
                    {
                        var result = Compute.BinaryKernels.Lengths(bin);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("Bin.Lengths requires BinarySeries.");
                }
                else if (mce.Method.Name == "Bin_ContainsOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    byte[] pattern = (byte[])((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.BinarySeries bin)
                    {
                        var result = Compute.BinaryKernels.Contains(bin, pattern);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("Bin.Contains requires BinarySeries.");
                }
                else if (mce.Method.Name == "Bin_StartsWithOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    byte[] prefix = (byte[])((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.BinarySeries bin)
                    {
                        var result = Compute.BinaryKernels.StartsWith(bin, prefix);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("Bin.StartsWith requires BinarySeries.");
                }
                else if (mce.Method.Name == "Bin_EndsWithOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    byte[] suffix = (byte[])((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.BinarySeries bin)
                    {
                        var result = Compute.BinaryKernels.EndsWith(bin, suffix);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("Bin.EndsWith requires BinarySeries.");
                }
                else if (mce.Method.Name == "Bin_EncodeOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string encoding = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    // Encode: text→binary (utf-8) or binary→string (hex/base64)
                    bool isUtf8Encoding = encoding.Equals("utf-8", StringComparison.OrdinalIgnoreCase) || encoding.Equals("utf8", StringComparison.OrdinalIgnoreCase);
                    if (isUtf8Encoding)
                    {
                        // Utf8String → Binary (EncodeUtf8)
                        if (series is Data.Utf8StringSeries utf8)
                        {
                            var result = Compute.BinaryKernels.EncodeUtf8(utf8);
                            disposables.Add(result);
                            return result;
                        }
                        throw new NotSupportedException("Bin.Encode('utf-8') requires Utf8StringSeries.");
                    }
                    else
                    {
                        // Binary → string (hex/base64)
                        if (series is Data.BinarySeries bin)
                        {
                            var result = Compute.BinaryKernels.Encode(bin, encoding);
                            disposables.Add(result);
                            return result;
                        }
                        throw new NotSupportedException("Bin.Encode requires BinarySeries for non-utf8 encodings.");
                    }
                }
                else if (mce.Method.Name == "Bin_DecodeOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string encoding = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    // Decode: binary→text (utf-8 or hex/base64) or string→binary (hex/base64 round-trip)
                    bool isUtf8Encoding = encoding.Equals("utf-8", StringComparison.OrdinalIgnoreCase) || encoding.Equals("utf8", StringComparison.OrdinalIgnoreCase);
                    if (isUtf8Encoding)
                    {
                        // Binary → Utf8String (DecodeUtf8)
                        if (series is Data.BinarySeries bin)
                        {
                            var result = Compute.BinaryKernels.DecodeUtf8(bin);
                            disposables.Add(result);
                            return result;
                        }
                        throw new NotSupportedException("Bin.Decode('utf-8') requires BinarySeries.");
                    }
                    else
                    {
                        // hex/base64: if input is BinarySeries, encode to string; if input is Utf8StringSeries, decode to binary
                        if (series is Data.BinarySeries bin)
                        {
                            // Binary → hex/base64 string (interpret as "encode binary to text representation")
                            var result = Compute.BinaryKernels.Encode(bin, encoding);
                            disposables.Add(result);
                            return result;
                        }
                        if (series is Data.Utf8StringSeries utf8)
                        {
                            // hex/base64 string → binary (round-trip decode)
                            var result = Compute.BinaryKernels.Decode(utf8, encoding);
                            disposables.Add(result);
                            return result;
                        }
                        throw new NotSupportedException("Bin.Decode requires BinarySeries or Utf8StringSeries for non-utf8 encodings.");
                    }
                }




                // ── Temporal ops: Hour, Minute, Second, Nanosecond on TimeSeries ──
                else if (mce.Method.Name == "HourOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.TimeSeries ts)
                    {
                        var result = new Data.Int32Series(series.Name + "_hour", series.Length);
                        for (int i = 0; i < ts.Length; i++)
                        {
                            if (ts.ValidityMask.IsNull(i)) result.ValidityMask.SetNull(i);
                            else result.Memory.Span[i] = ts.GetHour(i);
                        }
                        disposables.Add(result);
                        return result;
                    }
                    // Fallback to TemporalKernels for DatetimeSeries
                    var res = Compute.TemporalKernels.ExtractHour(series);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "MinuteOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.TimeSeries ts)
                    {
                        var result = new Data.Int32Series(series.Name + "_minute", series.Length);
                        for (int i = 0; i < ts.Length; i++)
                        {
                            if (ts.ValidityMask.IsNull(i)) result.ValidityMask.SetNull(i);
                            else result.Memory.Span[i] = ts.GetMinute(i);
                        }
                        disposables.Add(result);
                        return result;
                    }
                    var res2 = Compute.TemporalKernels.ExtractMinute(series);
                    disposables.Add(res2);
                    return res2;
                }
                else if (mce.Method.Name == "SecondOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.TimeSeries ts)
                    {
                        var result = new Data.Int32Series(series.Name + "_second", series.Length);
                        for (int i = 0; i < ts.Length; i++)
                        {
                            if (ts.ValidityMask.IsNull(i)) result.ValidityMask.SetNull(i);
                            else result.Memory.Span[i] = ts.GetSecond(i);
                        }
                        disposables.Add(result);
                        return result;
                    }
                    var res3 = Compute.TemporalKernels.ExtractSecond(series);
                    disposables.Add(res3);
                    return res3;
                }
                else if (mce.Method.Name == "NanosecondOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var res = Compute.TemporalKernels.ExtractNanosecond(series);
                    disposables.Add(res);
                    return res;
                }

                else if (mce.Method.Name == "RollingMeanOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int window = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.WindowKernels.RollingMean(series, window);
                    result.Rename(series.Name + "_rolling_mean");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "RollingSumOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int window = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.WindowKernels.RollingSum(series, window);
                    result.Rename(series.Name + "_rolling_sum");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "RollingStdOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int window = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.WindowKernels.RollingStd(series, window);
                    result.Rename(series.Name + "_rolling_std");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "RollingMinOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int window = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.WindowKernels.RollingMin(series, window);
                    result.Rename(series.Name + "_rolling_min");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "RollingMaxOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int window = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.WindowKernels.RollingMax(series, window);
                    result.Rename(series.Name + "_rolling_max");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "ExpandingSumOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.WindowKernels.ExpandingSum(series);
                    result.Rename(series.Name + "_expanding_sum");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "ExpandingMeanOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.WindowKernels.ExpandingMean(series);
                    result.Rename(series.Name + "_expanding_mean");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "ExpandingMinOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.WindowKernels.ExpandingMin(series);
                    result.Rename(series.Name + "_expanding_min");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "ExpandingMaxOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.WindowKernels.ExpandingMax(series);
                    result.Rename(series.Name + "_expanding_max");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "ExpandingStdOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.WindowKernels.ExpandingStd(series);
                    result.Rename(series.Name + "_expanding_std");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "EWMMeanOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    double alpha = (double)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.WindowKernels.EWMMean(series, alpha);
                    result.Rename(series.Name + "_ewm_mean");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "SumOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = series.Sum();
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "MeanOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = series.Mean();
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "MinOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = series.Min();
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "MaxOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = series.Max();
                    disposables.Add(result);
                    return result;
                }

                else if (mce.Method.Name == "StdOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = series.Std();
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "VarOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = series.Var();
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "FillNullLiteralOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    // Argument may be a UnaryExpression(Convert) wrapping a ConstantExpression for value types
                    var arg1 = mce.Arguments[1];
                    if (arg1 is UnaryExpression ueArg && ueArg.NodeType == ExpressionType.Convert)
                        arg1 = ueArg.Operand;
                    object val = ((ConstantExpression)arg1).Value!;
                    var result = series.FillNull(val);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "FillNullOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    FillStrategy strategy = (FillStrategy)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = series.FillNull(strategy);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "LengthOp")
                {

                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var lengthSeries = new Int32Series("length", 1) { [0] = series.Length };
                    disposables.Add(lengthSeries);
                    return lengthSeries;
                }
                else if (mce.Method.Name == "StartsWithOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string pattern = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = series.StartsWith(pattern);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "EndsWithOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string pattern = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = series.EndsWith(pattern);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "OverOp")
                {
                    var innerExpr = mce.Arguments[0];
                    var groupCols = (string[])((ConstantExpression)mce.Arguments[1]).Value!;

                    if (innerExpr is MethodCallExpression innerMce)
                    {
                        string aggType = innerMce.Method.Name.Replace("Op", "").ToLower(); // sum, mean, count
                        var targetColExpr = innerMce.Arguments[0];
                        var targetSeries = EvaluateExpression(targetColExpr, df, disposables);

                        var gCols = groupCols.Select(c => df.GetColumn(c)).ToArray();
                        var groups = Compute.GroupByKernels.GroupBy(gCols);

                        var aggregated = Compute.GroupByKernels.Aggregate(targetSeries, groups, aggType);
                        var broadcasted = Compute.WindowKernels.BroadcastOver(aggregated, groups, df.RowCount);

                        disposables.Add(broadcasted);
                        return broadcasted;
                    }
                    throw new NotSupportedException("Over requires an aggregation expression.");
                }
            return null;
        }
    }
}
