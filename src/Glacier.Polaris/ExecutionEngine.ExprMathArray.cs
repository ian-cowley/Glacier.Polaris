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
        private ISeries? EvaluateMathAndArrayOp(MethodCallExpression mce, DataFrame df, List<IDisposable> disposables)
        {
                if (mce.Method.Name == "FirstOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.AggregationKernels.First(series);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "LastOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.AggregationKernels.Last(series);
                    disposables.Add(result);
                    return result;
                }
                // --- IsDuplicated / IsUnique ---
                else if (mce.Method.Name == "IsDuplicatedOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.UniqueKernels.IsDuplicated(series);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "IsUniqueOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.UniqueKernels.IsUnique(series);
                    disposables.Add(result);
                    return result;
                }
                // --- EWMStd ---
                else if (mce.Method.Name == "EWMStdOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    double alpha = (double)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.WindowKernels.EWMStd(series, alpha);
                    result.Rename(series.Name + "_ewm_std");
                    disposables.Add(result);
                    return result;
                }
                // --- Array ops: Shift / Diff / Abs / Clip / DropNulls ---
                else if (mce.Method.Name == "ShiftOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int n = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.ArrayKernels.Shift(series, n);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "DiffOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int n = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.ArrayKernels.Diff(series, n);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "AbsOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.ArrayKernels.Abs(series);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "ReinterpretOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    Type targetType = (Type)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.ArrayKernels.Reinterpret(series, targetType);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "ClipOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    double min = (double)((ConstantExpression)mce.Arguments[1]).Value!;
                    double max = (double)((ConstantExpression)mce.Arguments[2]).Value!;
                    var result = Compute.ArrayKernels.Clip(series, min, max);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "DropNullsOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.ArrayKernels.DropNulls(series);
                    disposables.Add(result);
                    return result;
                }

                // --- Math ops: Sqrt / Log / Log10 / Exp / Sin / Cos / Tan ---
                else if (mce.Method.Name == "SqrtOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.MathKernels.Sqrt(series);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "LogOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.MathKernels.Log(series);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "Log10Op")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.MathKernels.Log10(series);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "ExpOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.MathKernels.Exp(series);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "SinOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.MathKernels.Sin(series);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "CosOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.MathKernels.Cos(series);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "TanOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.MathKernels.Tan(series);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "RankOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    bool descending = (bool)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.MathKernels.Rank(series, descending);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "PctChangeOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int n = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.MathKernels.PctChange(series, n);
                    disposables.Add(result);
                    return result;
                }

                // --- String ops ---
                else if (mce.Method.Name == "Str_ReplaceOp")

                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string oldValue = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    string newValue = (string)((ConstantExpression)mce.Arguments[2]).Value!;
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.Replace(utf8, oldValue, newValue); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.Replace requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_ReplaceAllOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string oldValue = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    string newValue = (string)((ConstantExpression)mce.Arguments[2]).Value!;
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.ReplaceAll(utf8, oldValue, newValue); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.ReplaceAll requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_StripOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.Strip(utf8); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.Strip requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_LStripOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.LStrip(utf8); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.LStrip requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_RStripOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.RStrip(utf8); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.RStrip requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_SplitOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string separator = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.Split(utf8, separator); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.Split requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_SliceOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int start = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    int? length = (int?)((ConstantExpression)mce.Arguments[2]).Value;
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.Slice(utf8, start, length); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.Slice requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_ToDateOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string? format = (string?)((ConstantExpression)mce.Arguments[1]).Value;
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.ParseDate(utf8, format); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.ToDate requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_ToDatetimeOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string? format = (string?)((ConstantExpression)mce.Arguments[1]).Value;
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.ParseDatetime(utf8, format); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.ToDatetime requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_HeadOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int n = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.Head(utf8, n); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.Head requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_TailOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int n = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.Tail(utf8, n); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.Tail requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_PadStartOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int width = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    char fillChar = (char)((ConstantExpression)mce.Arguments[2]).Value!;
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.PadStart(utf8, width, fillChar); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.PadStart requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_PadEndOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int width = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    char fillChar = (char)((ConstantExpression)mce.Arguments[2]).Value!;
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.PadEnd(utf8, width, fillChar); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.PadEnd requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_ToTitlecaseOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.ToTitlecase(utf8); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.ToTitlecase requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_ExtractOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string pattern = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.Extract(utf8, pattern); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.Extract requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_ReverseOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.Reverse(utf8); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.Reverse requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_ExtractAllOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string pattern = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.ExtractAll(utf8, pattern); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.ExtractAll requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_JsonEncodeOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.JsonEncode(utf8); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.JsonEncode requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_JsonDecodeOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.JsonDecode(utf8); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.JsonDecode requires Utf8StringSeries.");
                }
                // --- Floor / Ceil / Round ---
                else if (mce.Method.Name == "FloorOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.MathKernels.Floor(series);
                    result.Rename(series.Name + "_floor");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "CeilOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.MathKernels.Ceil(series);
                    result.Rename(series.Name + "_ceil");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "RoundOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int decimals = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.MathKernels.Round(series, decimals);
                    result.Rename(series.Name + "_round");
                    disposables.Add(result);
                    return result;
                }
                // --- Cumulative ops ---
                else if (mce.Method.Name == "CumCountOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    bool reverse = (bool)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.WindowKernels.ExpandingCount(series, reverse);
                    result.Rename(series.Name + "_cum_count");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "CumProdOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    bool reverse = (bool)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.WindowKernels.ExpandingProd(series, reverse);
                    result.Rename(series.Name + "_cum_prod");
                    disposables.Add(result);
                    return result;
                }
                // --- NullCount (aggregation) ---
                else if (mce.Method.Name == "NullCountOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.AggregationKernels.NullCount(series);
                    disposables.Add(result);
                    return result;
                }
                // --- ArgMin (aggregation) ---
                else if (mce.Method.Name == "ArgMinOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.AggregationKernels.ArgMin(series);
                    disposables.Add(result);
                    return result;
                }
                // --- ArgMax (aggregation) ---
                else if (mce.Method.Name == "ArgMaxOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.AggregationKernels.ArgMax(series);
                    disposables.Add(result);
                    return result;
                }
                // --- Select ops: GatherEvery, SearchSorted, Slice, TopK, BottomK ---
                else if (mce.Method.Name == "GatherEveryOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int n = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    int offset = (int)((ConstantExpression)mce.Arguments[2]).Value!;
                    var result = Compute.ArrayKernels.GatherEvery(series, n, offset);
                    result.Rename(series.Name + "_gather_every");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "SearchSortedOp")
                {
                    ISeries source = EvaluateExpression(mce.Arguments[0], df, disposables);
                    ISeries element = EvaluateExpression(mce.Arguments[1], df, disposables);
                    var result = Compute.ArrayKernels.SearchSorted(source, element);
                    result.Rename("search_sorted");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "SliceOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int offset = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    int? length = (int?)((ConstantExpression)mce.Arguments[2]).Value;
                    var result = Compute.ArrayKernels.SliceSeries(series, offset, length);
                    result.Rename(series.Name + "_slice");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "TopKOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int k = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.ArrayKernels.TopKSeries(series, k);
                    result.Rename(series.Name + "_topk");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "BottomKOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int k = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.ArrayKernels.BottomKSeries(series, k);
                    result.Rename(series.Name + "_bottomk");
                    disposables.Add(result);
                    return result;
                }
            return null;
        }
    }
}
