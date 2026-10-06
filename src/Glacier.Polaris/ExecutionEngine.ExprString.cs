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
        private ISeries? EvaluateStringAndConditionOp(MethodCallExpression mce, DataFrame df, List<IDisposable> disposables)
        {
                if (mce.Method.Name == "Str_ContainsOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string pattern = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.Utf8StringSeries utf8)
                    {
                        var intResult = new Data.Int32Series(utf8.Name + "_contains", utf8.Length);
                        Compute.StringKernels.Contains(utf8.DataBytes.Span, utf8.Offsets.Span, pattern, intResult.Memory.Span);
                        intResult.ValidityMask.CopyFrom(utf8.ValidityMask);
                        // Convert Int32Series (0/1) to BooleanSeries
                        var result = new Data.BooleanSeries(utf8.Name + "_contains", utf8.Length);
                        for (int i = 0; i < utf8.Length; i++)
                        {
                            result.Memory.Span[i] = intResult.Memory.Span[i] != 0;
                            if (intResult.ValidityMask.IsNull(i)) result.ValidityMask.SetNull(i);
                        }
                        disposables.Add(intResult);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("Str.Contains requires Utf8StringSeries.");
                }


                else if (mce.Method.Name == "Str_LengthsOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.Utf8StringSeries utf8)
                    {
                        var result = new Data.Int32Series(utf8.Name + "_len", utf8.Length);
                        Compute.StringKernels.Lengths(utf8.Offsets.Span, result.Memory.Span);
                        result.ValidityMask.CopyFrom(utf8.ValidityMask);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("Str.Lengths requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_StartsWithOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string prefix = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.Utf8StringSeries utf8)
                    {
                        var intResult = new Data.Int32Series(utf8.Name + "_startswith", utf8.Length);
                        intResult.Memory.Span.Clear(); // NativeMemory.Alloc doesn't zero; ensure non-matches are 0
                        Compute.StringKernels.StartsWith(utf8.DataBytes.Span, utf8.Offsets.Span, prefix, intResult.Memory.Span);
                        intResult.ValidityMask.CopyFrom(utf8.ValidityMask);
                        // Convert Int32Series (0/1) to BooleanSeries
                        var result = new Data.BooleanSeries(utf8.Name + "_startswith", utf8.Length);
                        for (int i = 0; i < utf8.Length; i++)
                        {
                            result.Memory.Span[i] = intResult.Memory.Span[i] != 0;
                            if (intResult.ValidityMask.IsNull(i)) result.ValidityMask.SetNull(i);
                        }
                        disposables.Add(intResult);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("Str.StartsWith requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_EndsWithOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string suffix = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.Utf8StringSeries utf8)
                    {
                        var intResult = new Data.Int32Series(utf8.Name + "_endswith", utf8.Length);
                        intResult.Memory.Span.Clear(); // NativeMemory.Alloc doesn't zero; ensure non-matches are 0
                        Compute.StringKernels.EndsWith(utf8.DataBytes.Span, utf8.Offsets.Span, suffix, intResult.Memory.Span);
                        intResult.ValidityMask.CopyFrom(utf8.ValidityMask);
                        // Convert Int32Series (0/1) to BooleanSeries
                        var result = new Data.BooleanSeries(utf8.Name + "_endswith", utf8.Length);
                        for (int i = 0; i < utf8.Length; i++)
                        {
                            result.Memory.Span[i] = intResult.Memory.Span[i] != 0;
                            if (intResult.ValidityMask.IsNull(i)) result.ValidityMask.SetNull(i);
                        }
                        disposables.Add(intResult);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("Str.EndsWith requires Utf8StringSeries.");
                }

                else if (mce.Method.Name == "Str_ToUpperOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.Utf8StringSeries utf8)
                    {
                        var result = Compute.StringKernels.ToUppercase(utf8);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("Str.ToUppercase requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_ToLowerOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.Utf8StringSeries utf8)
                    {
                        var result = Compute.StringKernels.ToLowercase(utf8);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("Str.ToLowercase requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "RegexMatchOp")
                {
                    string colName = (string)((ConstantExpression)mce.Arguments[0]).Value!;
                    string pattern = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    var series = df.GetColumn(colName);
                    if (series is Data.Utf8StringSeries utf8)
                    {
                        var intResult = new Data.Int32Series(utf8.Name + "_match", utf8.Length);
                        intResult.Memory.Span.Clear();
                        Compute.StringKernels.RegexMatch(utf8.DataBytes.Span, utf8.Offsets.Span, pattern, intResult.Memory.Span);
                        intResult.ValidityMask.CopyFrom(utf8.ValidityMask);
                        // Convert Int32Series (0/1) to BooleanSeries
                        var result = new Data.BooleanSeries(utf8.Name + "_match", utf8.Length);
                        for (int i = 0; i < utf8.Length; i++)
                        {
                            result.Memory.Span[i] = intResult.Memory.Span[i] != 0;
                            if (intResult.ValidityMask.IsNull(i)) result.ValidityMask.SetNull(i);
                        }
                        disposables.Add(intResult);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("RegexMatch requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "WhenThenOtherwiseOp")
                {
                    var conditionExpr = mce.Arguments[0];
                    var thenExpr = mce.Arguments[1];
                    var otherwiseExpr = mce.Arguments[2];

                    var conditionSeries = EvaluateExpression(conditionExpr, df, disposables);
                    var thenSeries = EvaluateExpression(thenExpr, df, disposables);
                    var otherwiseSeries = EvaluateExpression(otherwiseExpr, df, disposables);

                    var result = Compute.ConditionalKernels.Select(conditionSeries, thenSeries, otherwiseSeries);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "IsNullOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = new Data.BooleanSeries(series.Name + "_isnull", series.Length);
                    var resSpan = result.Memory.Span;
                    for (int i = 0; i < series.Length; i++) resSpan[i] = series.ValidityMask.IsNull(i);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "IsNotNullOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = new Data.BooleanSeries(series.Name + "_isnotnull", series.Length);
                    var resSpan = result.Memory.Span;
                    for (int i = 0; i < series.Length; i++) resSpan[i] = series.ValidityMask.IsValid(i);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "AllOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.AggregationKernels.All(series);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "AnyOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.AggregationKernels.Any(series);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "IsInOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var values = (object[])((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.PredicateKernels.IsIn(series, values);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "IsInExprOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var targetSeries = EvaluateExpression(mce.Arguments[1], df, disposables);
                    var result = Compute.PredicateKernels.IsIn(series, targetSeries);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "IsBetweenOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var lower = ((ConstantExpression)mce.Arguments[1]).Value!;
                    var upper = ((ConstantExpression)mce.Arguments[2]).Value!;
                    string closed = (string)((ConstantExpression)mce.Arguments[3]).Value!;
                    var result = Compute.PredicateKernels.IsBetween(series, lower, upper, closed);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "IsBetweenExprOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var lowerSeries = EvaluateExpression(mce.Arguments[1], df, disposables);
                    var upperSeries = EvaluateExpression(mce.Arguments[2], df, disposables);
                    string closed = (string)((ConstantExpression)mce.Arguments[3]).Value!;
                    var result = Compute.PredicateKernels.IsBetween(series, lowerSeries, upperSeries, closed);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "IsNanOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.PredicateKernels.IsNan(series);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "IsNotNanOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.PredicateKernels.IsNotNan(series);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "IsFiniteOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.PredicateKernels.IsFinite(series);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "IsInfiniteOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.PredicateKernels.IsInfinite(series);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "SignOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.MathKernels.Sign(series);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "PowOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    double exponent = Convert.ToDouble(((ConstantExpression)mce.Arguments[1]).Value!);
                    var result = Compute.MathKernels.Pow(series, exponent);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "PowExprOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var expSeries = EvaluateExpression(mce.Arguments[1], df, disposables);
                    var result = Compute.MathKernels.Pow(series, expSeries);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "Log1pOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.MathKernels.Log1p(series);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "CbrtOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.MathKernels.Cbrt(series);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "DotOp")
                {
                    var left = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var right = EvaluateExpression(mce.Arguments[1], df, disposables);
                    var result = Compute.MathKernels.Dot(left, right);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "CoalesceOp")
                {
                    var newArray = (NewArrayExpression)mce.Arguments[0];
                    var evaluatedSeries = new List<ISeries>(newArray.Expressions.Count);
                    foreach (var expr in newArray.Expressions)
                    {
                        var cExpr = (ConstantExpression)expr;
                        var polarisExpr = (Expr)cExpr.Value!;
                        var s = EvaluateExpression(polarisExpr.Expression, df, disposables);
                        evaluatedSeries.Add(s);
                    }
                    var result = Compute.MathKernels.Coalesce(evaluatedSeries.ToArray());
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "CastOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var targetType = (Type)((ConstantExpression)mce.Arguments[1]).Value!;

                    if ((targetType == typeof(double) || targetType == typeof(Data.Float64Series)) && series is Data.Utf8StringSeries utf8Double)
                    {
                        var result = new Data.Float64Series(utf8Double.Name + "_cast", utf8Double.Length);
                        for (int i = 0; i < utf8Double.Length; i++)
                        {
                            if (utf8Double.ValidityMask.IsValid(i))
                            {
                                var str = System.Text.Encoding.UTF8.GetString(utf8Double.GetStringSpan(i));
                                if (double.TryParse(str, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out double parsedVal))
                                {
                                    result.Memory.Span[i] = parsedVal;
                                }
                                else
                                {
                                    result.ValidityMask.SetNull(i);
                                }
                            }
                            else
                            {
                                result.ValidityMask.SetNull(i);
                            }
                        }
                        disposables.Add(result);
                        return result;
                    }
                    else if ((targetType == typeof(long) || targetType == typeof(Data.Int64Series)) && series is Data.Utf8StringSeries utf8Long)
                    {
                        var result = new Data.Int64Series(utf8Long.Name + "_cast", utf8Long.Length);
                        for (int i = 0; i < utf8Long.Length; i++)
                        {
                            if (utf8Long.ValidityMask.IsValid(i))
                            {
                                var str = System.Text.Encoding.UTF8.GetString(utf8Long.GetStringSpan(i));
                                if (long.TryParse(str, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out long parsedVal))
                                {
                                    result.Memory.Span[i] = parsedVal;
                                }
                                else
                                {
                                    result.ValidityMask.SetNull(i);
                                }
                            }
                            else
                            {
                                result.ValidityMask.SetNull(i);
                            }
                        }
                        disposables.Add(result);
                        return result;
                    }
                    else if (targetType == typeof(CategoricalSeries))
                    {
                        if (series is Data.Utf8StringSeries utf8)
                        {
                            var strings = new string[utf8.Length];
                            for (int i = 0; i < utf8.Length; i++)
                            {
                                if (utf8.ValidityMask.IsValid(i))
                                    strings[i] = System.Text.Encoding.UTF8.GetString(utf8.GetStringSpan(i));
                                else
                                    strings[i] = null!;
                            }
                            var result = CategoricalSeries.FromStrings(utf8.Name, strings);
                            disposables.Add(result);
                            return result;
                        }
                    }
                    throw new NotSupportedException($"Cast to {targetType.Name} not yet implemented.");
                }
            return null;
        }
    }
}
