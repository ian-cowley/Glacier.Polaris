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
        private ISeries? EvaluateTemporalOp(MethodCallExpression mce, DataFrame df, List<IDisposable> disposables)
        {
                if (mce.Method.Name == "Dt_YearOp" || mce.Method.Name == "YearOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var res = Compute.TemporalKernels.ExtractYear(series);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_MonthOp" || mce.Method.Name == "MonthOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var res = Compute.TemporalKernels.ExtractMonth(series);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_DayOp" || mce.Method.Name == "DayOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var res = Compute.TemporalKernels.ExtractDay(series);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_HourOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var res = Compute.TemporalKernels.ExtractHour(series);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_MinuteOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var res = Compute.TemporalKernels.ExtractMinute(series);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_SecondOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var res = Compute.TemporalKernels.ExtractSecond(series);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "MedianOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = series.Median();
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "CountOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = series.Count();
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "NUniqueOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = series.NUnique();
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "UniqueOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = series.Unique();
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "QuantileOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    double quantile = (double)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = series.Quantile(quantile);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "DurationOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    // If already a DurationSeries, just return it (no-op conversion)
                    if (series is Data.DurationSeries ds) return ds;
                    throw new NotSupportedException("DurationOp requires a DurationSeries input.");
                }
                else if (mce.Method.Name == "Dt_AddDurationOp" || mce.Method.Name == "Temporal_AddDurationOp")
                {

                    var temporal = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var duration = EvaluateExpression(mce.Arguments[1], df, disposables);
                    var res = Compute.TemporalKernels.AddDuration(temporal, duration);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_SubtractDurationOp" || mce.Method.Name == "Temporal_SubtractDurationOp")
                {
                    var temporal = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var duration = EvaluateExpression(mce.Arguments[1], df, disposables);
                    var res = Compute.TemporalKernels.SubtractDuration(temporal, duration);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_SubtractOp" || mce.Method.Name == "Temporal_SubtractOp")
                {
                    var left = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var right = EvaluateExpression(mce.Arguments[1], df, disposables);
                    var res = Compute.TemporalKernels.Subtract(left, right);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "TotalDaysOp" || mce.Method.Name == "Duration_TotalDaysOp")

                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var res = Compute.TemporalKernels.ExtractTotalDays(series);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "TotalHoursOp" || mce.Method.Name == "Duration_TotalHoursOp")

                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var res = Compute.TemporalKernels.ExtractTotalHours(series);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "TotalSecondsOp" || mce.Method.Name == "Duration_TotalSecondsOp")

                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var res = Compute.TemporalKernels.ExtractTotalSeconds(series);
                    disposables.Add(res);
                    return res;
                }

                // --- Weekday / Quarter ---
                else if (mce.Method.Name == "WeekdayOp" || mce.Method.Name == "Dt_WeekdayOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var res = Compute.TemporalKernels.ExtractWeekday(series);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "QuarterOp" || mce.Method.Name == "Dt_QuarterOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var res = Compute.TemporalKernels.ExtractQuarter(series);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_OffsetByOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string duration = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    var res = Compute.TemporalKernels.OffsetBy(series, duration);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_RoundOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string every = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    var res = Compute.TemporalKernels.Round(series, every);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_EpochOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string unit = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    var res = Compute.TemporalKernels.ExtractEpoch(series, unit);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_TruncateOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string every = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    var res = Compute.TemporalKernels.Truncate(series, every);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_ConvertTimeZoneOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string targetTimeZoneId = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    string sourceTimeZoneId = (string)((ConstantExpression)mce.Arguments[2]).Value!;
                    var res = Compute.TemporalKernels.ConvertTimeZone(series, targetTimeZoneId, sourceTimeZoneId);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_OrdinalDayOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var res = Compute.TemporalKernels.ExtractOrdinalDay(series);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_TimestampOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string unit = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    var res = Compute.TemporalKernels.ExtractTimestamp(series, unit);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_WithTimeUnitOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string unit = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    var res = Compute.TemporalKernels.WithTimeUnit(series, unit);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_CastTimeUnitOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string unit = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    var res = Compute.TemporalKernels.CastTimeUnit(series, unit);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_MonthStartOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var res = Compute.TemporalKernels.MonthStart(series);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_MonthEndOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var res = Compute.TemporalKernels.MonthEnd(series);
                    disposables.Add(res);
                    return res;
                }
                // --- First / Last ---
            return null;
        }
    }
}
