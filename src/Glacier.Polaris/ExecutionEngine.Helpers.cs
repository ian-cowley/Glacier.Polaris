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
        private void CombineMasks(ISeries left, ISeries right, ISeries result)
        {
            // Handle broadcasting: if one is length 1, just use the other's mask
            if (left.Length == 1 && right.Length > 1)
            {
                result.ValidityMask.CopyFrom(right.ValidityMask);
            }
            else if (right.Length == 1 && left.Length > 1)
            {
                result.ValidityMask.CopyFrom(left.ValidityMask);
            }
            else
            {
                result.ValidityMask.CopyFrom(left.ValidityMask);
                result.ValidityMask.And(right.ValidityMask);
            }
        }


        private ISeries CreateLiteralSeries(object? val, int rowCount)
        {
            if (val is int i) return new Data.Int32Series("lit", Enumerable.Repeat(i, rowCount).ToArray());
            if (val is double d) return new Data.Float64Series("lit", Enumerable.Repeat(d, rowCount).ToArray());
            if (val is string s) return new Data.Utf8StringSeries("lit", Enumerable.Repeat(s, rowCount).ToArray());
            if (val is bool b) return new Data.BooleanSeries("lit", Enumerable.Repeat(b, rowCount).ToArray());
            if (val is long l) return new Data.Int64Series("lit", Enumerable.Repeat(l, rowCount).ToArray());
            if (val is decimal m) return new Data.DecimalSeries("lit", Enumerable.Repeat<decimal?>(m, rowCount).ToArray());
            throw new NotSupportedException($"Literal type {val?.GetType().Name} not supported.");
        }

        private ISeries DispatchCompare(ISeries left, ISeries right, Compute.FilterOperation op)
        {
            // Simple double-dispatch or switch on type
            if (left is Data.Int32Series l32 && right is Data.Int32Series r32) return Compute.ComparisonKernels.Compare(l32, r32, op);
            if (left is Data.Float64Series lF64 && right is Data.Float64Series rF64) return Compute.ComparisonKernels.Compare(lF64, rF64, op);
            if (left is Data.Int64Series l64 && right is Data.Int64Series r64) return Compute.ComparisonKernels.Compare(l64, r64, op);
            if (left is Data.BooleanSeries lBool && right is Data.BooleanSeries rBool) return Compute.ComparisonKernels.Compare(lBool, rBool, op);
            if (left is Data.Utf8StringSeries lStr && right is Data.Utf8StringSeries rStr) return CompareStringSeries(lStr, rStr, op);
            // Promote minor integer types to Int32 / Int64 / Float64 for comparison
            if (left is Data.Int8Series || left is Data.Int16Series || left is Data.UInt8Series || left is Data.UInt16Series)
                return DispatchCompare(PromoteToInt32Series(left), right, op);
            if (right is Data.Int8Series || right is Data.Int16Series || right is Data.UInt8Series || right is Data.UInt16Series)
                return DispatchCompare(left, PromoteToInt32Series(right), op);
            if (left is Data.UInt32Series) return DispatchCompare(PromoteToInt64Series(left), right, op);
            if (right is Data.UInt32Series) return DispatchCompare(left, PromoteToInt64Series(right), op);
            if (left is Data.Float32Series) return DispatchCompare(PromoteToFloat64Series(left), right, op);
            if (right is Data.Float32Series) return DispatchCompare(left, PromoteToFloat64Series(right), op);

            // Handle promotion if needed, but for now strict types
            throw new NotSupportedException($"Comparison between {left.DataType.Name} and {right.DataType.Name} not supported.");
        }

        private static Data.BooleanSeries CompareStringSeries(Data.Utf8StringSeries left, Data.Utf8StringSeries right, Compute.FilterOperation op)
        {
            int len = Math.Max(left.Length, right.Length);
            // Scalar broadcast: if one side is length 1, treat it as a scalar
            bool leftScalar = left.Length == 1;
            bool rightScalar = right.Length == 1;
            var result = new Data.BooleanSeries("cmp", len);
            for (int i = 0; i < len; i++)
            {
                int li = leftScalar ? 0 : i;
                int ri = rightScalar ? 0 : i;
                if (left.ValidityMask.IsNull(li) || right.ValidityMask.IsNull(ri))
                {
                    result.ValidityMask.SetNull(i);
                    continue;
                }
                ReadOnlySpan<byte> lSpan = left.GetStringSpan(li);
                ReadOnlySpan<byte> rSpan = right.GetStringSpan(ri);
                int cmp = lSpan.SequenceCompareTo(rSpan);
                result.Memory.Span[i] = op switch
                {
                    Compute.FilterOperation.Equal => cmp == 0,
                    Compute.FilterOperation.NotEqual => cmp != 0,
                    Compute.FilterOperation.GreaterThan => cmp > 0,
                    Compute.FilterOperation.GreaterThanOrEqual => cmp >= 0,
                    Compute.FilterOperation.LessThan => cmp < 0,
                    Compute.FilterOperation.LessThanOrEqual => cmp <= 0,
                    _ => false
                };
            }
            return result;
        }

        // Thin wrappers so DispatchCompare can delegate to existing PromoteSeries helpers
        private static Data.Int32Series PromoteToInt32Series(ISeries s) => PromoteToInt32(s);
        private static Data.Int64Series PromoteToInt64Series(ISeries s) => PromoteToInt64(s);
        private static Data.Float64Series PromoteToFloat64Series(ISeries s) => PromoteToFloat64(s);

        private ISeries Broadcast(ISeries series, int length)
        {
            if (series.Length == length) return series;
            if (series is Data.Int32Series i32)
            {
                var val = i32.Memory.Span[0];
                var result = new Data.Int32Series(series.Name, length);
                result.Memory.Span.Fill(val);
                result.ValidityMask.CopyFrom(series.ValidityMask);
                return result;
            }
            if (series is Data.Float64Series f64)
            {
                var val = f64.Memory.Span[0];
                var result = new Data.Float64Series(series.Name, length);
                result.Memory.Span.Fill(val);
                result.ValidityMask.CopyFrom(series.ValidityMask);
                return result;
            }
            if (series is Data.Int64Series i64)
            {
                var val = i64.Memory.Span[0];
                var result = new Data.Int64Series(series.Name, length);
                result.Memory.Span.Fill(val);
                result.ValidityMask.CopyFrom(series.ValidityMask);
                return result;
            }
            if (series is Data.BooleanSeries bs)
            {
                var val = bs.Memory.Span[0];
                var result = new Data.BooleanSeries(series.Name, length);
                result.Memory.Span.Fill(val);
                result.ValidityMask.CopyFrom(series.ValidityMask);
                return result;
            }
            return series;
        }

        private ISeries BroadcastScalar(ISeries series, int length)
        {
            if (series.Length == length) return series;
            if (length <= 0) return series;
            if (series is Data.Int32Series i32)
            {
                var val = i32.Memory.Span[0];
                var result = new Data.Int32Series(series.Name, length);
                for (int i = 0; i < length; i++) result.Memory.Span[i] = val;
                return result;
            }
            if (series is Data.Float64Series f64)
            {
                var val = f64.Memory.Span[0];
                var result = new Data.Float64Series(series.Name, length);
                for (int i = 0; i < length; i++) result.Memory.Span[i] = val;
                return result;
            }
            if (series is Data.Int64Series i64)
            {
                var val = i64.Memory.Span[0];
                var result = new Data.Int64Series(series.Name, length);
                for (int i = 0; i < length; i++) result.Memory.Span[i] = val;
                return result;
            }
            if (series is Data.BooleanSeries bs)
            {
                var val = bs.Memory.Span[0];
                var result = new Data.BooleanSeries(series.Name, length);
                for (int i = 0; i < length; i++) result.Memory.Span[i] = val;
                return result;
            }
            return series;
        }

        private ISeries DispatchArithmetic(ISeries left, ISeries right, ExpressionType type)
        {
            // Broadcast scalars to match vector length
            if (left.Length == 1 && right.Length > 1)
                left = BroadcastScalar(left, right.Length);
            else if (right.Length == 1 && left.Length > 1)
                right = BroadcastScalar(right, left.Length);

            // Boolean (And/Or) - BooleanSeries & BooleanSeries = BooleanSeries
            if (left is Data.BooleanSeries lBool && right is Data.BooleanSeries rBool)
            {
                var result = new Data.BooleanSeries("res", lBool.Length);
                if (type == ExpressionType.And)
                    for (int i = 0; i < lBool.Length; i++)
                        result.Memory.Span[i] = lBool.Memory.Span[i] && rBool.Memory.Span[i];
                else if (type == ExpressionType.Or)
                    for (int i = 0; i < lBool.Length; i++)
                        result.Memory.Span[i] = lBool.Memory.Span[i] || rBool.Memory.Span[i];
                else
                    throw new NotSupportedException($"BooleanSeries does not support operation {type}");
                result.ValidityMask.CopyFrom(lBool.ValidityMask);
                result.ValidityMask.And(rBool.ValidityMask);
                return result;
            }

            if (left is Data.DatetimeSeries || left is Data.DateSeries)
            {

                if (right is Data.DurationSeries)
                {
                    if (type == ExpressionType.Add) return Compute.TemporalKernels.AddDuration(left, right);
                    if (type == ExpressionType.Subtract) return Compute.TemporalKernels.SubtractDuration(left, right);
                }
                if (right is Data.DatetimeSeries && type == ExpressionType.Subtract)
                {
                    return Compute.TemporalKernels.Subtract(left, right);
                }
            }

            // Exact type matches (fast path)
            if (left is Data.Int32Series l32 && right is Data.Int32Series r32)
            {
                // Division: Int32/Int32 should promote to Float64 (like Python Polars)
                if (type == ExpressionType.Divide)
                {
                    var divRes = new Data.Float64Series("res", l32.Length);
                    var lSpan = l32.Memory.Span;
                    var rSpan = r32.Memory.Span;
                    var resSpan = divRes.Memory.Span;
                    for (int i = 0; i < l32.Length; i++)
                        resSpan[i] = (double)lSpan[i] / rSpan[i];
                    return divRes;
                }
                var res = new Data.Int32Series("res", l32.Length);
                if (type == ExpressionType.Add) Compute.ArithmeticKernels.Add<int>(l32.Memory.Span, r32.Memory.Span, res.Memory.Span);
                else if (type == ExpressionType.Subtract) Compute.ArithmeticKernels.Subtract<int>(l32.Memory.Span, r32.Memory.Span, res.Memory.Span);
                else if (type == ExpressionType.Multiply) Compute.ArithmeticKernels.Multiply<int>(l32.Memory.Span, r32.Memory.Span, res.Memory.Span);
                return res;
            }
            if (left is Data.Float64Series lF64 && right is Data.Float64Series rF64)
            {
                var res = new Data.Float64Series("res", lF64.Length);
                if (type == ExpressionType.Add) Compute.ArithmeticKernels.Add<double>(lF64.Memory.Span, rF64.Memory.Span, res.Memory.Span);
                else if (type == ExpressionType.Subtract) Compute.ArithmeticKernels.Subtract<double>(lF64.Memory.Span, rF64.Memory.Span, res.Memory.Span);
                else if (type == ExpressionType.Multiply) Compute.ArithmeticKernels.Multiply<double>(lF64.Memory.Span, rF64.Memory.Span, res.Memory.Span);
                else if (type == ExpressionType.Divide) Compute.ArithmeticKernels.Divide<double>(lF64.Memory.Span, rF64.Memory.Span, res.Memory.Span);
                return res;
            }

            if (left is Data.Int64Series l64 && right is Data.Int64Series r64)
            {
                var res = new Data.Int64Series("res", l64.Length);
                if (type == ExpressionType.Add) Compute.ArithmeticKernels.Add<long>(l64.Memory.Span, r64.Memory.Span, res.Memory.Span);
                else if (type == ExpressionType.Subtract) Compute.ArithmeticKernels.Subtract<long>(l64.Memory.Span, r64.Memory.Span, res.Memory.Span);
                else if (type == ExpressionType.Multiply) Compute.ArithmeticKernels.Multiply<long>(l64.Memory.Span, r64.Memory.Span, res.Memory.Span);
                else if (type == ExpressionType.Divide) Compute.ArithmeticKernels.Divide<long>(l64.Memory.Span, r64.Memory.Span, res.Memory.Span);
                return res;
            }

            // Same-type matches for the remaining integer and float types
            if (left is Data.Int8Series lI8 && right is Data.Int8Series rI8)
            {
                var res = new Data.Int32Series("res", lI8.Length); // Widen to Int32
                if (type == ExpressionType.Add) for (int i = 0; i < lI8.Length; i++) res.Memory.Span[i] = lI8.Memory.Span[i] + rI8.Memory.Span[i];
                else if (type == ExpressionType.Subtract) for (int i = 0; i < lI8.Length; i++) res.Memory.Span[i] = lI8.Memory.Span[i] - rI8.Memory.Span[i];
                else if (type == ExpressionType.Multiply) for (int i = 0; i < lI8.Length; i++) res.Memory.Span[i] = lI8.Memory.Span[i] * rI8.Memory.Span[i];
                else if (type == ExpressionType.Divide) for (int i = 0; i < lI8.Length; i++) res.Memory.Span[i] = lI8.Memory.Span[i] / rI8.Memory.Span[i];
                return res;
            }
            if (left is Data.Int16Series lI16 && right is Data.Int16Series rI16)
            {
                var res = new Data.Int32Series("res", lI16.Length); // Widen to Int32
                if (type == ExpressionType.Add) for (int i = 0; i < lI16.Length; i++) res.Memory.Span[i] = lI16.Memory.Span[i] + rI16.Memory.Span[i];
                else if (type == ExpressionType.Subtract) for (int i = 0; i < lI16.Length; i++) res.Memory.Span[i] = lI16.Memory.Span[i] - rI16.Memory.Span[i];
                else if (type == ExpressionType.Multiply) for (int i = 0; i < lI16.Length; i++) res.Memory.Span[i] = lI16.Memory.Span[i] * rI16.Memory.Span[i];
                else if (type == ExpressionType.Divide) for (int i = 0; i < lI16.Length; i++) res.Memory.Span[i] = lI16.Memory.Span[i] / rI16.Memory.Span[i];
                return res;
            }
            if (left is Data.UInt8Series lU8 && right is Data.UInt8Series rU8)
            {
                var res = new Data.Int32Series("res", lU8.Length); // Widen to Int32
                if (type == ExpressionType.Add) for (int i = 0; i < lU8.Length; i++) res.Memory.Span[i] = lU8.Memory.Span[i] + rU8.Memory.Span[i];
                else if (type == ExpressionType.Subtract) for (int i = 0; i < lU8.Length; i++) res.Memory.Span[i] = lU8.Memory.Span[i] - rU8.Memory.Span[i];
                else if (type == ExpressionType.Multiply) for (int i = 0; i < lU8.Length; i++) res.Memory.Span[i] = lU8.Memory.Span[i] * rU8.Memory.Span[i];
                else if (type == ExpressionType.Divide) for (int i = 0; i < lU8.Length; i++) res.Memory.Span[i] = lU8.Memory.Span[i] / rU8.Memory.Span[i];
                return res;
            }
            if (left is Data.UInt16Series lU16 && right is Data.UInt16Series rU16)
            {
                var res = new Data.Int32Series("res", lU16.Length); // Widen to Int32
                if (type == ExpressionType.Add) for (int i = 0; i < lU16.Length; i++) res.Memory.Span[i] = lU16.Memory.Span[i] + rU16.Memory.Span[i];
                else if (type == ExpressionType.Subtract) for (int i = 0; i < lU16.Length; i++) res.Memory.Span[i] = lU16.Memory.Span[i] - rU16.Memory.Span[i];
                else if (type == ExpressionType.Multiply) for (int i = 0; i < lU16.Length; i++) res.Memory.Span[i] = lU16.Memory.Span[i] * rU16.Memory.Span[i];
                else if (type == ExpressionType.Divide) for (int i = 0; i < lU16.Length; i++) res.Memory.Span[i] = lU16.Memory.Span[i] / rU16.Memory.Span[i];
                return res;
            }
            if (left is Data.UInt32Series lU32 && right is Data.UInt32Series rU32)
            {
                var res = new Data.Int64Series("res", lU32.Length); // Widen to Int64
                if (type == ExpressionType.Add) for (int i = 0; i < lU32.Length; i++) res.Memory.Span[i] = (long)lU32.Memory.Span[i] + (long)rU32.Memory.Span[i];
                else if (type == ExpressionType.Subtract) for (int i = 0; i < lU32.Length; i++) res.Memory.Span[i] = (long)lU32.Memory.Span[i] - (long)rU32.Memory.Span[i];
                else if (type == ExpressionType.Multiply) for (int i = 0; i < lU32.Length; i++) res.Memory.Span[i] = (long)lU32.Memory.Span[i] * (long)rU32.Memory.Span[i];
                else if (type == ExpressionType.Divide) for (int i = 0; i < lU32.Length; i++) res.Memory.Span[i] = (long)lU32.Memory.Span[i] / (long)rU32.Memory.Span[i];
                return res;
            }
            if (left is Data.UInt64Series lU64 && right is Data.UInt64Series rU64)
            {
                var res = new Data.Float64Series("res", lU64.Length); // Widen to Float64 for safety
                if (type == ExpressionType.Add) for (int i = 0; i < lU64.Length; i++) res.Memory.Span[i] = (double)lU64.Memory.Span[i] + (double)rU64.Memory.Span[i];
                else if (type == ExpressionType.Subtract) for (int i = 0; i < lU64.Length; i++) res.Memory.Span[i] = (double)lU64.Memory.Span[i] - (double)rU64.Memory.Span[i];
                else if (type == ExpressionType.Multiply) for (int i = 0; i < lU64.Length; i++) res.Memory.Span[i] = (double)lU64.Memory.Span[i] * (double)rU64.Memory.Span[i];
                else if (type == ExpressionType.Divide) for (int i = 0; i < lU64.Length; i++) res.Memory.Span[i] = (double)lU64.Memory.Span[i] / (double)rU64.Memory.Span[i];
                return res;
            }
            if (left is Data.Float32Series lF32 && right is Data.Float32Series rF32)
            {
                var res = new Data.Float64Series("res", lF32.Length); // Widen to Float64
                if (type == ExpressionType.Add) for (int i = 0; i < lF32.Length; i++) res.Memory.Span[i] = (double)lF32.Memory.Span[i] + (double)rF32.Memory.Span[i];
                else if (type == ExpressionType.Subtract) for (int i = 0; i < lF32.Length; i++) res.Memory.Span[i] = (double)lF32.Memory.Span[i] - (double)rF32.Memory.Span[i];
                else if (type == ExpressionType.Multiply) for (int i = 0; i < lF32.Length; i++) res.Memory.Span[i] = (double)lF32.Memory.Span[i] * (double)rF32.Memory.Span[i];
                else if (type == ExpressionType.Divide) for (int i = 0; i < lF32.Length; i++) res.Memory.Span[i] = (double)lF32.Memory.Span[i] / (double)rF32.Memory.Span[i];
                return res;
            }


            // Mixed-type promotion: promote narrower types to wider types
            // Determine the target type based on the widest type present
            var typeOrder = GetNumericTypeOrder(left, right);
            if (typeOrder != null)
            {
                var (promotedLeft, promotedRight, resultType) = typeOrder.Value;
                if (promotedLeft != null) return DispatchArithmetic(promotedLeft, right, type);
                if (promotedRight != null) return DispatchArithmetic(left, promotedRight, type);
                // Both promoted? shouldn't happen since we assign both
            }

            // Handle NullSeries: NullSeries op X = NullSeries
            if (left is Data.NullSeries) return new Data.NullSeries("null", left.Length);
            if (right is Data.NullSeries) return new Data.NullSeries("null", right.Length);

            // Handle Decimal arithmetic (decimal doesn't support INumber<T>, use manual loop)
            if (left is Data.DecimalSeries lDec && right is Data.DecimalSeries rDec)
            {
                var res = new Data.DecimalSeries("res", lDec.Length);
                var lSpan = lDec.Memory.Span;
                var rSpan = rDec.Memory.Span;
                var rSpan2 = res.Memory.Span;
                if (type == ExpressionType.Add)
                    for (int i = 0; i < lSpan.Length; i++) rSpan2[i] = lSpan[i] + rSpan[i];
                else if (type == ExpressionType.Subtract)
                    for (int i = 0; i < lSpan.Length; i++) rSpan2[i] = lSpan[i] - rSpan[i];
                else if (type == ExpressionType.Multiply)
                    for (int i = 0; i < lSpan.Length; i++) rSpan2[i] = lSpan[i] * rSpan[i];
                else if (type == ExpressionType.Divide)
                    for (int i = 0; i < lSpan.Length; i++) rSpan2[i] = lSpan[i] / rSpan[i];
                return res;
            }

            throw new NotSupportedException($"Arithmetic between {left.DataType.Name} and {right.DataType.Name} not supported.");
        }

        /// <summary>
        /// Determines if type promotion is needed and returns (promotedLeft, promotedRight, resultTypeName).
        /// If a promotion is needed, sets the appropriate side to the promoted series.
        /// </summary>
        private (ISeries? promotedLeft, ISeries? promotedRight, string resultType)? GetNumericTypeOrder(ISeries left, ISeries right)
        {
            // Assign each type a numeric rank (higher = wider)
            int Rank(ISeries s)
            {
                if (s is Data.Float64Series) return 80;
                if (s is Data.Float32Series) return 70;
                if (s is Data.Int64Series) return 60;
                if (s is Data.UInt64Series) return 59; // UInt64 can't safely fit in Int64
                if (s is Data.UInt32Series) return 55;
                if (s is Data.Int32Series) return 50;
                if (s is Data.UInt16Series) return 45;
                if (s is Data.Int16Series) return 40;
                if (s is Data.UInt8Series) return 35;
                if (s is Data.Int8Series) return 30;
                return 0; // Non-numeric
            }

            int leftRank = Rank(left);
            int rightRank = Rank(right);
            if (leftRank == 0 || rightRank == 0) return null; // Non-numeric, skip

            if (leftRank == rightRank) return null; // Same type, already handled

            // Determine if either side needs promotion
            ISeries? promotedLeft = null;
            ISeries? promotedRight = null;
            string resultType = "";

            if (leftRank < rightRank)
            {
                // Promote left to match right
                promotedLeft = PromoteSeries(left, right);
                resultType = right.DataType.Name;
            }
            else
            {
                // Promote right to match left
                promotedRight = PromoteSeries(right, left);
                resultType = left.DataType.Name;
            }

            return (promotedLeft, promotedRight, resultType);
        }

        /// <summary>
        /// Create a new series by promoting 'source' to match 'target's type.
        /// </summary>
        private static ISeries PromoteSeries(ISeries source, ISeries target)
        {
            if (target is Data.Float64Series)
            {
                return PromoteToFloat64(source);
            }
            if (target is Data.Float32Series)
            {
                return PromoteToFloat32(source);
            }
            if (target is Data.Int64Series)
            {
                return PromoteToInt64(source);
            }
            if (target is Data.UInt64Series)
            {
                return PromoteToUInt64(source);
            }
            if (target is Data.UInt32Series)
            {
                return PromoteToUInt32(source);
            }
            if (target is Data.Int32Series)
            {
                return PromoteToInt32(source);
            }
            if (target is Data.UInt16Series)
            {
                return PromoteToUInt16(source);
            }
            if (target is Data.Int16Series)
            {
                return PromoteToInt16(source);
            }
            if (target is Data.UInt8Series)
            {
                return PromoteToUInt8(source);
            }
            if (target is Data.Int8Series)
            {
                return PromoteToInt8(source);
            }
            throw new NotSupportedException($"Cannot promote to {target.DataType.Name}");
        }

        private static Data.Float64Series PromoteToFloat64(ISeries source)
        {
            var f64 = new Data.Float64Series(source.Name, source.Length);
            for (int i = 0; i < source.Length; i++)
            {
                if (source.ValidityMask.IsNull(i)) { f64.ValidityMask.SetNull(i); continue; }
                f64.Memory.Span[i] = source.Get(i) switch
                {
                    sbyte v => v,
                    byte v => v,
                    short v => v,
                    ushort v => v,
                    int v => v,
                    uint v => v,
                    long v => v,
                    ulong v => v,
                    float v => v,
                    double v => v,
                    _ => 0
                };
            }
            f64.ValidityMask.CopyFrom(source.ValidityMask);
            return f64;
        }

        private static Data.Float32Series PromoteToFloat32(ISeries source)
        {
            var f32 = new Data.Float32Series(source.Name, source.Length);
            for (int i = 0; i < source.Length; i++)
            {
                if (source.ValidityMask.IsNull(i)) { f32.ValidityMask.SetNull(i); continue; }
                f32.Memory.Span[i] = Convert.ToSingle(source.Get(i));
            }
            f32.ValidityMask.CopyFrom(source.ValidityMask);
            return f32;
        }

        private static Data.Int64Series PromoteToInt64(ISeries source)
        {
            var i64 = new Data.Int64Series(source.Name, source.Length);
            for (int i = 0; i < source.Length; i++)
            {
                if (source.ValidityMask.IsNull(i)) { i64.ValidityMask.SetNull(i); continue; }
                i64.Memory.Span[i] = Convert.ToInt64(source.Get(i));
            }
            i64.ValidityMask.CopyFrom(source.ValidityMask);
            return i64;
        }

        private static Data.UInt64Series PromoteToUInt64(ISeries source)
        {
            var u64 = new Data.UInt64Series(source.Name, source.Length);
            for (int i = 0; i < source.Length; i++)
            {
                if (source.ValidityMask.IsNull(i)) { u64.ValidityMask.SetNull(i); continue; }
                u64.Memory.Span[i] = Convert.ToUInt64(source.Get(i));
            }
            u64.ValidityMask.CopyFrom(source.ValidityMask);
            return u64;
        }

        private static Data.UInt32Series PromoteToUInt32(ISeries source)
        {
            var u32 = new Data.UInt32Series(source.Name, source.Length);
            for (int i = 0; i < source.Length; i++)
            {
                if (source.ValidityMask.IsNull(i)) { u32.ValidityMask.SetNull(i); continue; }
                u32.Memory.Span[i] = Convert.ToUInt32(source.Get(i));
            }
            u32.ValidityMask.CopyFrom(source.ValidityMask);
            return u32;
        }

        private static Data.Int32Series PromoteToInt32(ISeries source)
        {
            var i32 = new Data.Int32Series(source.Name, source.Length);
            for (int i = 0; i < source.Length; i++)
            {
                if (source.ValidityMask.IsNull(i)) { i32.ValidityMask.SetNull(i); continue; }
                i32.Memory.Span[i] = Convert.ToInt32(source.Get(i));
            }
            i32.ValidityMask.CopyFrom(source.ValidityMask);
            return i32;
        }

        private static Data.UInt16Series PromoteToUInt16(ISeries source)
        {
            var u16 = new Data.UInt16Series(source.Name, source.Length);
            for (int i = 0; i < source.Length; i++)
            {
                if (source.ValidityMask.IsNull(i)) { u16.ValidityMask.SetNull(i); continue; }
                u16.Memory.Span[i] = Convert.ToUInt16(source.Get(i));
            }
            u16.ValidityMask.CopyFrom(source.ValidityMask);
            return u16;
        }

        private static Data.Int16Series PromoteToInt16(ISeries source)
        {
            var i16 = new Data.Int16Series(source.Name, source.Length);
            for (int i = 0; i < source.Length; i++)
            {
                if (source.ValidityMask.IsNull(i)) { i16.ValidityMask.SetNull(i); continue; }
                i16.Memory.Span[i] = Convert.ToInt16(source.Get(i));
            }
            i16.ValidityMask.CopyFrom(source.ValidityMask);
            return i16;
        }

        private static Data.UInt8Series PromoteToUInt8(ISeries source)
        {
            var u8 = new Data.UInt8Series(source.Name, source.Length);
            for (int i = 0; i < source.Length; i++)
            {
                if (source.ValidityMask.IsNull(i)) { u8.ValidityMask.SetNull(i); continue; }
                u8.Memory.Span[i] = Convert.ToByte(source.Get(i));
            }
            u8.ValidityMask.CopyFrom(source.ValidityMask);
            return u8;
        }

        private static Data.Int8Series PromoteToInt8(ISeries source)
        {
            var i8 = new Data.Int8Series(source.Name, source.Length);
            for (int i = 0; i < source.Length; i++)
            {
                if (source.ValidityMask.IsNull(i)) { i8.ValidityMask.SetNull(i); continue; }
                i8.Memory.Span[i] = Convert.ToSByte(source.Get(i));
            }
            i8.ValidityMask.CopyFrom(source.ValidityMask);
            return i8;
        }

    }
}
