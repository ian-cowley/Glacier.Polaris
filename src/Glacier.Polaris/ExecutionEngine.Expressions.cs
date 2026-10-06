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
        private ISeries EvaluateExpression(Expression body, DataFrame df, List<IDisposable> disposables)
        {
            if (body is MethodCallExpression mce)
            {
                var res = EvaluateWindowAndAggOp(mce, df, disposables);
                if (res != null) return res;
                res = EvaluateStringAndConditionOp(mce, df, disposables);
                if (res != null) return res;
                res = EvaluateListAndStructOp(mce, df, disposables);
                if (res != null) return res;
                res = EvaluateTemporalOp(mce, df, disposables);
                if (res != null) return res;
                res = EvaluateMathAndArrayOp(mce, df, disposables);
                if (res != null) return res;
            }
            if (body is BinaryExpression binary)
            {
                var left = EvaluateExpression(binary.Left, df, disposables);
                var right = EvaluateExpression(binary.Right, df, disposables);

                bool isCmp = binary.NodeType == ExpressionType.Equal || binary.NodeType == ExpressionType.NotEqual ||
                             binary.NodeType == ExpressionType.GreaterThan || binary.NodeType == ExpressionType.GreaterThanOrEqual ||
                             binary.NodeType == ExpressionType.LessThan || binary.NodeType == ExpressionType.LessThanOrEqual;

                Compute.FilterOperation cmpOp = isCmp ? binary.NodeType switch
                {
                    ExpressionType.Equal => Compute.FilterOperation.Equal,
                    ExpressionType.NotEqual => Compute.FilterOperation.NotEqual,
                    ExpressionType.GreaterThan => Compute.FilterOperation.GreaterThan,
                    ExpressionType.GreaterThanOrEqual => Compute.FilterOperation.GreaterThanOrEqual,
                    ExpressionType.LessThan => Compute.FilterOperation.LessThan,
                    ExpressionType.LessThanOrEqual => Compute.FilterOperation.LessThanOrEqual,
                    _ => default
                } : default;

                if (isCmp)
                {
                    var result = DispatchCompare(left, right, cmpOp);
                    CombineMasks(left, right, result);
                    disposables.Add(result);
                    return result;
                }

                // Arithmetic
                var arithResult = DispatchArithmetic(left, right, binary.NodeType);
                CombineMasks(left, right, arithResult);
                disposables.Add(arithResult);
                return arithResult;
            }

            if (body is UnaryExpression unary && unary.NodeType == ExpressionType.Negate)
            {
                var operand = EvaluateExpression(unary.Operand, df, disposables);
                if (operand is Data.Int32Series i32)
                {
                    var result = new Data.Int32Series("neg", i32.Length);
                    for (int i = 0; i < i32.Length; i++)
                        if (i32.ValidityMask.IsValid(i))
                            result.Memory.Span[i] = -i32.Memory.Span[i];
                        else
                            result.ValidityMask.SetNull(i);
                    disposables.Add(result);
                    return result;
                }
                if (operand is Data.Int64Series i64)
                {
                    var result = new Data.Int64Series("neg", i64.Length);
                    for (int i = 0; i < i64.Length; i++)
                        if (i64.ValidityMask.IsValid(i))
                            result.Memory.Span[i] = -i64.Memory.Span[i];
                        else
                            result.ValidityMask.SetNull(i);
                    disposables.Add(result);
                    return result;
                }
                if (operand is Data.Float64Series f64)
                {
                    var result = new Data.Float64Series("neg", f64.Length);
                    for (int i = 0; i < f64.Length; i++)
                        if (f64.ValidityMask.IsValid(i))
                            result.Memory.Span[i] = -f64.Memory.Span[i];
                        else
                            result.ValidityMask.SetNull(i);
                    disposables.Add(result);
                    return result;
                }
                throw new NotSupportedException($"Negation not supported for {operand.DataType.Name}");
            }

            if (body is ConstantExpression constExpr)
            {
                return CreateLiteralSeries(constExpr.Value, df.RowCount);
            }

            throw new NotSupportedException($"Unsupported expression: {body}");

        }
    }
}
