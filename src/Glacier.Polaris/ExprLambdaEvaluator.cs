using System;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;

namespace Glacier.Polaris
{
    /// <summary>
    /// Evaluates <see cref="Expression{TDelegate}"/> trees producing <see cref="Expr"/> instances
    /// directly without calling <c>LambdaExpression.Compile()</c>, maintaining full Native AOT compatibility.
    /// </summary>
    internal static class ExprLambdaEvaluator
    {
        public static Expr Evaluate(Expression expression)
        {
            while (expression is UnaryExpression unary && unary.NodeType == ExpressionType.Quote)
            {
                expression = unary.Operand;
            }

            if (expression is LambdaExpression lambda)
            {
                expression = lambda.Body;
            }

            var value = EvaluateNode(expression);
            if (value is Expr expr)
            {
                return expr;
            }

            throw new InvalidOperationException($"Expression did not evaluate to Expr: {expression}");
        }

        private static object? EvaluateNode(Expression node)
        {
            switch (node)
            {
                case ConstantExpression c:
                    return c.Value;

                case ParameterExpression:
                    return null;

                case MemberExpression m:
                {
                    object? target = m.Expression != null ? EvaluateNode(m.Expression) : null;
                    if (m.Member is FieldInfo fi)
                        return fi.GetValue(target);
                    if (m.Member is PropertyInfo pi)
                        return pi.GetValue(target);
                    throw new NotSupportedException($"Unsupported member expression: {m}");
                }

                case MethodCallExpression mc:
                {
                    object? instance = mc.Object != null ? EvaluateNode(mc.Object) : null;
                    var args = new object?[mc.Arguments.Count];
                    for (int i = 0; i < args.Length; i++)
                    {
                        args[i] = EvaluateNode(mc.Arguments[i]);
                    }
                    return mc.Method.Invoke(instance, args);
                }

                case UnaryExpression u:
                {
                    if (u.NodeType == ExpressionType.Quote)
                    {
                        return u.Operand;
                    }

                    object? operand = EvaluateNode(u.Operand);
                    if (u.Method != null)
                    {
                        return u.Method.Invoke(null, new[] { operand });
                    }

                    return u.NodeType switch
                    {
                        ExpressionType.Convert or ExpressionType.ConvertChecked => ConvertPrimitive(operand, u.Type),
                        ExpressionType.Negate or ExpressionType.NegateChecked => NegatePrimitive(operand),
                        ExpressionType.Not => operand is bool b ? !b : throw new NotSupportedException($"Unsupported Not operand: {operand}"),
                        ExpressionType.UnaryPlus => operand,
                        ExpressionType.ArrayLength => ((Array)operand!).Length,
                        _ => throw new NotSupportedException($"Unsupported unary expression: {u}")
                    };
                }

                case BinaryExpression b:
                {
                    if (b.NodeType == ExpressionType.AndAlso)
                        return (bool)EvaluateNode(b.Left)! && (bool)EvaluateNode(b.Right)!;
                    if (b.NodeType == ExpressionType.OrElse)
                        return (bool)EvaluateNode(b.Left)! || (bool)EvaluateNode(b.Right)!;
                    if (b.NodeType == ExpressionType.Coalesce)
                        return EvaluateNode(b.Left) ?? EvaluateNode(b.Right);

                    object? left = EvaluateNode(b.Left);
                    object? right = EvaluateNode(b.Right);

                    if (b.NodeType == ExpressionType.ArrayIndex)
                        return ((Array)left!).GetValue(Convert.ToInt32(right, CultureInfo.InvariantCulture));

                    if (b.Method != null)
                        return b.Method.Invoke(null, new[] { left, right });

                    return EvaluatePrimitiveBinary(b.NodeType, left, right);
                }

                case NewArrayExpression na:
                    return EvaluateNewArray(na);

                case NewExpression ne:
                {
                    if (ne.Constructor == null)
                        throw new NotSupportedException($"Parameterless value-type construction not supported in expression: {ne}");
                    var args = new object?[ne.Arguments.Count];
                    for (int i = 0; i < args.Length; i++)
                        args[i] = EvaluateNode(ne.Arguments[i]);
                    return ne.Constructor.Invoke(args);
                }

                case ConditionalExpression ce:
                    return (bool)EvaluateNode(ce.Test)! ? EvaluateNode(ce.IfTrue) : EvaluateNode(ce.IfFalse);

                default:
                    throw new NotSupportedException($"Unsupported expression node in Expr lambda: {node.NodeType} ({node})");
            }
        }

        private static object EvaluateNewArray(NewArrayExpression na)
        {
            int count = na.Expressions.Count;
            Type elemType = na.Type.GetElementType()!;

            if (elemType == typeof(Expr))
            {
                var arr = new Expr[count];
                for (int i = 0; i < count; i++) arr[i] = (Expr)EvaluateNode(na.Expressions[i])!;
                return arr;
            }
            if (elemType == typeof(string))
            {
                var arr = new string[count];
                for (int i = 0; i < count; i++) arr[i] = (string)EvaluateNode(na.Expressions[i])!;
                return arr;
            }
            if (elemType == typeof(int))
            {
                var arr = new int[count];
                for (int i = 0; i < count; i++) arr[i] = (int)EvaluateNode(na.Expressions[i])!;
                return arr;
            }
            if (elemType == typeof(long))
            {
                var arr = new long[count];
                for (int i = 0; i < count; i++) arr[i] = (long)EvaluateNode(na.Expressions[i])!;
                return arr;
            }
            if (elemType == typeof(double))
            {
                var arr = new double[count];
                for (int i = 0; i < count; i++) arr[i] = (double)EvaluateNode(na.Expressions[i])!;
                return arr;
            }
            if (elemType == typeof(bool))
            {
                var arr = new bool[count];
                for (int i = 0; i < count; i++) arr[i] = (bool)EvaluateNode(na.Expressions[i])!;
                return arr;
            }

            var objArr = new object?[count];
            for (int i = 0; i < count; i++) objArr[i] = EvaluateNode(na.Expressions[i]);
            return objArr;
        }

        private static object? ConvertPrimitive(object? value, Type targetType)
        {
            if (value == null) return null;
            if (targetType == typeof(object) || targetType.IsInstanceOfType(value)) return value;
            Type underlying = Nullable.GetUnderlyingType(targetType) ?? targetType;
            if (underlying.IsInstanceOfType(value)) return value;
            if (underlying.IsEnum) return Enum.ToObject(underlying, value);
            return Convert.ChangeType(value, underlying, CultureInfo.InvariantCulture);
        }

        private static object NegatePrimitive(object? value) => value switch
        {
            int i => -i,
            long l => -l,
            double d => -d,
            float f => -f,
            decimal m => -m,
            _ => throw new NotSupportedException($"Cannot negate {value}")
        };

        private static object EvaluatePrimitiveBinary(ExpressionType nodeType, object? left, object? right)
        {
            if (nodeType == ExpressionType.Equal) return Equals(left, right);
            if (nodeType == ExpressionType.NotEqual) return !Equals(left, right);

            if (left is string ls && right is string rs && nodeType == ExpressionType.Add)
                return ls + rs;

            if (left is double || right is double)
            {
                double ld = Convert.ToDouble(left, CultureInfo.InvariantCulture);
                double rd = Convert.ToDouble(right, CultureInfo.InvariantCulture);
                return nodeType switch
                {
                    ExpressionType.Add or ExpressionType.AddChecked => ld + rd,
                    ExpressionType.Subtract or ExpressionType.SubtractChecked => ld - rd,
                    ExpressionType.Multiply or ExpressionType.MultiplyChecked => ld * rd,
                    ExpressionType.Divide => ld / rd,
                    ExpressionType.GreaterThan => ld > rd,
                    ExpressionType.GreaterThanOrEqual => ld >= rd,
                    ExpressionType.LessThan => ld < rd,
                    ExpressionType.LessThanOrEqual => ld <= rd,
                    _ => throw new NotSupportedException($"Unsupported binary op {nodeType}")
                };
            }

            if (left is long || right is long)
            {
                long ll = Convert.ToInt64(left, CultureInfo.InvariantCulture);
                long rl = Convert.ToInt64(right, CultureInfo.InvariantCulture);
                return nodeType switch
                {
                    ExpressionType.Add or ExpressionType.AddChecked => ll + rl,
                    ExpressionType.Subtract or ExpressionType.SubtractChecked => ll - rl,
                    ExpressionType.Multiply or ExpressionType.MultiplyChecked => ll * rl,
                    ExpressionType.Divide => ll / rl,
                    ExpressionType.Modulo => ll % rl,
                    ExpressionType.GreaterThan => ll > rl,
                    ExpressionType.GreaterThanOrEqual => ll >= rl,
                    ExpressionType.LessThan => ll < rl,
                    ExpressionType.LessThanOrEqual => ll <= rl,
                    _ => throw new NotSupportedException($"Unsupported binary op {nodeType}")
                };
            }

            int li = Convert.ToInt32(left, CultureInfo.InvariantCulture);
            int ri = Convert.ToInt32(right, CultureInfo.InvariantCulture);
            return nodeType switch
            {
                ExpressionType.Add or ExpressionType.AddChecked => li + ri,
                ExpressionType.Subtract or ExpressionType.SubtractChecked => li - ri,
                ExpressionType.Multiply or ExpressionType.MultiplyChecked => li * ri,
                ExpressionType.Divide => li / ri,
                ExpressionType.Modulo => li % ri,
                ExpressionType.GreaterThan => li > ri,
                ExpressionType.GreaterThanOrEqual => li >= ri,
                ExpressionType.LessThan => li < ri,
                ExpressionType.LessThanOrEqual => li <= ri,
                _ => throw new NotSupportedException($"Unsupported binary op {nodeType}")
            };
        }
    }
}
