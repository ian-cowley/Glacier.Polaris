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
        private ISeries? EvaluateListAndStructOp(MethodCallExpression mce, DataFrame df, List<IDisposable> disposables)
        {
                if (mce.Method.Name == "List_SumOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.ListSeries list)
                    {
                        var result = Compute.ListKernels.Sum(list.Offsets, list.Values);
                        PropagateListNulls(list, result);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.Sum requires ListSeries.");
                }

                else if (mce.Method.Name == "List_MeanOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.ListSeries list)
                    {
                        var result = Compute.ListKernels.Mean(list.Offsets, list.Values);
                        PropagateListNulls(list, result);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.Mean requires ListSeries.");
                }
                else if (mce.Method.Name == "List_MinOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.ListSeries list)
                    {
                        var result = Compute.ListKernels.Min(list.Offsets, list.Values);
                        PropagateListNulls(list, result);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.Min requires ListSeries.");
                }
                else if (mce.Method.Name == "List_MaxOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.ListSeries list)
                    {
                        var result = Compute.ListKernels.Max(list.Offsets, list.Values);
                        PropagateListNulls(list, result);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.Max requires ListSeries.");
                }

                else if (mce.Method.Name == "List_LengthsOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.ListSeries list)
                    {
                        var result = Compute.ListKernels.Lengths(list.Offsets);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.Lengths requires ListSeries.");
                }
                else if (mce.Method.Name == "List_GetOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int index = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.ListSeries list)
                    {
                        var result = Compute.ListKernels.GetItem(list.Offsets, list.Values, index);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.Get requires ListSeries.");
                }
                else if (mce.Method.Name == "List_ContainsOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var elementExpr = mce.Arguments[1];
                    object? element = null;

                    var actualExpr = elementExpr;
                    if (actualExpr is UnaryExpression ue && ue.NodeType == ExpressionType.Convert)
                        actualExpr = ue.Operand;

                    if (actualExpr is ConstantExpression ce)
                    {
                        element = ce.Value;
                        // If the element is an Expr (e.g., Expr.Lit(1)), evaluate it to get the actual value
                        if (element is Expr exprElement)
                        {
                            var elementSeries = EvaluateExpression(exprElement.Expression, df, disposables);
                            element = elementSeries.Get(0);
                        }
                    }
                    else
                    {
                        var elementSeries = EvaluateExpression(elementExpr, df, disposables);
                        element = elementSeries.Get(0);
                    }

                    if (series is Data.ListSeries list && element != null)
                    {
                        var result = Compute.ListKernels.Contains(list.Offsets, list.Values, element);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.Contains requires ListSeries and a valid element.");
                }

                else if (mce.Method.Name == "List_JoinOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string sep = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.ListSeries list)
                    {
                        var result = Compute.ListKernels.Join(list.Offsets, list.Values, sep);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.Join requires ListSeries.");
                }
                else if (mce.Method.Name == "List_UniqueOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.ListSeries list)
                    {
                        var result = Compute.ListKernels.Unique(list.Offsets, list.Values);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.Unique requires ListSeries.");
                }
                else if (mce.Method.Name == "List_SortOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    bool descending = (bool)((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.ListSeries list)
                    {
                        var result = Compute.ListKernels.Sort(list.Offsets, list.Values, descending);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.Sort requires ListSeries.");
                }
                else if (mce.Method.Name == "List_ReverseOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.ListSeries list)
                    {
                        var result = Compute.ListKernels.Reverse(list.Offsets, list.Values);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.Reverse requires ListSeries.");
                }
                else if (mce.Method.Name == "List_ArgMinOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.ListSeries list)
                    {
                        var result = Compute.ListKernels.ArgMin(list.Offsets, list.Values);
                        PropagateListNulls(list, result);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.ArgMin requires ListSeries.");
                }
                else if (mce.Method.Name == "List_ArgMaxOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.ListSeries list)
                    {
                        var result = Compute.ListKernels.ArgMax(list.Offsets, list.Values);
                        PropagateListNulls(list, result);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.ArgMax requires ListSeries.");
                }
                else if (mce.Method.Name == "List_DiffOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int n = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.ListSeries list)
                    {
                        var result = Compute.ListKernels.Diff(list.Offsets, list.Values, n);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.Diff requires ListSeries.");
                }
                else if (mce.Method.Name == "List_ShiftOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int n = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.ListSeries list)
                    {
                        var result = Compute.ListKernels.Shift(list.Offsets, list.Values, n);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.Shift requires ListSeries.");
                }
                else if (mce.Method.Name == "List_SliceOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int offset = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    int? lengthArg = (int?)((ConstantExpression)mce.Arguments[2]).Value;
                    if (series is Data.ListSeries list)
                    {
                        var result = Compute.ListKernels.Slice(list.Offsets, list.Values, offset, lengthArg);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.Slice requires ListSeries.");
                }
                else if (mce.Method.Name == "List_EvalOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.ListSeries list)
                    {
                        var elemExprBody = mce.Arguments[1];
                        var result = Compute.ListKernels.Eval(list, (subDf) => EvaluateExpression(elemExprBody, subDf, disposables));
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.Eval requires ListSeries.");
                }
                else if (mce.Method.Name == "ElementOp")
                {
                    return df.GetColumn("element");
                }
                else if (mce.Method.Name == "Struct_FieldOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string fieldName = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.StructSeries structSeries)
                    {
                        var field = structSeries.Fields.FirstOrDefault(f => f.Name == fieldName);
                        if (field == null) throw new ArgumentException($"Field '{fieldName}' not found in struct.");
                        return field;
                    }
                    throw new NotSupportedException("Struct.Field requires StructSeries.");
                }
                else if (mce.Method.Name == "Struct_RenameFieldsOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string[] newNames = (string[])((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.StructSeries structSeries)
                    {
                        var result = Compute.StructKernels.RenameFields(structSeries, newNames);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("Struct.RenameFields requires StructSeries.");
                }
                else if (mce.Method.Name == "Struct_JsonEncodeOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.StructSeries structSeries)
                    {
                        var result = Compute.StructKernels.JsonEncode(structSeries);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("Struct.JsonEncode requires StructSeries.");
                }
                else if (mce.Method.Name == "Struct_WithFieldsOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var fieldExprArr = (Expr[])((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.StructSeries structSeries)
                    {
                        var newFields = new List<ISeries>();
                        foreach (var fe in fieldExprArr)
                        {
                            var fieldBody = fe.Expression;
                            string alias = null;
                            if (fieldBody is MethodCallExpression fmce && fmce.Method.Name == "AliasOp")
                            {
                                alias = (string)((ConstantExpression)fmce.Arguments[1]).Value!;
                                fieldBody = fmce.Arguments[0];
                            }
                            else if (fieldBody is MethodCallExpression colMce && colMce.Method.Name == "Col")
                            {
                                alias = (string)((ConstantExpression)colMce.Arguments[0]).Value!;
                            }
                            if (alias == null) throw new InvalidOperationException("Struct.WithFields requires each field expression to have an alias.");
                            var fieldSeries = EvaluateExpression(fieldBody, df, disposables);
                            fieldSeries.Rename(alias);
                            if (fieldSeries is IDisposable d && disposables.Contains(d))
                                disposables.Remove(d);
                            newFields.Add(fieldSeries);
                        }
                        var result = Compute.StructKernels.WithFields(structSeries, newFields.ToArray());
                        if (result is IDisposable rd && !disposables.Contains(rd))
                            disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("Struct.WithFields requires StructSeries.");
                }
            return null;
        }
    }
}
