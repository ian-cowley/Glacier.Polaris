using System;
using System.Linq.Expressions;

namespace Glacier.Polaris
{
    public sealed class Expr
    {
        public System.Linq.Expressions.Expression Expression { get; }

        internal Expr(System.Linq.Expressions.Expression expression)
        {
            Expression = expression;
        }

        public static Expr Col(string name)
        {
            var method = typeof(Functions).GetMethod(nameof(Functions.Col)) ?? typeof(Expr).GetMethod(nameof(Col));
            return new Expr(System.Linq.Expressions.Expression.Call(null, method!, System.Linq.Expressions.Expression.Constant(name)));
        }

        // Arithmetic Operators (Expr)
        public static Expr operator +(Expr left, Expr right) => BuildBinary(left, right, System.Linq.Expressions.ExpressionType.Add);
        public static Expr operator -(Expr left, Expr right) => BuildBinary(left, right, System.Linq.Expressions.ExpressionType.Subtract);
        public static Expr operator *(Expr left, Expr right) => BuildBinary(left, right, System.Linq.Expressions.ExpressionType.Multiply);
        public static Expr operator /(Expr left, Expr right) => BuildBinary(left, right, System.Linq.Expressions.ExpressionType.Divide);

        // Arithmetic Operators (Int32 Literal)
        public static Expr operator +(Expr left, int right) => BuildBinary(left, new Expr(System.Linq.Expressions.Expression.Constant(right)), System.Linq.Expressions.ExpressionType.Add);
        public static Expr operator -(Expr left, int right) => BuildBinary(left, new Expr(System.Linq.Expressions.Expression.Constant(right)), System.Linq.Expressions.ExpressionType.Subtract);
        public static Expr operator *(Expr left, int right) => BuildBinary(left, new Expr(System.Linq.Expressions.Expression.Constant(right)), System.Linq.Expressions.ExpressionType.Multiply);
        public static Expr operator /(Expr left, int right) => BuildBinary(left, new Expr(System.Linq.Expressions.Expression.Constant(right)), System.Linq.Expressions.ExpressionType.Divide);

        // Arithmetic Operators (Double Literal)
        public static Expr operator +(Expr left, double right) => BuildBinary(left, new Expr(System.Linq.Expressions.Expression.Constant(right)), System.Linq.Expressions.ExpressionType.Add);
        public static Expr operator -(Expr left, double right) => BuildBinary(left, new Expr(System.Linq.Expressions.Expression.Constant(right)), System.Linq.Expressions.ExpressionType.Subtract);
        public static Expr operator *(Expr left, double right) => BuildBinary(left, new Expr(System.Linq.Expressions.Expression.Constant(right)), System.Linq.Expressions.ExpressionType.Multiply);
        public static Expr operator /(Expr left, double right) => BuildBinary(left, new Expr(System.Linq.Expressions.Expression.Constant(right)), System.Linq.Expressions.ExpressionType.Divide);

        // Boolean Operators
        public static Expr operator &(Expr left, Expr right) => BuildBinary(left, right, System.Linq.Expressions.ExpressionType.And);
        public static Expr operator |(Expr left, Expr right) => BuildBinary(left, right, System.Linq.Expressions.ExpressionType.Or);
        public static bool operator true(Expr e) => throw new InvalidOperationException("Expr used in conditional context; use & instead of &&.");
        public static bool operator false(Expr e) => throw new InvalidOperationException("Expr used in conditional context; use & instead of &&.");

        // Comparison Operators
        public static Expr operator >(Expr left, Expr right) => BuildBinary(left, right, System.Linq.Expressions.ExpressionType.GreaterThan);

        public static Expr operator <(Expr left, Expr right) => BuildBinary(left, right, System.Linq.Expressions.ExpressionType.LessThan);
        public static Expr operator >=(Expr left, Expr right) => BuildBinary(left, right, System.Linq.Expressions.ExpressionType.GreaterThanOrEqual);
        public static Expr operator <=(Expr left, Expr right) => BuildBinary(left, right, System.Linq.Expressions.ExpressionType.LessThanOrEqual);
        public static Expr operator ==(Expr left, Expr right) => BuildBinary(left, right, System.Linq.Expressions.ExpressionType.Equal);
        public static Expr operator !=(Expr left, Expr right) => BuildBinary(left, right, System.Linq.Expressions.ExpressionType.NotEqual);

        // Literal conversions for comparisons (Int32)
        public static Expr operator >(Expr left, int right) => BuildBinary(left, new Expr(System.Linq.Expressions.Expression.Constant(right)), System.Linq.Expressions.ExpressionType.GreaterThan);
        public static Expr operator <(Expr left, int right) => BuildBinary(left, new Expr(System.Linq.Expressions.Expression.Constant(right)), System.Linq.Expressions.ExpressionType.LessThan);
        public static Expr operator >=(Expr left, int right) => BuildBinary(left, new Expr(System.Linq.Expressions.Expression.Constant(right)), System.Linq.Expressions.ExpressionType.GreaterThanOrEqual);
        public static Expr operator <=(Expr left, int right) => BuildBinary(left, new Expr(System.Linq.Expressions.Expression.Constant(right)), System.Linq.Expressions.ExpressionType.LessThanOrEqual);
        public static Expr operator ==(Expr left, int right) => BuildBinary(left, new Expr(System.Linq.Expressions.Expression.Constant(right)), System.Linq.Expressions.ExpressionType.Equal);
        public static Expr operator !=(Expr left, int right) => BuildBinary(left, new Expr(System.Linq.Expressions.Expression.Constant(right)), System.Linq.Expressions.ExpressionType.NotEqual);

        // Literal conversions for comparisons (Double)
        public static Expr operator >(Expr left, double right) => BuildBinary(left, new Expr(System.Linq.Expressions.Expression.Constant(right)), System.Linq.Expressions.ExpressionType.GreaterThan);
        public static Expr operator <(Expr left, double right) => BuildBinary(left, new Expr(System.Linq.Expressions.Expression.Constant(right)), System.Linq.Expressions.ExpressionType.LessThan);
        public static Expr operator >=(Expr left, double right) => BuildBinary(left, new Expr(System.Linq.Expressions.Expression.Constant(right)), System.Linq.Expressions.ExpressionType.GreaterThanOrEqual);
        public static Expr operator <=(Expr left, double right) => BuildBinary(left, new Expr(System.Linq.Expressions.Expression.Constant(right)), System.Linq.Expressions.ExpressionType.LessThanOrEqual);
        public static Expr operator ==(Expr left, double right) => BuildBinary(left, new Expr(System.Linq.Expressions.Expression.Constant(right)), System.Linq.Expressions.ExpressionType.Equal);
        public static Expr operator !=(Expr left, double right) => BuildBinary(left, new Expr(System.Linq.Expressions.Expression.Constant(right)), System.Linq.Expressions.ExpressionType.NotEqual);

        private static Expr BuildBinary(Expr left, Expr right, System.Linq.Expressions.ExpressionType type)
        {
            var binary = System.Linq.Expressions.Expression.MakeBinary(type, left.Expression, right.Expression);
            return new Expr(binary);
        }

        public static Expr Lit(object value)
        {
            var method = typeof(Expr).GetMethod(nameof(LitOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, System.Linq.Expressions.Expression.Constant(value, typeof(object))));
        }

        public static WhenExpr When(Expr condition) => new WhenExpr(condition);

        public Expr Sum() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(SumOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr Mean() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(MeanOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr Min() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(MinOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr Max() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(MaxOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr Std() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(StdOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr Var() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(VarOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr Median() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(MedianOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr Count() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(CountOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr NUnique() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(NUniqueOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr Quantile(double quantile) => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(QuantileOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression, System.Linq.Expressions.Expression.Constant(quantile)));
        public Expr RollingMean(int window) => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(RollingMeanOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression, System.Linq.Expressions.Expression.Constant(window)));
        public Expr RollingSum(int window) => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(RollingSumOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression, System.Linq.Expressions.Expression.Constant(window)));
        public Expr RollingMin(int window) => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(RollingMinOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression, System.Linq.Expressions.Expression.Constant(window)));
        public Expr RollingMax(int window) => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(RollingMaxOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression, System.Linq.Expressions.Expression.Constant(window)));
        public Expr RollingStd(int window) => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(RollingStdOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression, System.Linq.Expressions.Expression.Constant(window)));

        public Expr EWMMean(double alpha) => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(EWMMeanOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression, System.Linq.Expressions.Expression.Constant(alpha)));

        public Expr ExpandingMean() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(ExpandingMeanOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr ExpandingSum() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(ExpandingSumOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr ExpandingMin() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(ExpandingMinOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr ExpandingMax() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(ExpandingMaxOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr ExpandingStd() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(ExpandingStdOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public ListNamespace List() => new ListNamespace(this);
        public StructNamespace Struct() => new StructNamespace(this);

        public Expr Unique() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(UniqueOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr Duration() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(DurationOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr Contains(object value) => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(ContainsOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression, System.Linq.Expressions.Expression.Constant(value)));
        public Expr Join(string separator) => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(JoinStringOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression, System.Linq.Expressions.Expression.Constant(separator)));

        public Expr TotalDays() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(TotalDaysOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr TotalHours() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(TotalHoursOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr TotalSeconds() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(TotalSecondsOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));

        public Expr Shift(int n) => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(ShiftOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression, System.Linq.Expressions.Expression.Constant(n)));

        public Expr Diff(int n) => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(DiffOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression, System.Linq.Expressions.Expression.Constant(n)));

        public Expr Abs() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(AbsOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));

        public Expr Clip(double min, double max) => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(ClipOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression, System.Linq.Expressions.Expression.Constant(min), System.Linq.Expressions.Expression.Constant(max)));

        public Expr DropNulls() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(DropNullsOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));

        public Expr Over(params string[] columns)
        {
            return new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(OverOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression, System.Linq.Expressions.Expression.Constant(columns)));
        }


        public Expr FillNull(FillStrategy strategy)
        {
            var method = typeof(Expr).GetMethod(nameof(FillNullOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, this.Expression, System.Linq.Expressions.Expression.Constant(strategy)));
        }


        public Expr FillNull(object value)
        {
            var method = typeof(Expr).GetMethod(nameof(FillNullLiteralOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            // Must box value types to object via Expression.Convert to match the method's object parameter
            System.Linq.Expressions.Expression valueExpr = System.Linq.Expressions.Expression.Constant(value);
            if (value != null && value.GetType().IsValueType)
                valueExpr = System.Linq.Expressions.Expression.Convert(valueExpr, typeof(object));
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, this.Expression, valueExpr));
        }

        public Expr FillNull(Expr value)
        {
            var method = typeof(Expr).GetMethod("FillNullExprOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static) ?? typeof(Expr).GetMethod("FillNullOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, this.Expression, value.Expression));
        }

        public static Expr operator -(Expr e) => new Expr(System.Linq.Expressions.Expression.Negate(e.Expression));
        public StringNamespace Str() => new StringNamespace(this);
        public BinaryNamespace Bin() => new BinaryNamespace(this);
        public TemporalNamespace Dt() => new TemporalNamespace(this);

        public Expr Alias(string name)
        {
            return new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(AliasOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression, System.Linq.Expressions.Expression.Constant(name)));
        }

        public Expr IsNull()
        {
            return new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(IsNullOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        }

        public Expr IsNotNull()
        {
            return new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(IsNotNullOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        }

        public Expr Cast(Type targetType)
        {
            return new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(CastOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression, System.Linq.Expressions.Expression.Constant(targetType)));
        }

        public static Expr RegexMatch(string name, string pattern)
        {
            return new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(RegexMatchOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, System.Linq.Expressions.Expression.Constant(name), System.Linq.Expressions.Expression.Constant(pattern)));
        }

        internal static Expr SumOp(Expr e) => null!;
        internal static Expr MeanOp(Expr e) => null!;
        internal static Expr MinOp(Expr e) => null!;
        internal static Expr MaxOp(Expr e) => null!;
        internal static Expr StdOp(Expr e) => null!;
        internal static Expr VarOp(Expr e) => null!;
        internal static Expr MedianOp(Expr e) => null!;
        internal static Expr CountOp(Expr e) => null!;
        internal static Expr NUniqueOp(Expr e) => null!;
        internal static Expr QuantileOp(Expr e, double quantile) => null!;
        internal static Expr OverOp(Expr e, string[] columns) => null!;
        internal static Expr RollingMeanOp(Expr e, int window) => null!;
        internal static Expr RollingSumOp(Expr e, int window) => null!;
        public Expr EWMStd(double alpha) => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(EWMStdOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression, System.Linq.Expressions.Expression.Constant(alpha)));
        public Expr First() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(FirstOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr Last() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(LastOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr IsDuplicated() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(IsDuplicatedOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr IsUnique() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(IsUniqueOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression)); internal static Expr RollingMinOp(Expr e, int window) => null!;
        internal static Expr RollingMaxOp(Expr e, int window) => null!;
        internal static Expr EWMMeanOp(Expr e, double alpha) => null!;
        internal static Expr ExpandingMeanOp(Expr e) => null!;
        internal static Expr ExpandingSumOp(Expr e) => null!;
        internal static Expr ExpandingMinOp(Expr e) => null!;
        internal static Expr ExpandingMaxOp(Expr e) => null!;
        internal static Expr ExpandingStdOp(Expr e) => null!;
        internal static Expr ListOp(Expr e) => null!;
        internal static Expr UniqueOp(Expr e) => null!;
        internal static Expr DurationOp(Expr e) => null!;
        internal static Expr Temporal_SubtractDurationOp(Expr e, Expr duration) => null!;
        internal static Expr Temporal_SubtractOp(Expr e, Expr other) => null!;
        internal static Expr AliasOp(Expr e, string name) => null!;
        internal static Expr Bin_EncodeOp(Expr e, string encoding) => null!;
        internal static Expr Bin_DecodeOp(Expr e, string encoding) => null!;
        internal static Expr ContainsOp(Expr e, object value) => null!;
        internal static Expr JoinStringOp(Expr e, string separator) => null!;
        internal static Expr TotalDaysOp(Expr e) => null!;
        internal static Expr TotalHoursOp(Expr e) => null!;
        internal static Expr TotalSecondsOp(Expr e) => null!;
        internal static Expr RollingStdOp(Expr e, int window) => null!;
        internal static Expr List_GetOp(Expr e, int index) => null!;
        internal static Expr List_SumOp(Expr e) => null!;
        internal static Expr List_MeanOp(Expr e) => null!;
        internal static Expr List_MinOp(Expr e) => null!;
        internal static Expr List_MaxOp(Expr e) => null!;
        internal static Expr List_UniqueOp(Expr e) => null!;
        internal static Expr List_ContainsOp(Expr e, object value) => null!;
        internal static Expr List_JoinOp(Expr e, string separator) => null!;
        internal static Expr List_LengthsOp(Expr e) => null!;
        internal static Expr Struct_FieldOp(Expr e, string name) => null!;
        internal static Expr Bin_StartsWithOp(Expr e, byte[] prefix) => null!;
        internal static Expr Bin_EndsWithOp(Expr e, byte[] suffix) => null!;
        internal static Expr RegexMatchOp(string name, string pattern) => null!;
        internal static Expr Str_ContainsOp(Expr e, string pattern) => null!;
        internal static Expr Str_LengthsOp(Expr e) => null!;
        internal static Expr Str_StartsWithOp(Expr e, string prefix) => null!;
        internal static Expr Str_EndsWithOp(Expr e, string suffix) => null!;
        internal static Expr Str_ToUpperOp(Expr e) => null!;
        internal static Expr Str_ToLowerOp(Expr e) => null!;
        internal static Expr WhenThenOtherwiseOp(Expr condition, Expr thenResult, Expr otherwiseResult) => null!;
        internal static Expr LitOp(object value) => null!;
        public Expr Year() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(YearOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr Month() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(MonthOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr Day() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(DayOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));

        internal static Expr IsNullOp(Expr e) => null!;
        internal static Expr IsNotNullOp(Expr e) => null!;
        internal static Expr YearOp(Expr e) => null!;
        internal static Expr MonthOp(Expr e) => null!;
        internal static Expr DayOp(Expr e) => null!;
        internal static Expr HourOp(Expr e) => null!;
        internal static Expr MinuteOp(Expr e) => null!;
        internal static Expr SecondOp(Expr e) => null!;
        internal static Expr NanosecondOp(Expr e) => null!;
        internal static Expr Bin_ContainsOp(Expr e, byte[] pattern) => null!;
        internal static Expr Bin_LengthsOp(Expr e) => null!;
        internal static Expr FillNullOp(Expr e, FillStrategy strategy) => null!;
        internal static Expr FillNullLiteralOp(Expr e, object value) => null!;
        internal static Expr CastOp(Expr source, Type targetType) => null!;
        internal static Expr ShiftOp(Expr e, int n) => null!;
        internal static Expr DiffOp(Expr e, int n) => null!;
        internal static Expr AbsOp(Expr e) => null!;
        internal static Expr ClipOp(Expr e, double min, double max) => null!;
        internal static Expr DropNullsOp(Expr e) => null!;

        public override bool Equals(object? obj) => ReferenceEquals(this, obj);
        public override int GetHashCode() => Expression.GetHashCode();
        internal static Expr EWMStdOp(Expr e, double alpha) => null!;
        internal static Expr FirstOp(Expr e) => null!;
        internal static Expr LastOp(Expr e) => null!;
        internal static Expr IsDuplicatedOp(Expr e) => null!;
        internal static Expr IsUniqueOp(Expr e) => null!;
        internal static Expr Str_ReplaceOp(Expr e, string oldValue, string newValue) => null!;
        internal static Expr Str_ReplaceAllOp(Expr e, string oldValue, string newValue) => null!;
        internal static Expr Str_StripOp(Expr e) => null!;
        internal static Expr Str_LStripOp(Expr e) => null!;
        internal static Expr Str_RStripOp(Expr e) => null!;
        internal static Expr Str_SplitOp(Expr e, string separator) => null!;
        internal static Expr Str_SliceOp(Expr e, int start, int? length) => null!;
        internal static Expr Str_ToDateOp(Expr e, string? format) => null!;
        internal static Expr Str_ToDatetimeOp(Expr e, string? format) => null!;
        internal static Expr WeekdayOp(Expr e) => null!;
        internal static Expr QuarterOp(Expr e) => null!;
        internal static Expr Dt_WeekdayOp(Expr e) => null!;
        internal static Expr Dt_QuarterOp(Expr e) => null!;
        internal static Expr Dt_OffsetByOp(Expr e, string duration) => null!;
        internal static Expr Dt_RoundOp(Expr e, string every) => null!;
        internal static Expr Dt_EpochOp(Expr e, string unit) => null!;
        internal static Expr SqrtOp(Expr e) => null!;
        internal static Expr LogOp(Expr e) => null!;
        internal static Expr Log10Op(Expr e) => null!;
        internal static Expr ExpOp(Expr e) => null!;
        internal static Expr SinOp(Expr e) => null!;
        internal static Expr CosOp(Expr e) => null!;
        internal static Expr TanOp(Expr e) => null!;
        internal static Expr RankOp(Expr e, bool descending) => null!;
        internal static Expr PctChangeOp(Expr e, int n) => null!; public Expr Sqrt() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(SqrtOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr Log() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(LogOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr Log10() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(Log10Op), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr Exp() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(ExpOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr Sin() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(SinOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr Cos() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(CosOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr Tan() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(TanOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr Rank(bool descending = false) => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(RankOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression, System.Linq.Expressions.Expression.Constant(descending)));
        public Expr PctChange(int n = 1) => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(PctChangeOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression, System.Linq.Expressions.Expression.Constant(n))); public Expr Floor() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(FloorOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr Ceil() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(CeilOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr Round(int decimals = 0) => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(RoundOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression, System.Linq.Expressions.Expression.Constant(decimals)));
        public Expr NullCount() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(NullCountOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression)); internal static Expr FloorOp(Expr e) => null!;
        internal static Expr CeilOp(Expr e) => null!;
        internal static Expr RoundOp(Expr e, int decimals) => null!;
        internal static Expr NullCountOp(Expr e) => null!;
        internal static Expr ArgMinOp(Expr e) => null!;
        internal static Expr ArgMaxOp(Expr e) => null!; public Expr CumCount(bool reverse = false) => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(CumCountOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression, System.Linq.Expressions.Expression.Constant(reverse)));

        public Expr CumProd(bool reverse = false) => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(CumProdOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression, System.Linq.Expressions.Expression.Constant(reverse))); internal static Expr CumCountOp(Expr e, bool reverse) => null!;
        internal static Expr CumProdOp(Expr e, bool reverse) => null!;

        internal static Expr Struct_RenameFieldsOp(Expr e, string[] newNames) => null!;
        internal static Expr Struct_JsonEncodeOp(Expr e) => null!;
        internal static Expr Struct_WithFieldsOp(Expr e, Expr[] fieldExprs) => null!; internal static Expr List_SortOp(Expr e, bool descending) => null!;
        internal static Expr List_ReverseOp(Expr e) => null!;
        internal static Expr Str_HeadOp(Expr e, int n) => null!;
        internal static Expr Str_TailOp(Expr e, int n) => null!;
        internal static Expr Str_PadStartOp(Expr e, int width, char fillChar) => null!;
        internal static Expr Str_PadEndOp(Expr e, int width, char fillChar) => null!;
        internal static Expr Str_ToTitlecaseOp(Expr e) => null!;
        internal static Expr Str_ExtractOp(Expr e, string pattern) => null!;
        internal static Expr Str_ReverseOp(Expr e) => null!; internal static Expr Dt_TruncateOp(Expr e, string every) => null!;
        internal static Expr Dt_OrdinalDayOp(Expr e) => null!;
        internal static Expr Dt_TimestampOp(Expr e, string unit) => null!;
        internal static Expr Dt_WithTimeUnitOp(Expr e, string unit) => null!;
        internal static Expr Dt_CastTimeUnitOp(Expr e, string unit) => null!;
        internal static Expr Dt_MonthStartOp(Expr e) => null!;
        internal static Expr Dt_MonthEndOp(Expr e) => null!;
        internal static Expr Dt_ConvertTimeZoneOp(Expr e, string targetTimeZoneId, string sourceTimeZoneId) => null!;
        /// <summary>Represents a reference to the element within a list.eval() context. Equivalent to Python Polars' pl.element().</summary>
        public static Expr Element()
        {
            var method = typeof(Expr).GetMethod(nameof(ElementOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method));
        }
        internal static Expr ElementOp() => null!; internal static Expr List_EvalOp(Expr e, Expr elementExpr) => null!;
        public Expr GatherEvery(int n, int offset = 0)
        {
            var method = typeof(Expr).GetMethod(nameof(GatherEveryOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, this.Expression, System.Linq.Expressions.Expression.Constant(n), System.Linq.Expressions.Expression.Constant(offset)));
        }

        public Expr SearchSorted(Expr element)
        {
            var method = typeof(Expr).GetMethod(nameof(SearchSortedOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, this.Expression, element.Expression));
        }

        public Expr Slice(int offset, int? length = null)
        {
            var method = typeof(Expr).GetMethod(nameof(SliceOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, this.Expression, System.Linq.Expressions.Expression.Constant(offset), System.Linq.Expressions.Expression.Constant(length, typeof(int?))));
        }

        public Expr TopK(int k)
        {
            var method = typeof(Expr).GetMethod(nameof(TopKOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, this.Expression, System.Linq.Expressions.Expression.Constant(k)));
        }

        public Expr BottomK(int k)
        {
            var method = typeof(Expr).GetMethod(nameof(BottomKOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, this.Expression, System.Linq.Expressions.Expression.Constant(k)));
        }
        internal static Expr Str_ExtractAllOp(Expr e, string pattern) => null!;
        internal static Expr Str_JsonDecodeOp(Expr e) => null!;
        internal static Expr Str_JsonEncodeOp(Expr e) => null!;
        internal static Expr GatherEveryOp(Expr e, int n, int offset) => null!;
        internal static Expr SearchSortedOp(Expr e, Expr element) => null!;
        internal static Expr SliceOp(Expr e, int offset, int? length) => null!;
        internal static Expr TopKOp(Expr e, int k) => null!;
        internal static Expr BottomKOp(Expr e, int k) => null!; public Expr ArgMin() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(ArgMinOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr ArgMax() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(ArgMaxOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        internal static Expr List_ArgMinOp(Expr e) => null!;
        internal static Expr List_ArgMaxOp(Expr e) => null!;
        internal static Expr List_DiffOp(Expr e, int n) => null!;
        internal static Expr List_ShiftOp(Expr e, int n) => null!;
        internal static Expr List_SliceOp(Expr e, int offset, int? length) => null!;
        public Expr ValueCounts(bool sort = false, bool parallel = true) => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(ValueCountsOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression, System.Linq.Expressions.Expression.Constant(sort), System.Linq.Expressions.Expression.Constant(parallel)));
        public Expr IsFirst() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(IsFirstOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr Entropy() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(EntropyOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr ApproxNUnique() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(ApproxNUniqueOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        public Expr Hash() => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(HashOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression));
        internal static Expr ValueCountsOp(Expr e, bool sort, bool parallel) => null!;
        internal static Expr IsFirstOp(Expr e) => null!;
        internal static Expr EntropyOp(Expr e) => null!;
        internal static Expr ApproxNUniqueOp(Expr e) => null!;
        internal static Expr HashOp(Expr e) => null!; public Expr MapElements(Func<object?, object?> mapping, Type returnType) => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(MapElementsOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression, System.Linq.Expressions.Expression.Constant(mapping), System.Linq.Expressions.Expression.Constant(returnType)));
        public Expr Reinterpret(Type targetType) => new Expr(System.Linq.Expressions.Expression.Call(null, typeof(Expr).GetMethod(nameof(ReinterpretOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!, this.Expression, System.Linq.Expressions.Expression.Constant(targetType)));
        internal static Expr MapElementsOp(Expr e, Func<object?, object?> mapping, Type returnType) => null!;
        internal static Expr ReinterpretOp(Expr e, Type targetType) => null!;
    }

    public sealed class WhenExpr
    {
        private readonly Expr _condition;
        internal WhenExpr(Expr condition) => _condition = condition;

        public ThenExpr Then(Expr result) => new ThenExpr(_condition, result);
        public ThenExpr Then(object literal) => new ThenExpr(_condition, Expr.Lit(literal));
    }

    public sealed class ThenExpr
    {
        private readonly Expr _condition;
        private readonly Expr _thenResult;
        internal ThenExpr(Expr condition, Expr result)
        {
            _condition = condition;
            _thenResult = result;
        }

        public Expr Otherwise(Expr result)
        {
            var method = typeof(Expr).GetMethod("WhenThenOtherwiseOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _condition.Expression, _thenResult.Expression, result.Expression));
        }

        public Expr Otherwise(object literal) => Otherwise(Expr.Lit(literal));
    }

    public sealed class StringNamespace
    {
        private readonly Expr _expr;
        internal StringNamespace(Expr expr) => _expr = expr;

        public Expr Contains(string pattern)
        {
            var method = typeof(Expr).GetMethod("Str_ContainsOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(pattern)));
        }

        public Expr Lengths()
        {
            var method = typeof(Expr).GetMethod("Str_LengthsOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }

        public Expr StartsWith(string prefix)
        {
            var method = typeof(Expr).GetMethod("Str_StartsWithOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(prefix)));
        }

        public Expr EndsWith(string suffix)
        {
            var method = typeof(Expr).GetMethod("Str_EndsWithOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(suffix)));
        }

        public Expr ToUppercase()
        {
            var method = typeof(Expr).GetMethod("Str_ToUpperOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }

        public Expr ToLowercase()
        {
            var method = typeof(Expr).GetMethod("Str_ToLowerOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }
        public Expr Replace(string oldValue, string newValue)
        {
            var method = typeof(Expr).GetMethod("Str_ReplaceOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(oldValue), System.Linq.Expressions.Expression.Constant(newValue)));
        }

        public Expr ReplaceAll(string oldValue, string newValue)
        {
            var method = typeof(Expr).GetMethod("Str_ReplaceAllOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(oldValue), System.Linq.Expressions.Expression.Constant(newValue)));
        }

        public Expr Strip()
        {
            var method = typeof(Expr).GetMethod("Str_StripOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }

        public Expr LStrip()
        {
            var method = typeof(Expr).GetMethod("Str_LStripOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }

        public Expr RStrip()
        {
            var method = typeof(Expr).GetMethod("Str_RStripOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }

        public Expr Split(string separator)
        {
            var method = typeof(Expr).GetMethod("Str_SplitOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(separator)));
        }

        public Expr Slice(int start, int? length = null)
        {
            var method = typeof(Expr).GetMethod("Str_SliceOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(start), System.Linq.Expressions.Expression.Constant(length, typeof(int?))));
        }
        public Expr ToDate(string? format = null)
        {
            var method = typeof(Expr).GetMethod("Str_ToDateOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(format, typeof(string))));
        }
        public Expr ToDatetime(string? format = null)
        {
            var method = typeof(Expr).GetMethod("Str_ToDatetimeOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(format, typeof(string))));
        }
        public Expr Head(int n)
        {
            var method = typeof(Expr).GetMethod("Str_HeadOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(n)));
        }

        public Expr Tail(int n)
        {
            var method = typeof(Expr).GetMethod("Str_TailOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(n)));
        }

        public Expr PadStart(int width, char fillChar = ' ')
        {
            var method = typeof(Expr).GetMethod("Str_PadStartOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(width), System.Linq.Expressions.Expression.Constant(fillChar)));
        }

        public Expr PadEnd(int width, char fillChar = ' ')
        {
            var method = typeof(Expr).GetMethod("Str_PadEndOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(width), System.Linq.Expressions.Expression.Constant(fillChar)));
        }

        public Expr ToTitlecase()
        {
            var method = typeof(Expr).GetMethod("Str_ToTitlecaseOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }

        public Expr Extract(string pattern)
        {
            var method = typeof(Expr).GetMethod("Str_ExtractOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(pattern)));
        }

        public Expr Reverse()
        {
            var method = typeof(Expr).GetMethod("Str_ReverseOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }
        public Expr ExtractAll(string pattern)
        {
            var method = typeof(Expr).GetMethod("Str_ExtractAllOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(pattern)));
        }

        public Expr JsonDecode()
        {
            var method = typeof(Expr).GetMethod("Str_JsonDecodeOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }

        public Expr JsonEncode()
        {
            var method = typeof(Expr).GetMethod("Str_JsonEncodeOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }
    }

    public sealed class BinaryNamespace
    {
        private readonly Expr _expr;
        internal BinaryNamespace(Expr expr) => _expr = expr;

        public Expr Contains(byte[] pattern)
        {
            var method = typeof(Expr).GetMethod("Bin_ContainsOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(pattern)));
        }

        public Expr Lengths()
        {
            var method = typeof(Expr).GetMethod("Bin_LengthsOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }

        public Expr Length() => Lengths();

        public Expr Encode(string encoding)
        {
            var method = typeof(Expr).GetMethod("Bin_EncodeOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(encoding)));
        }

        public Expr Decode(string encoding)
        {
            var method = typeof(Expr).GetMethod("Bin_DecodeOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(encoding)));
        }

        public Expr StartsWith(byte[] prefix)
        {
            var method = typeof(Expr).GetMethod("Bin_StartsWithOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(prefix)));
        }

        public Expr EndsWith(byte[] suffix)
        {
            var method = typeof(Expr).GetMethod("Bin_EndsWithOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(suffix)));
        }
    }

    public sealed class TemporalNamespace
    {
        private readonly Expr _expr;
        internal TemporalNamespace(Expr expr) => _expr = expr;

        public Expr SubtractDuration(Expr duration)
        {
            var method = typeof(Expr).GetMethod("Temporal_SubtractDurationOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, duration.Expression));
        }

        public Expr Subtract(Expr other)
        {
            var method = typeof(Expr).GetMethod("Temporal_SubtractOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, other.Expression));
        }

        public Expr Year()
        {
            var method = typeof(Expr).GetMethod("YearOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }

        public Expr Month()
        {
            var method = typeof(Expr).GetMethod("MonthOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }

        public Expr Day()
        {
            var method = typeof(Expr).GetMethod("DayOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }

        public Expr Hour()
        {
            var method = typeof(Expr).GetMethod("HourOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }

        public Expr Minute()
        {
            var method = typeof(Expr).GetMethod("MinuteOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }

        public Expr Second()
        {
            var method = typeof(Expr).GetMethod("SecondOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }

        public Expr Nanosecond()
        {
            var method = typeof(Expr).GetMethod("NanosecondOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }
        public Expr Weekday()
        {
            var method = typeof(Expr).GetMethod("WeekdayOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static) ?? typeof(Expr).GetMethod("Dt_WeekdayOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }

        public Expr Quarter()
        {
            var method = typeof(Expr).GetMethod("QuarterOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static) ?? typeof(Expr).GetMethod("Dt_QuarterOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }

        public Expr OffsetBy(string duration)
        {
            var method = typeof(Expr).GetMethod("Dt_OffsetByOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(duration)));
        }

        public Expr Round(string every)
        {
            var method = typeof(Expr).GetMethod("Dt_RoundOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(every)));
        }

        public Expr Epoch(string unit = "s")
        {
            var method = typeof(Expr).GetMethod("Dt_EpochOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(unit)));
        }

        public Expr Truncate(string every)
        {
            var method = typeof(Expr).GetMethod("Dt_TruncateOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(every)));
        }

        public Expr OrdinalDay()
        {
            var method = typeof(Expr).GetMethod("Dt_OrdinalDayOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }

        public Expr Timestamp(string unit = "ns")
        {
            var method = typeof(Expr).GetMethod("Dt_TimestampOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(unit)));
        }

        public Expr WithTimeUnit(string unit)
        {
            var method = typeof(Expr).GetMethod("Dt_WithTimeUnitOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(unit)));
        }

        public Expr CastTimeUnit(string unit)
        {
            var method = typeof(Expr).GetMethod("Dt_CastTimeUnitOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(unit)));
        }

        public Expr MonthStart()
        {
            var method = typeof(Expr).GetMethod("Dt_MonthStartOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }

        public Expr MonthEnd()
        {
            var method = typeof(Expr).GetMethod("Dt_MonthEndOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }
    }

    public static class Aggregations
    {
        public static Expr Sum(this Expr e) => e.Sum();
        public static Expr Mean(this Expr e) => e.Mean();
        public static Expr Count(this Expr e) => e.Count();
        public static Expr NullCount(this Expr e) => e.NullCount();
        public static Expr ArgMin(this Expr e) => e.ArgMin();
        public static Expr ArgMax(this Expr e) => e.ArgMax();
    }
    public static class Functions
    {
        public static Expr Col(string name) => Expr.Col(name);
        public static Expr RollingMean(string name, int window) => Expr.Col(name).RollingMean(window);
        public static Expr RegexMatch(string name, string pattern) => Expr.RegexMatch(name, pattern);
    }

    public sealed class ListNamespace
    {
        private readonly Expr _expr;
        internal ListNamespace(Expr expr) => _expr = expr;

        public Expr Get(int index)
        {
            var method = typeof(Expr).GetMethod("List_GetOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(index)));
        }

        public Expr Sum()
        {
            var method = typeof(Expr).GetMethod("List_SumOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }

        public Expr Mean()
        {
            var method = typeof(Expr).GetMethod("List_MeanOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }

        public Expr Min()
        {
            var method = typeof(Expr).GetMethod("List_MinOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }

        public Expr Max()
        {
            var method = typeof(Expr).GetMethod("List_MaxOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }

        public Expr Unique()
        {
            var method = typeof(Expr).GetMethod("List_UniqueOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }

        public Expr Contains(object value)
        {
            var method = typeof(Expr).GetMethod("List_ContainsOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            // Box value types to match the 'object' parameter type
            var constExpr = System.Linq.Expressions.Expression.Constant(value, typeof(object));
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, constExpr));
        }

        public Expr Join(string separator)
        {
            var method = typeof(Expr).GetMethod("List_JoinOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(separator)));
        }

        public Expr Lengths()
        {
            var method = typeof(Expr).GetMethod("List_LengthsOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }
        public Expr Sort(bool descending = false)
        {
            var method = typeof(Expr).GetMethod("List_SortOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(descending)));
        }

        public Expr Reverse()
        {
            var method = typeof(Expr).GetMethod("List_ReverseOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }
        /// <summary>Apply an expression element-wise to each list. Equivalent to Python Polars' list.eval().</summary>
        public Expr Eval(Expr elementExpr)
        {
            var method = typeof(Expr).GetMethod("List_EvalOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, elementExpr.Expression));
        }
        public Expr ArgMin()
        {
            var method = typeof(Expr).GetMethod("List_ArgMinOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }

        public Expr ArgMax()
        {
            var method = typeof(Expr).GetMethod("List_ArgMaxOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }

        public Expr Diff(int n = 1)
        {
            var method = typeof(Expr).GetMethod("List_DiffOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(n)));
        }

        public Expr Shift(int n)
        {
            var method = typeof(Expr).GetMethod("List_ShiftOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(n)));
        }

        public Expr Slice(int offset, int? length = null)
        {
            var method = typeof(Expr).GetMethod("List_SliceOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(offset), System.Linq.Expressions.Expression.Constant(length, typeof(int?))));
        }
    }

    public sealed class StructNamespace
    {
        private readonly Expr _expr;
        internal StructNamespace(Expr expr) => _expr = expr;

        public Expr Field(string name)
        {
            var method = typeof(Expr).GetMethod("Struct_FieldOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(name)));
        }
        public Expr RenameFields(string[] newNames)
        {
            var method = typeof(Expr).GetMethod("Struct_RenameFieldsOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, System.Linq.Expressions.Expression.Constant(newNames)));
        }

        public Expr JsonEncode()
        {
            var method = typeof(Expr).GetMethod("Struct_JsonEncodeOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression));
        }

        public Expr WithFields(params Expr[] fieldExprs)
        {
            var method = typeof(Expr).GetMethod("Struct_WithFieldsOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var exprsArray = System.Linq.Expressions.Expression.NewArrayInit(typeof(Expr), fieldExprs.Select(e => System.Linq.Expressions.Expression.Constant(e)));
            return new Expr(System.Linq.Expressions.Expression.Call(null, method, _expr.Expression, exprsArray));
        }
    }
}
