namespace EngineeredWood.Iceberg.Expressions;

/// <summary>
/// Factory methods for building expressions.
/// The convenience overloads (column name + literal value) produce
/// UnboundReference + LiteralExpression wrapped in the appropriate predicate.
/// </summary>
public static class Expressions
{
    // --- References and literals ---

    public static Expression Ref(string name) => new UnboundReference(name);
    public static Expression Ref(int fieldId, string name, IcebergType type) => new BoundReference(fieldId, name, type);
    public static Expression Lit(LiteralValue value) => new LiteralExpression(value);
    public static Expression Lit(long value) => new LiteralExpression(LiteralValue.Of(value));
    public static Expression Lit(int value) => new LiteralExpression(LiteralValue.Of(value));
    public static Expression Lit(double value) => new LiteralExpression(LiteralValue.Of(value));
    public static Expression Lit(string value) => new LiteralExpression(LiteralValue.Of(value));

    // --- Function calls ---

    public static Expression Apply(string funcName, params Expression[] arguments) =>
        new ApplyExpression(new FunctionIdentifier(funcName), arguments);

    public static Expression Apply(FunctionIdentifier func, params Expression[] arguments) =>
        new ApplyExpression(func, arguments);

    // --- Boolean connectives ---

    public static Expression AlwaysTrue() => Expression.True;
    public static Expression AlwaysFalse() => Expression.False;

    public static Expression And(Expression left, Expression right)
    {
        if (left is FalseExpression || right is FalseExpression) return Expression.False;
        if (left is TrueExpression) return right;
        if (right is TrueExpression) return left;
        return new AndExpression(left, right);
    }

    public static Expression Or(Expression left, Expression right)
    {
        if (left is TrueExpression || right is TrueExpression) return Expression.True;
        if (left is FalseExpression) return right;
        if (right is FalseExpression) return left;
        return new OrExpression(left, right);
    }

    public static Expression Not(Expression child) => child switch
    {
        TrueExpression => Expression.False,
        FalseExpression => Expression.True,
        NotExpression n => n.Child,
        _ => new NotExpression(child),
    };

    // --- Comparison predicates (column name + literal convenience) ---

    public static Expression Equal(string column, long value) =>
        new ComparisonPredicate(ComparisonOperator.Eq, Ref(column), Lit(value));
    public static Expression Equal(string column, int value) =>
        new ComparisonPredicate(ComparisonOperator.Eq, Ref(column), Lit(value));
    public static Expression Equal(string column, string value) =>
        new ComparisonPredicate(ComparisonOperator.Eq, Ref(column), Lit(value));
    public static Expression Equal(string column, double value) =>
        new ComparisonPredicate(ComparisonOperator.Eq, Ref(column), Lit(value));
    public static Expression Equal(string column, float value) =>
        new ComparisonPredicate(ComparisonOperator.Eq, Ref(column), new LiteralExpression(LiteralValue.Of(value)));
    public static Expression Equal(string column, bool value) =>
        new ComparisonPredicate(ComparisonOperator.Eq, Ref(column), new LiteralExpression(LiteralValue.Of(value)));

    public static Expression NotEqual(string column, long value) =>
        new ComparisonPredicate(ComparisonOperator.NotEq, Ref(column), Lit(value));
    public static Expression NotEqual(string column, string value) =>
        new ComparisonPredicate(ComparisonOperator.NotEq, Ref(column), Lit(value));

    public static Expression LessThan(string column, long value) =>
        new ComparisonPredicate(ComparisonOperator.Lt, Ref(column), Lit(value));
    public static Expression LessThan(string column, int value) =>
        new ComparisonPredicate(ComparisonOperator.Lt, Ref(column), Lit(value));
    public static Expression LessThan(string column, double value) =>
        new ComparisonPredicate(ComparisonOperator.Lt, Ref(column), Lit(value));
    public static Expression LessThan(string column, string value) =>
        new ComparisonPredicate(ComparisonOperator.Lt, Ref(column), Lit(value));

    public static Expression LessThanOrEqual(string column, long value) =>
        new ComparisonPredicate(ComparisonOperator.LtEq, Ref(column), Lit(value));
    public static Expression LessThanOrEqual(string column, int value) =>
        new ComparisonPredicate(ComparisonOperator.LtEq, Ref(column), Lit(value));

    public static Expression GreaterThan(string column, long value) =>
        new ComparisonPredicate(ComparisonOperator.Gt, Ref(column), Lit(value));
    public static Expression GreaterThan(string column, int value) =>
        new ComparisonPredicate(ComparisonOperator.Gt, Ref(column), Lit(value));
    public static Expression GreaterThan(string column, double value) =>
        new ComparisonPredicate(ComparisonOperator.Gt, Ref(column), Lit(value));
    public static Expression GreaterThan(string column, string value) =>
        new ComparisonPredicate(ComparisonOperator.Gt, Ref(column), Lit(value));

    public static Expression GreaterThanOrEqual(string column, long value) =>
        new ComparisonPredicate(ComparisonOperator.GtEq, Ref(column), Lit(value));
    public static Expression GreaterThanOrEqual(string column, int value) =>
        new ComparisonPredicate(ComparisonOperator.GtEq, Ref(column), Lit(value));

    // --- Unary predicates ---

    public static Expression IsNull(string column) =>
        new UnaryPredicate(UnaryOperator.IsNull, Ref(column));
    public static Expression NotNull(string column) =>
        new UnaryPredicate(UnaryOperator.NotNull, Ref(column));
    public static Expression IsNaN(string column) =>
        new UnaryPredicate(UnaryOperator.IsNan, Ref(column));
    public static Expression NotNaN(string column) =>
        new UnaryPredicate(UnaryOperator.NotNan, Ref(column));

    // --- Set predicates ---

    public static Expression In(string column, params long[] values) =>
        new SetPredicate(SetOperator.In, Ref(column), values.Select(LiteralValue.Of).ToList());
    public static Expression In(string column, params string[] values) =>
        new SetPredicate(SetOperator.In, Ref(column), values.Select(LiteralValue.Of).ToList());
    public static Expression NotIn(string column, params long[] values) =>
        new SetPredicate(SetOperator.NotIn, Ref(column), values.Select(LiteralValue.Of).ToList());

    // --- String predicates ---

    public static Expression StartsWith(string column, string prefix) =>
        new ComparisonPredicate(ComparisonOperator.StartsWith, Ref(column), Lit(prefix));
    public static Expression NotStartsWith(string column, string prefix) =>
        new ComparisonPredicate(ComparisonOperator.NotStartsWith, Ref(column), Lit(prefix));

    // --- Generic expression-based predicates ---

    public static Expression Equal(Expression left, Expression right) =>
        new ComparisonPredicate(ComparisonOperator.Eq, left, right);
    public static Expression LessThan(Expression left, Expression right) =>
        new ComparisonPredicate(ComparisonOperator.Lt, left, right);
    public static Expression GreaterThan(Expression left, Expression right) =>
        new ComparisonPredicate(ComparisonOperator.Gt, left, right);
}
