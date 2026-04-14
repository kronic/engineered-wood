namespace EngineeredWood.Iceberg.Expressions;

/// <summary>
/// Binds unbound references (column names) to field IDs using a schema.
/// Recursively walks the expression tree.
/// </summary>
internal static class ExpressionBinder
{
    public static Expression Bind(Expression expr, Schema schema)
    {
        return expr switch
        {
            TrueExpression or FalseExpression => expr,
            AndExpression a => Expressions.And(Bind(a.Left, schema), Bind(a.Right, schema)),
            OrExpression o => Expressions.Or(Bind(o.Left, schema), Bind(o.Right, schema)),
            NotExpression n => Expressions.Not(Bind(n.Child, schema)),
            ComparisonPredicate c => BindComparison(c, schema),
            UnaryPredicate u => BindUnary(u, schema),
            SetPredicate s => BindSet(s, schema),
            UnboundReference r => BindReference(r, schema),
            BoundReference => expr,
            LiteralExpression => expr,
            ApplyExpression a => BindApply(a, schema),
            _ => expr,
        };
    }

    private static Expression BindComparison(ComparisonPredicate pred, Schema schema)
    {
        var left = Bind(pred.Left, schema);
        var right = Bind(pred.Right, schema);

        // If the reference couldn't be resolved, the predicate can't filter
        if (left is TrueExpression || right is TrueExpression)
            return Expression.True;

        return new ComparisonPredicate(pred.Op, left, right);
    }

    private static Expression BindUnary(UnaryPredicate pred, Schema schema)
    {
        var child = Bind(pred.Child, schema);
        if (child is TrueExpression)
            return Expression.True;
        return new UnaryPredicate(pred.Op, child);
    }

    private static Expression BindSet(SetPredicate pred, Schema schema)
    {
        var child = Bind(pred.Child, schema);
        if (child is TrueExpression)
            return Expression.True;
        return new SetPredicate(pred.Op, child, pred.Values);
    }

    private static Expression BindReference(UnboundReference r, Schema schema)
    {
        var field = schema.Fields.FirstOrDefault(f => f.Name == r.Name);
        if (field is null)
            return Expression.True; // unknown column can't filter
        return new BoundReference(field.Id, field.Name, field.Type);
    }

    private static Expression BindApply(ApplyExpression apply, Schema schema)
    {
        var boundArgs = apply.Arguments.Select(a => Bind(a, schema)).ToList();
        return new ApplyExpression(apply.Function, boundArgs);
    }
}
