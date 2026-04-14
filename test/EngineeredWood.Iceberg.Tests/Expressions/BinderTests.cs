using EngineeredWood.Iceberg.Expressions;
using Ex = EngineeredWood.Iceberg.Expressions.Expressions;

namespace EngineeredWood.Iceberg.Tests.Expressions;

public class BinderTests
{
    private readonly Schema _schema = new(0, [
        new NestedField(1, "id", IcebergType.Long, true),
        new NestedField(2, "name", IcebergType.String, false),
        new NestedField(3, "score", IcebergType.Double, false),
    ]);

    [Fact]
    public void Bind_ResolvesColumnNameToFieldId()
    {
        var expr = Ex.Equal("id", 42L);
        var bound = ExpressionBinder.Bind(expr, _schema);

        Assert.IsType<ComparisonPredicate>(bound);
        var pred = (ComparisonPredicate)bound;
        Assert.IsType<BoundReference>(pred.Left);
        Assert.Equal(1, ((BoundReference)pred.Left).FieldId);
        Assert.Equal(ComparisonOperator.Eq, pred.Op);
    }

    [Fact]
    public void Bind_UnknownColumn_ReturnsTrue()
    {
        var expr = Ex.Equal("nonexistent", 1);
        var bound = ExpressionBinder.Bind(expr, _schema);
        Assert.IsType<TrueExpression>(bound);
    }

    [Fact]
    public void Bind_And_BindsBothSides()
    {
        var expr = Ex.And(
            Ex.Equal("id", 1L),
            Ex.GreaterThan("score", 90.0));

        var bound = ExpressionBinder.Bind(expr, _schema);
        Assert.IsType<AndExpression>(bound);

        var and = (AndExpression)bound;
        var left = (ComparisonPredicate)and.Left;
        var right = (ComparisonPredicate)and.Right;
        Assert.Equal(1, ((BoundReference)left.Left).FieldId);
        Assert.Equal(3, ((BoundReference)right.Left).FieldId);
    }

    [Fact]
    public void Bind_Or_BindsBothSides()
    {
        var expr = Ex.Or(
            Ex.Equal("name", "alice"),
            Ex.Equal("name", "bob"));

        var bound = ExpressionBinder.Bind(expr, _schema);
        Assert.IsType<OrExpression>(bound);
    }

    [Fact]
    public void Bind_Not_BindsChild()
    {
        var expr = Ex.Not(Ex.IsNull("name"));
        var bound = ExpressionBinder.Bind(expr, _schema);
        Assert.IsType<NotExpression>(bound);

        var not = (NotExpression)bound;
        Assert.IsType<UnaryPredicate>(not.Child);
        var unary = (UnaryPredicate)not.Child;
        Assert.IsType<BoundReference>(unary.Child);
    }

    [Fact]
    public void Bind_PreservesFieldType()
    {
        var expr = Ex.Equal("name", "test");
        var bound = (ComparisonPredicate)ExpressionBinder.Bind(expr, _schema);
        var r = (BoundReference)bound.Left;
        Assert.Equal(IcebergType.String, r.FieldType);
    }

    [Fact]
    public void Bind_ApplyExpression_BindsArguments()
    {
        var expr = Ex.Apply("day", Ex.Ref("id"));
        var bound = ExpressionBinder.Bind(expr, _schema);

        Assert.IsType<ApplyExpression>(bound);
        var apply = (ApplyExpression)bound;
        Assert.IsType<BoundReference>(apply.Arguments[0]);
    }
}
