using EngineeredWood.Iceberg.Expressions;
using Ex = EngineeredWood.Iceberg.Expressions.Expressions;

namespace EngineeredWood.Iceberg.Tests.Expressions;

public class ExpressionTests
{
    [Fact]
    public void And_WithFalse_ReturnsFalse()
    {
        var result = Ex.And(Expression.True, Expression.False);
        Assert.IsType<FalseExpression>(result);
    }

    [Fact]
    public void And_WithTrue_ReturnsOther()
    {
        var pred = Ex.Equal("x", 1);
        var result = Ex.And(Expression.True, pred);
        Assert.Same(pred, result);
    }

    [Fact]
    public void Or_WithTrue_ReturnsTrue()
    {
        var result = Ex.Or(Expression.False, Expression.True);
        Assert.IsType<TrueExpression>(result);
    }

    [Fact]
    public void Or_WithFalse_ReturnsOther()
    {
        var pred = Ex.Equal("x", 1);
        var result = Ex.Or(Expression.False, pred);
        Assert.Same(pred, result);
    }

    [Fact]
    public void Not_True_ReturnsFalse()
    {
        Assert.IsType<FalseExpression>(Ex.Not(Expression.True));
    }

    [Fact]
    public void Not_Not_Unwraps()
    {
        var pred = Ex.Equal("x", 1);
        var result = Ex.Not(Ex.Not(pred));
        Assert.Same(pred, result);
    }

    [Fact]
    public void LiteralValue_Comparison()
    {
        Assert.True(LiteralValue.Of(1) < LiteralValue.Of(2));
        Assert.True(LiteralValue.Of(10L) > LiteralValue.Of(5L));
        Assert.True(LiteralValue.Of("abc") < LiteralValue.Of("xyz"));
        Assert.True(LiteralValue.Of(1.0) <= LiteralValue.Of(1.0));
    }

    [Fact]
    public void LiteralValue_CrossTypeNumericComparison()
    {
        Assert.True(LiteralValue.Of(5) < LiteralValue.Of(10L));
        Assert.True(LiteralValue.Of(1.0f) < LiteralValue.Of(2.0));
    }

    [Fact]
    public void Equal_ProducesComparisonPredicate()
    {
        var expr = Ex.Equal("id", 42L);
        Assert.IsType<ComparisonPredicate>(expr);
        var cmp = (ComparisonPredicate)expr;
        Assert.IsType<UnboundReference>(cmp.Left);
        Assert.IsType<LiteralExpression>(cmp.Right);
        Assert.Equal(ComparisonOperator.Eq, cmp.Op);
    }

    [Fact]
    public void IsNull_ProducesUnaryPredicate()
    {
        var expr = Ex.IsNull("name");
        Assert.IsType<UnaryPredicate>(expr);
        var u = (UnaryPredicate)expr;
        Assert.IsType<UnboundReference>(u.Child);
        Assert.Equal(UnaryOperator.IsNull, u.Op);
    }

    [Fact]
    public void In_ProducesSetPredicate()
    {
        var expr = Ex.In("id", 1L, 2L, 3L);
        Assert.IsType<SetPredicate>(expr);
        var s = (SetPredicate)expr;
        Assert.IsType<UnboundReference>(s.Child);
        Assert.Equal(3, s.Values.Count);
    }

    [Fact]
    public void Ref_ProducesUnboundReference()
    {
        var expr = Ex.Ref("col");
        Assert.IsType<UnboundReference>(expr);
        Assert.Equal("col", ((UnboundReference)expr).Name);
    }

    [Fact]
    public void Apply_ProducesApplyExpression()
    {
        var expr = Ex.Apply("day", Ex.Ref("ts"));
        Assert.IsType<ApplyExpression>(expr);
        var a = (ApplyExpression)expr;
        Assert.Equal("day", a.Function.Name);
        Assert.Single(a.Arguments);
    }

    [Fact]
    public void GenericEqual_TakesExpressions()
    {
        var expr = Ex.Equal(Ex.Ref("a"), Ex.Ref("b"));
        Assert.IsType<ComparisonPredicate>(expr);
        var cmp = (ComparisonPredicate)expr;
        Assert.IsType<UnboundReference>(cmp.Left);
        Assert.IsType<UnboundReference>(cmp.Right);
    }
}
