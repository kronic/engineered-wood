using EngineeredWood.Iceberg.Expressions;

namespace EngineeredWood.Iceberg.Tests.Expressions;

public class ManifestEvaluatorTests
{
    // Field IDs
    private const int Id = 1;
    private const int Name = 2;
    private const int Score = 3;

    private static BoundReference BRef(int fieldId, IcebergType type) =>
        new(fieldId, $"f{fieldId}", type);

    private static Expression Cmp(ComparisonOperator op, int fieldId, LiteralValue value) =>
        new ComparisonPredicate(op, BRef(fieldId, IcebergType.Long), new LiteralExpression(value));

    private static Expression Unary(UnaryOperator op, int fieldId) =>
        new UnaryPredicate(op, BRef(fieldId, IcebergType.Long));

    private static Expression InSet(int fieldId, params LiteralValue[] values) =>
        new SetPredicate(SetOperator.In, BRef(fieldId, IcebergType.Long), values);

    private static DataFileStats Stats(
        long recordCount = 1000,
        Dictionary<int, LiteralValue>? lower = null,
        Dictionary<int, LiteralValue>? upper = null,
        Dictionary<int, long>? nullCounts = null) =>
        new()
        {
            RecordCount = recordCount,
            LowerBounds = lower ?? new Dictionary<int, LiteralValue>(),
            UpperBounds = upper ?? new Dictionary<int, LiteralValue>(),
            NullCounts = nullCounts ?? new Dictionary<int, long>(),
        };

    // --- Eq ---

    [Fact]
    public void Eq_ValueInRange_MightMatch()
    {
        var stats = Stats(
            lower: new() { [Id] = LiteralValue.Of(10L) },
            upper: new() { [Id] = LiteralValue.Of(100L) });
        Assert.True(ManifestEvaluator.MightMatch(
            Cmp(ComparisonOperator.Eq, Id, LiteralValue.Of(50L)), stats));
    }

    [Fact]
    public void Eq_ValueBelowRange_NoMatch()
    {
        var stats = Stats(
            lower: new() { [Id] = LiteralValue.Of(10L) },
            upper: new() { [Id] = LiteralValue.Of(100L) });
        Assert.False(ManifestEvaluator.MightMatch(
            Cmp(ComparisonOperator.Eq, Id, LiteralValue.Of(5L)), stats));
    }

    [Fact]
    public void Eq_ValueAboveRange_NoMatch()
    {
        var stats = Stats(
            lower: new() { [Id] = LiteralValue.Of(10L) },
            upper: new() { [Id] = LiteralValue.Of(100L) });
        Assert.False(ManifestEvaluator.MightMatch(
            Cmp(ComparisonOperator.Eq, Id, LiteralValue.Of(200L)), stats));
    }

    [Fact]
    public void Eq_AllNulls_NoMatch()
    {
        var stats = Stats(recordCount: 100, nullCounts: new() { [Id] = 100 });
        Assert.False(ManifestEvaluator.MightMatch(
            Cmp(ComparisonOperator.Eq, Id, LiteralValue.Of(1L)), stats));
    }

    // --- Lt ---

    [Fact]
    public void Lt_LowerBelowValue_MightMatch()
    {
        var stats = Stats(lower: new() { [Id] = LiteralValue.Of(10L) });
        Assert.True(ManifestEvaluator.MightMatch(
            Cmp(ComparisonOperator.Lt, Id, LiteralValue.Of(50L)), stats));
    }

    [Fact]
    public void Lt_LowerAtValue_NoMatch()
    {
        var stats = Stats(lower: new() { [Id] = LiteralValue.Of(50L) });
        Assert.False(ManifestEvaluator.MightMatch(
            Cmp(ComparisonOperator.Lt, Id, LiteralValue.Of(50L)), stats));
    }

    // --- Gt ---

    [Fact]
    public void Gt_UpperAboveValue_MightMatch()
    {
        var stats = Stats(upper: new() { [Id] = LiteralValue.Of(100L) });
        Assert.True(ManifestEvaluator.MightMatch(
            Cmp(ComparisonOperator.Gt, Id, LiteralValue.Of(50L)), stats));
    }

    [Fact]
    public void Gt_UpperAtValue_NoMatch()
    {
        var stats = Stats(upper: new() { [Id] = LiteralValue.Of(50L) });
        Assert.False(ManifestEvaluator.MightMatch(
            Cmp(ComparisonOperator.Gt, Id, LiteralValue.Of(50L)), stats));
    }

    // --- LtEq / GtEq ---

    [Fact]
    public void LtEq_LowerAtValue_MightMatch()
    {
        var stats = Stats(lower: new() { [Id] = LiteralValue.Of(50L) });
        Assert.True(ManifestEvaluator.MightMatch(
            Cmp(ComparisonOperator.LtEq, Id, LiteralValue.Of(50L)), stats));
    }

    [Fact]
    public void GtEq_UpperAtValue_MightMatch()
    {
        var stats = Stats(upper: new() { [Id] = LiteralValue.Of(50L) });
        Assert.True(ManifestEvaluator.MightMatch(
            Cmp(ComparisonOperator.GtEq, Id, LiteralValue.Of(50L)), stats));
    }

    // --- IsNull / NotNull ---

    [Fact]
    public void IsNull_NoNulls_NoMatch()
    {
        var stats = Stats(nullCounts: new() { [Id] = 0 });
        Assert.False(ManifestEvaluator.MightMatch(Unary(UnaryOperator.IsNull, Id), stats));
    }

    [Fact]
    public void IsNull_HasNulls_MightMatch()
    {
        var stats = Stats(nullCounts: new() { [Id] = 5 });
        Assert.True(ManifestEvaluator.MightMatch(Unary(UnaryOperator.IsNull, Id), stats));
    }

    [Fact]
    public void NotNull_AllNulls_NoMatch()
    {
        var stats = Stats(recordCount: 100, nullCounts: new() { [Id] = 100 });
        Assert.False(ManifestEvaluator.MightMatch(Unary(UnaryOperator.NotNull, Id), stats));
    }

    // --- In ---

    [Fact]
    public void In_SomeValuesInRange_MightMatch()
    {
        var stats = Stats(
            lower: new() { [Id] = LiteralValue.Of(10L) },
            upper: new() { [Id] = LiteralValue.Of(20L) });
        Assert.True(ManifestEvaluator.MightMatch(
            InSet(Id, LiteralValue.Of(5L), LiteralValue.Of(15L), LiteralValue.Of(25L)), stats));
    }

    [Fact]
    public void In_NoValuesInRange_NoMatch()
    {
        var stats = Stats(
            lower: new() { [Id] = LiteralValue.Of(10L) },
            upper: new() { [Id] = LiteralValue.Of(20L) });
        Assert.False(ManifestEvaluator.MightMatch(
            InSet(Id, LiteralValue.Of(1L), LiteralValue.Of(25L)), stats));
    }

    // --- String predicates ---

    [Fact]
    public void Eq_String_InRange_MightMatch()
    {
        var stats = Stats(
            lower: new() { [Name] = LiteralValue.Of("alice") },
            upper: new() { [Name] = LiteralValue.Of("dave") });
        Assert.True(ManifestEvaluator.MightMatch(
            new ComparisonPredicate(ComparisonOperator.Eq,
                BRef(Name, IcebergType.String), new LiteralExpression(LiteralValue.Of("bob"))),
            stats));
    }

    [Fact]
    public void Eq_String_OutOfRange_NoMatch()
    {
        var stats = Stats(
            lower: new() { [Name] = LiteralValue.Of("alice") },
            upper: new() { [Name] = LiteralValue.Of("dave") });
        Assert.False(ManifestEvaluator.MightMatch(
            new ComparisonPredicate(ComparisonOperator.Eq,
                BRef(Name, IcebergType.String), new LiteralExpression(LiteralValue.Of("zoe"))),
            stats));
    }

    [Fact]
    public void StartsWith_PrefixInRange_MightMatch()
    {
        var stats = Stats(
            lower: new() { [Name] = LiteralValue.Of("abc") },
            upper: new() { [Name] = LiteralValue.Of("def") });
        Assert.True(ManifestEvaluator.MightMatch(
            new ComparisonPredicate(ComparisonOperator.StartsWith,
                BRef(Name, IcebergType.String), new LiteralExpression(LiteralValue.Of("bc"))),
            stats));
    }

    [Fact]
    public void StartsWith_PrefixAboveRange_NoMatch()
    {
        var stats = Stats(
            lower: new() { [Name] = LiteralValue.Of("abc") },
            upper: new() { [Name] = LiteralValue.Of("abd") });
        Assert.False(ManifestEvaluator.MightMatch(
            new ComparisonPredicate(ComparisonOperator.StartsWith,
                BRef(Name, IcebergType.String), new LiteralExpression(LiteralValue.Of("xyz"))),
            stats));
    }

    // --- Compound expressions ---

    [Fact]
    public void And_BothMatch_MightMatch()
    {
        var stats = Stats(
            lower: new() { [Id] = LiteralValue.Of(10L), [Score] = LiteralValue.Of(1.0) },
            upper: new() { [Id] = LiteralValue.Of(100L), [Score] = LiteralValue.Of(99.0) });

        var expr = new AndExpression(
            Cmp(ComparisonOperator.Gt, Id, LiteralValue.Of(5L)),
            new ComparisonPredicate(ComparisonOperator.Lt,
                BRef(Score, IcebergType.Double), new LiteralExpression(LiteralValue.Of(50.0))));

        Assert.True(ManifestEvaluator.MightMatch(expr, stats));
    }

    [Fact]
    public void And_OneDoesNotMatch_NoMatch()
    {
        var stats = Stats(
            lower: new() { [Id] = LiteralValue.Of(10L) },
            upper: new() { [Id] = LiteralValue.Of(100L) });

        var expr = new AndExpression(
            Cmp(ComparisonOperator.Gt, Id, LiteralValue.Of(5L)),
            Cmp(ComparisonOperator.Lt, Id, LiteralValue.Of(5L)));

        Assert.False(ManifestEvaluator.MightMatch(expr, stats));
    }

    [Fact]
    public void Or_OneMatches_MightMatch()
    {
        var stats = Stats(
            lower: new() { [Id] = LiteralValue.Of(10L) },
            upper: new() { [Id] = LiteralValue.Of(100L) });

        var expr = new OrExpression(
            Cmp(ComparisonOperator.Eq, Id, LiteralValue.Of(50L)),
            Cmp(ComparisonOperator.Eq, Id, LiteralValue.Of(200L)));

        Assert.True(ManifestEvaluator.MightMatch(expr, stats));
    }

    // --- No stats (conservative) ---

    [Fact]
    public void NoStats_AlwaysMightMatch()
    {
        var stats = Stats();
        Assert.True(ManifestEvaluator.MightMatch(
            Cmp(ComparisonOperator.Eq, Id, LiteralValue.Of(42L)), stats));
    }
}
