using EngineeredWood.Iceberg.Manifest;

namespace EngineeredWood.Iceberg.Expressions;

/// <summary>
/// Evaluates bound expressions against data file statistics (lower/upper bounds, null counts)
/// to determine if a file might contain matching rows.
///
/// Returns true if the file MIGHT contain matches (cannot be skipped).
/// Returns false if the file CANNOT contain matches (safe to skip).
/// </summary>
internal static class ManifestEvaluator
{
    public static bool MightMatch(Expression expr, DataFileStats stats)
    {
        return expr switch
        {
            TrueExpression => true,
            FalseExpression => false,
            AndExpression a => MightMatch(a.Left, stats) && MightMatch(a.Right, stats),
            OrExpression o => MightMatch(o.Left, stats) || MightMatch(o.Right, stats),
            NotExpression n => !CannotMatch(n.Child, stats),
            ComparisonPredicate c => EvalComparison(c, stats),
            UnaryPredicate u => EvalUnary(u, stats),
            SetPredicate s => EvalSet(s, stats),
            _ => true, // unknown expressions, apply, etc. — conservative
        };
    }

    private static bool CannotMatch(Expression expr, DataFileStats stats)
    {
        return expr switch
        {
            TrueExpression => false,
            FalseExpression => true,
            AndExpression a => CannotMatch(a.Left, stats) || CannotMatch(a.Right, stats),
            OrExpression o => CannotMatch(o.Left, stats) && CannotMatch(o.Right, stats),
            NotExpression n => !MightMatch(n.Child, stats),
            ComparisonPredicate c => !EvalComparison(c, stats),
            UnaryPredicate u => !EvalUnary(u, stats),
            SetPredicate s => !EvalSet(s, stats),
            _ => false,
        };
    }

    // --- Helpers to extract field ID and literal from expression children ---

    private static bool TryGetFieldId(Expression expr, out int fieldId)
    {
        if (expr is BoundReference r) { fieldId = r.FieldId; return true; }
        fieldId = 0;
        return false;
    }

    private static bool TryGetLiteral(Expression expr, out LiteralValue value)
    {
        if (expr is LiteralExpression l) { value = l.Value; return true; }
        value = null!;
        return false;
    }

    // --- Comparison predicates ---

    private static bool EvalComparison(ComparisonPredicate pred, DataFileStats stats)
    {
        // We can only evaluate ref-vs-literal comparisons against stats.
        // For ref-vs-ref or apply-vs-literal, be conservative.
        if (TryGetFieldId(pred.Left, out var fieldId) && TryGetLiteral(pred.Right, out var value))
            return EvalRefLiteral(fieldId, pred.Op, value, stats);

        // Flipped: literal on left, ref on right
        if (TryGetLiteral(pred.Left, out value) && TryGetFieldId(pred.Right, out fieldId))
        {
            var flipped = FlipOp(pred.Op);
            return flipped is not null ? EvalRefLiteral(fieldId, flipped.Value, value, stats) : true;
        }

        return true; // conservative
    }

    private static ComparisonOperator? FlipOp(ComparisonOperator op) => op switch
    {
        ComparisonOperator.Eq => ComparisonOperator.Eq,
        ComparisonOperator.NotEq => ComparisonOperator.NotEq,
        ComparisonOperator.Lt => ComparisonOperator.Gt,
        ComparisonOperator.LtEq => ComparisonOperator.GtEq,
        ComparisonOperator.Gt => ComparisonOperator.Lt,
        ComparisonOperator.GtEq => ComparisonOperator.LtEq,
        _ => null, // starts-with etc. can't be flipped meaningfully
    };

    private static bool EvalRefLiteral(int fieldId, ComparisonOperator op, LiteralValue value, DataFileStats stats)
    {
        return op switch
        {
            ComparisonOperator.Eq => EvalEq(fieldId, value, stats),
            ComparisonOperator.NotEq => EvalNotEq(fieldId, value, stats),
            ComparisonOperator.Lt => EvalLt(fieldId, value, stats),
            ComparisonOperator.LtEq => EvalLtEq(fieldId, value, stats),
            ComparisonOperator.Gt => EvalGt(fieldId, value, stats),
            ComparisonOperator.GtEq => EvalGtEq(fieldId, value, stats),
            ComparisonOperator.StartsWith => EvalStartsWith(fieldId, value, stats),
            ComparisonOperator.NotStartsWith => true,
            _ => true,
        };
    }

    // --- Unary predicates ---

    private static bool EvalUnary(UnaryPredicate pred, DataFileStats stats)
    {
        if (!TryGetFieldId(pred.Child, out var fieldId))
            return true;

        return pred.Op switch
        {
            UnaryOperator.IsNull => EvalIsNull(fieldId, stats),
            UnaryOperator.NotNull => EvalNotNull(fieldId, stats),
            UnaryOperator.IsNan => true,
            UnaryOperator.NotNan => true,
            _ => true,
        };
    }

    // --- Set predicates ---

    private static bool EvalSet(SetPredicate pred, DataFileStats stats)
    {
        if (!TryGetFieldId(pred.Child, out var fieldId))
            return true;

        return pred.Op switch
        {
            SetOperator.In => EvalIn(fieldId, pred.Values, stats),
            SetOperator.NotIn => true,
            _ => true,
        };
    }

    // --- Evaluation logic (unchanged from before) ---

    private static bool EvalIsNull(int fieldId, DataFileStats stats)
    {
        if (stats.NullCounts.TryGetValue(fieldId, out var nullCount))
            return nullCount > 0;
        return true;
    }

    private static bool EvalNotNull(int fieldId, DataFileStats stats)
    {
        if (stats.NullCounts.TryGetValue(fieldId, out var nullCount))
            return nullCount < stats.RecordCount;
        return true;
    }

    private static bool EvalEq(int fieldId, LiteralValue value, DataFileStats stats)
    {
        if (stats.NullCounts.TryGetValue(fieldId, out var nc) && nc == stats.RecordCount)
            return false;
        if (stats.LowerBounds.TryGetValue(fieldId, out var lower) && value < lower)
            return false;
        if (stats.UpperBounds.TryGetValue(fieldId, out var upper) && value > upper)
            return false;
        return true;
    }

    private static bool EvalNotEq(int fieldId, LiteralValue value, DataFileStats stats)
    {
        if (stats.LowerBounds.TryGetValue(fieldId, out var lower) &&
            stats.UpperBounds.TryGetValue(fieldId, out var upper) &&
            lower.CompareTo(upper) == 0 && lower.CompareTo(value) == 0)
        {
            if (stats.NullCounts.TryGetValue(fieldId, out var nc) && nc > 0)
                return true;
            return false;
        }
        return true;
    }

    private static bool EvalLt(int fieldId, LiteralValue value, DataFileStats stats)
    {
        if (stats.NullCounts.TryGetValue(fieldId, out var nc) && nc == stats.RecordCount)
            return false;
        if (stats.LowerBounds.TryGetValue(fieldId, out var lower) && lower >= value)
            return false;
        return true;
    }

    private static bool EvalLtEq(int fieldId, LiteralValue value, DataFileStats stats)
    {
        if (stats.NullCounts.TryGetValue(fieldId, out var nc) && nc == stats.RecordCount)
            return false;
        if (stats.LowerBounds.TryGetValue(fieldId, out var lower) && lower > value)
            return false;
        return true;
    }

    private static bool EvalGt(int fieldId, LiteralValue value, DataFileStats stats)
    {
        if (stats.NullCounts.TryGetValue(fieldId, out var nc) && nc == stats.RecordCount)
            return false;
        if (stats.UpperBounds.TryGetValue(fieldId, out var upper) && upper <= value)
            return false;
        return true;
    }

    private static bool EvalGtEq(int fieldId, LiteralValue value, DataFileStats stats)
    {
        if (stats.NullCounts.TryGetValue(fieldId, out var nc) && nc == stats.RecordCount)
            return false;
        if (stats.UpperBounds.TryGetValue(fieldId, out var upper) && upper < value)
            return false;
        return true;
    }

    private static bool EvalIn(int fieldId, IReadOnlyList<LiteralValue> values, DataFileStats stats)
    {
        if (stats.NullCounts.TryGetValue(fieldId, out var nc) && nc == stats.RecordCount)
            return false;
        var hasLower = stats.LowerBounds.TryGetValue(fieldId, out var lower);
        var hasUpper = stats.UpperBounds.TryGetValue(fieldId, out var upper);
        if (!hasLower && !hasUpper)
            return true;
        foreach (var v in values)
        {
            var aboveLower = !hasLower || v >= lower!;
            var belowUpper = !hasUpper || v <= upper!;
            if (aboveLower && belowUpper)
                return true;
        }
        return false;
    }

    private static bool EvalStartsWith(int fieldId, LiteralValue prefix, DataFileStats stats)
    {
        if (prefix.Value is not string prefixStr)
            return true;
        if (stats.UpperBounds.TryGetValue(fieldId, out var upper) && upper.Value is string upperStr)
        {
            if (string.Compare(prefixStr, upperStr, StringComparison.Ordinal) > 0)
                return false;
        }
        if (stats.LowerBounds.TryGetValue(fieldId, out var lower) && lower.Value is string lowerStr)
        {
            var prefixEnd = IncrementPrefix(prefixStr);
            if (prefixEnd is not null &&
                string.Compare(prefixEnd, lowerStr, StringComparison.Ordinal) <= 0)
                return false;
        }
        return true;
    }

    private static string? IncrementPrefix(string prefix)
    {
        if (prefix.Length == 0) return null;
        var chars = prefix.ToCharArray();
        var last = chars[^1];
        if (last == char.MaxValue) return null;
        chars[^1] = (char)(last + 1);
        return new string(chars);
    }
}

/// <summary>
/// Column-level statistics for a single data file, used by ManifestEvaluator.
/// </summary>
internal sealed class DataFileStats
{
    public required long RecordCount { get; init; }
    public IReadOnlyDictionary<int, LiteralValue> LowerBounds { get; init; } = new Dictionary<int, LiteralValue>();
    public IReadOnlyDictionary<int, LiteralValue> UpperBounds { get; init; } = new Dictionary<int, LiteralValue>();
    public IReadOnlyDictionary<int, long> NullCounts { get; init; } = new Dictionary<int, long>();
    public IReadOnlyDictionary<int, long> NanCounts { get; init; } = new Dictionary<int, long>();
}
