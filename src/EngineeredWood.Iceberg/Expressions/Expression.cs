namespace EngineeredWood.Iceberg.Expressions;

/// <summary>
/// Base type for all Iceberg expressions.
/// An expression can be a reference, literal, function call, or predicate.
/// </summary>
public abstract record Expression
{
    public static readonly Expression True = new TrueExpression();
    public static readonly Expression False = new FalseExpression();
}

// --- Leaf expressions ---

/// <summary>
/// An unbound column reference (by name). Resolved to a BoundReference by binding.
/// </summary>
public sealed record UnboundReference(string Name) : Expression;

/// <summary>
/// A bound column reference (by field ID). Produced by binding an UnboundReference.
/// </summary>
internal sealed record BoundReference(int FieldId, string Name, IcebergType FieldType) : Expression;

/// <summary>
/// A literal value expression, optionally carrying an explicit Iceberg data type.
/// </summary>
public sealed record LiteralExpression(LiteralValue Value, IcebergType? DataType = null) : Expression;

/// <summary>
/// A function call expression: func-name applied to argument expressions.
/// Replaces the old Transform-in-expression pattern.
/// </summary>
public sealed record ApplyExpression(FunctionIdentifier Function, IReadOnlyList<Expression> Arguments) : Expression;

/// <summary>
/// Identifies a function by optional catalog, namespace, and required name.
/// A simple string name has no catalog/namespace.
/// </summary>
public sealed record FunctionIdentifier(string Name, string? Catalog = null, IReadOnlyList<string>? Namespace = null);

// --- Boolean connectives ---

public sealed record TrueExpression : Expression;
public sealed record FalseExpression : Expression;
public sealed record AndExpression(Expression Left, Expression Right) : Expression;
public sealed record OrExpression(Expression Left, Expression Right) : Expression;
public sealed record NotExpression(Expression Child) : Expression;

// --- Predicates ---
// Comparison predicates take left/right expressions (not term/value).
// Unary predicates take a child expression.
// Set predicates take a child expression and a list of literal values.

/// <summary>
/// A comparison predicate: left OP right (e.g. ref("id") eq lit(42)).
/// </summary>
public sealed record ComparisonPredicate(
    ComparisonOperator Op,
    Expression Left,
    Expression Right) : Expression;

/// <summary>
/// A unary predicate: OP(child) (e.g. is-null(ref("name"))).
/// </summary>
public sealed record UnaryPredicate(
    UnaryOperator Op,
    Expression Child) : Expression;

/// <summary>
/// A set predicate: child IN/NOT IN values.
/// </summary>
public sealed record SetPredicate(
    SetOperator Op,
    Expression Child,
    IReadOnlyList<LiteralValue> Values) : Expression;

public enum ComparisonOperator
{
    Eq, NotEq, Lt, LtEq, Gt, GtEq, StartsWith, NotStartsWith,
}

public enum UnaryOperator
{
    IsNull, NotNull, IsNan, NotNan,
}

public enum SetOperator
{
    In, NotIn,
}
