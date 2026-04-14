namespace EngineeredWood.Iceberg.Expressions;

/// <summary>
/// A typed literal value for use in predicates.
/// </summary>
public sealed class LiteralValue : IComparable<LiteralValue>
{
    public object Value { get; }

    private LiteralValue(object value) => Value = value;

    public static LiteralValue Of(bool value) => new(value);
    public static LiteralValue Of(int value) => new(value);
    public static LiteralValue Of(long value) => new(value);
    public static LiteralValue Of(float value) => new(value);
    public static LiteralValue Of(double value) => new(value);
    public static LiteralValue Of(string value) => new(value);
    public static LiteralValue Of(byte[] value) => new(value);
#if NET6_0_OR_GREATER
    public static LiteralValue Of(DateOnly value) => new(value);
#endif
    public static LiteralValue Of(Guid value) => new(value);

    /// <summary>
    /// Compare two literal values. Both must be the same underlying type.
    /// </summary>
    public int CompareTo(LiteralValue? other)
    {
        if (other is null) return 1;

        return (Value, other.Value) switch
        {
            (bool a, bool b) => a.CompareTo(b),
            (int a, int b) => a.CompareTo(b),
            (long a, long b) => a.CompareTo(b),
            (float a, float b) => a.CompareTo(b),
            (double a, double b) => a.CompareTo(b),
            (string a, string b) => string.Compare(a, b, StringComparison.Ordinal),
#if NET6_0_OR_GREATER
            (DateOnly a, DateOnly b) => a.CompareTo(b),
#endif
            (Guid a, Guid b) => a.CompareTo(b),
            // Cross-type numeric promotion
            (int a, long b) => ((long)a).CompareTo(b),
            (long a, int b) => a.CompareTo((long)b),
            (float a, double b) => ((double)a).CompareTo(b),
            (double a, float b) => a.CompareTo((double)b),
            _ => throw new InvalidOperationException(
                $"Cannot compare {Value.GetType().Name} with {other.Value.GetType().Name}"),
        };
    }

    public override bool Equals(object? obj) =>
        obj is LiteralValue other && Value.Equals(other.Value);

    public override int GetHashCode() => Value.GetHashCode();

    public override string ToString() => Value.ToString() ?? "";

    public static bool operator <(LiteralValue left, LiteralValue right) => left.CompareTo(right) < 0;
    public static bool operator >(LiteralValue left, LiteralValue right) => left.CompareTo(right) > 0;
    public static bool operator <=(LiteralValue left, LiteralValue right) => left.CompareTo(right) <= 0;
    public static bool operator >=(LiteralValue left, LiteralValue right) => left.CompareTo(right) >= 0;
}
