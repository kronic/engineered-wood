namespace EngineeredWood.Parquet;

/// <summary>
/// Options that control how Parquet data is read and mapped to Apache Arrow types.
/// </summary>
public sealed class ParquetReadOptions
{
    /// <summary>Default options: all features disabled, producing standard Arrow types.</summary>
    public static readonly ParquetReadOptions Default = new();

    /// <summary>
    /// When true, BYTE_ARRAY (string/binary) columns are returned as
    /// <see cref="Apache.Arrow.Types.StringViewType"/> or
    /// <see cref="Apache.Arrow.Types.BinaryViewType"/> instead of the default
    /// <see cref="Apache.Arrow.Types.StringType"/> / <see cref="Apache.Arrow.Types.BinaryType"/>.
    /// <para>
    /// View arrays store values ≤12 bytes inline in the views buffer — no heap copy.
    /// Longer values are copied to a single overflow buffer referenced by (buffer_index, offset).
    /// This eliminates the nullable scatter step and reduces allocation for short strings.
    /// </para>
    /// </summary>
    public bool UseViewTypes { get; init; } = false;
}
