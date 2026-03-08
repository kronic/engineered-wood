using EngineeredWood.Orc.Proto;

namespace EngineeredWood.Orc;

/// <summary>
/// Options for configuring ORC file writing.
/// </summary>
public sealed class OrcWriterOptions
{
    /// <summary>
    /// Target stripe size in bytes. Default is 64 MB.
    /// </summary>
    public long StripeSize { get; set; } = 64 * 1024 * 1024;

    /// <summary>
    /// Compression algorithm. Default is ZSTD.
    /// </summary>
    public CompressionKind Compression { get; set; } = CompressionKind.Zstd;

    /// <summary>
    /// Compression block size. Default is 256 KB.
    /// </summary>
    public int CompressionBlockSize { get; set; } = 256 * 1024;

    /// <summary>
    /// Default encoding family for integer columns.
    /// </summary>
    public EncodingFamily DefaultIntegerEncoding { get; set; } = EncodingFamily.V2;

    /// <summary>
    /// Default encoding family for string columns.
    /// DictionaryV2 uses dictionary encoding; DirectV2 uses direct encoding.
    /// </summary>
    public EncodingFamily DefaultStringEncoding { get; set; } = EncodingFamily.DictionaryV2;

    /// <summary>
    /// Per-column encoding overrides, keyed by column name.
    /// </summary>
    public Dictionary<string, EncodingFamily>? ColumnEncodings { get; set; }

    /// <summary>
    /// Dictionary size threshold (max unique values). If a string column exceeds this,
    /// it falls back to direct encoding. Default is 40000.
    /// </summary>
    public int DictionaryKeySizeThreshold { get; set; } = 40000;

    /// <summary>
    /// Whether to collect and emit column statistics. Default is true.
    /// </summary>
    public bool EnableStatistics { get; set; } = true;

    /// <summary>
    /// Number of rows between row index entries within a stripe.
    /// Set to 0 to disable row index generation. Default is 10,000.
    /// </summary>
    public int RowIndexStride { get; set; } = 10_000;

    /// <summary>
    /// User-defined metadata key/value pairs to include in the ORC file footer.
    /// Keys are strings, values are byte arrays.
    /// </summary>
    public Dictionary<string, byte[]>? UserMetadata { get; set; }
}

/// <summary>
/// High-level encoding family selection.
/// </summary>
public enum EncodingFamily
{
    /// <summary>RLE v2 Direct encoding for integers, Direct for strings.</summary>
    V2,
    /// <summary>RLE v2 Dictionary encoding for strings.</summary>
    DictionaryV2,
}
