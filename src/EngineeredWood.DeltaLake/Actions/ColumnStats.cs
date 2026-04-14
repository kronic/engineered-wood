using System.Text.Json;

namespace EngineeredWood.DeltaLake.Actions;

/// <summary>
/// Per-file column-level statistics stored in the <c>stats</c> field
/// of <see cref="AddFile"/> actions.
/// </summary>
public sealed record ColumnStats
{
    /// <summary>Total number of records in the file.</summary>
    public long NumRecords { get; init; }

    /// <summary>Minimum values per column (column name → value).</summary>
    public IReadOnlyDictionary<string, JsonElement>? MinValues { get; init; }

    /// <summary>Maximum values per column (column name → value).</summary>
    public IReadOnlyDictionary<string, JsonElement>? MaxValues { get; init; }

    /// <summary>Null count per column (column name → count).</summary>
    public IReadOnlyDictionary<string, long>? NullCount { get; init; }
}
