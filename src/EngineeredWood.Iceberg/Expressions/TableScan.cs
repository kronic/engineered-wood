using EngineeredWood.Iceberg.Manifest;
using EngineeredWood.IO;

namespace EngineeredWood.Iceberg.Expressions;

/// <summary>
/// Plans a table scan by evaluating filter expressions against file-level statistics
/// to prune files that cannot contain matching rows.
/// </summary>
public sealed class TableScan
{
    private readonly TableMetadata _metadata;
    private readonly ITableFileSystem _fs;
    private Expression _filter = Expression.True;
    private long? _snapshotId;

    /// <summary>
    /// Initializes a new scan against the specified table metadata and file system.
    /// </summary>
    public TableScan(TableMetadata metadata, ITableFileSystem fs)
    {
        _metadata = metadata;
        _fs = fs;
    }

    /// <summary>
    /// Add a filter expression. Multiple filters are ANDed together.
    /// </summary>
    public TableScan Filter(Expression filter)
    {
        _filter = _filter is TrueExpression ? filter : Expressions.And(_filter, filter);
        return this;
    }

    /// <summary>
    /// Use a specific snapshot instead of the current one.
    /// </summary>
    public TableScan UseSnapshot(long snapshotId)
    {
        _snapshotId = snapshotId;
        return this;
    }

    /// <summary>
    /// Plan the scan: returns data files that might contain matching rows,
    /// plus any applicable delete files.
    /// </summary>
    public async ValueTask<ScanResult> PlanFilesAsync(CancellationToken ct = default)
    {
        var effectiveMetadata = _snapshotId is not null
            ? TimeTravel.AtSnapshot(_metadata, _snapshotId.Value)
            : _metadata;

        if (effectiveMetadata.CurrentSnapshotId is null)
            return new ScanResult([], [], 0, 0);

        var snapshot = effectiveMetadata.Snapshots.First(
            s => s.SnapshotId == effectiveMetadata.CurrentSnapshotId);

        if (!await _fs.ExistsAsync(snapshot.ManifestList, ct).ConfigureAwait(false))
            return new ScanResult([], [], 0, 0);

        // Bind the filter to the current schema
        var schema = effectiveMetadata.Schemas.First(
            s => s.SchemaId == effectiveMetadata.CurrentSchemaId);
        var boundFilter = ExpressionBinder.Bind(_filter, schema);

        var manifestList = await ManifestIO.ReadManifestListAsync(_fs, snapshot.ManifestList, ct)
            .ConfigureAwait(false);

        var dataFiles = new List<DataFile>();
        var deleteFiles = new List<DataFile>();
        int totalFilesScanned = 0;
        int filesSkipped = 0;

        foreach (var mle in manifestList)
        {
            if (!await _fs.ExistsAsync(mle.ManifestPath, ct).ConfigureAwait(false))
                continue;

            var entries = await ManifestIO.ReadManifestAsync(_fs, mle.ManifestPath, ct)
                .ConfigureAwait(false);

            foreach (var entry in entries)
            {
                if (entry.Status == ManifestEntryStatus.Deleted)
                    continue;

                totalFilesScanned++;

                // Delete files are always included (they apply to data files)
                if (entry.DataFile.Content != FileContent.Data)
                {
                    deleteFiles.Add(entry.DataFile);
                    continue;
                }

                // Evaluate filter against data file stats
                if (boundFilter is not TrueExpression)
                {
                    var stats = BuildStats(entry.DataFile);
                    if (!ManifestEvaluator.MightMatch(boundFilter, stats))
                    {
                        filesSkipped++;
                        continue;
                    }
                }

                dataFiles.Add(entry.DataFile);
            }
        }

        return new ScanResult(dataFiles, deleteFiles, totalFilesScanned, filesSkipped);
    }

    /// <summary>
    /// Plan the scan against a pre-loaded list of data files (bypasses manifest I/O).
    /// Useful when files are already in memory with column statistics attached.
    /// </summary>
    public ScanResult PlanFiles(IReadOnlyList<DataFile> candidateFiles)
    {
        var schema = _metadata.Schemas.First(
            s => s.SchemaId == _metadata.CurrentSchemaId);
        var boundFilter = ExpressionBinder.Bind(_filter, schema);

        var dataFiles = new List<DataFile>();
        var deleteFiles = new List<DataFile>();
        int totalScanned = 0;
        int skipped = 0;

        foreach (var df in candidateFiles)
        {
            if (df.Content != FileContent.Data)
            {
                deleteFiles.Add(df);
                continue;
            }

            totalScanned++;

            if (boundFilter is not TrueExpression)
            {
                var stats = BuildStats(df);
                if (!ManifestEvaluator.MightMatch(boundFilter, stats))
                {
                    skipped++;
                    continue;
                }
            }

            dataFiles.Add(df);
        }

        return new ScanResult(dataFiles, deleteFiles, totalScanned, skipped);
    }

    private static DataFileStats BuildStats(DataFile df)
    {
        var lowerBounds = new Dictionary<int, LiteralValue>();
        var upperBounds = new Dictionary<int, LiteralValue>();

        if (df.ColumnLowerBounds is not null)
        {
            foreach (var (fieldId, value) in df.ColumnLowerBounds)
                lowerBounds[fieldId] = value;
        }

        if (df.ColumnUpperBounds is not null)
        {
            foreach (var (fieldId, value) in df.ColumnUpperBounds)
                upperBounds[fieldId] = value;
        }

        return new DataFileStats
        {
            RecordCount = df.RecordCount,
            LowerBounds = lowerBounds,
            UpperBounds = upperBounds,
            NullCounts = df.NullValueCounts ?? new Dictionary<int, long>(),
        };
    }
}

/// <summary>
/// Result of a table scan plan, containing the data files that may match the filter
/// and statistics about how many files were pruned.
/// </summary>
/// <param name="DataFiles">Data files that may contain matching rows.</param>
/// <param name="DeleteFiles">Delete files that apply to the matched data files.</param>
/// <param name="TotalFilesScanned">Total number of data files evaluated.</param>
/// <param name="FilesSkipped">Number of data files pruned by the filter.</param>
public sealed record ScanResult(
    IReadOnlyList<DataFile> DataFiles,
    IReadOnlyList<DataFile> DeleteFiles,
    int TotalFilesScanned,
    int FilesSkipped)
{
    /// <summary>Number of data files that matched the filter.</summary>
    public int FilesMatched => DataFiles.Count;
}
