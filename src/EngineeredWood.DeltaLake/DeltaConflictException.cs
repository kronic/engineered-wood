namespace EngineeredWood.DeltaLake;

/// <summary>
/// Thrown when a commit fails due to a concurrent write conflict
/// (the target version already exists).
/// </summary>
public class DeltaConflictException : Exception
{
    public long AttemptedVersion { get; }

    public DeltaConflictException(long attemptedVersion)
        : base($"Commit conflict: version {attemptedVersion} already exists.")
    {
        AttemptedVersion = attemptedVersion;
    }
}
