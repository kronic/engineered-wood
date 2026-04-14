namespace EngineeredWood.DeltaLake;

/// <summary>
/// Thrown when a Delta Lake table contains data that violates the protocol
/// or cannot be interpreted by this implementation.
/// </summary>
public class DeltaFormatException : Exception
{
    public DeltaFormatException(string message) : base(message) { }

    public DeltaFormatException(string message, Exception innerException)
        : base(message, innerException) { }
}
