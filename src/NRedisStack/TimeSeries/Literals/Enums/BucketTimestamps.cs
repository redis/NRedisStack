namespace NRedisStack.Literals.Enums;

/// <summary>
/// controls how bucket timestamps are reported.
/// </summary>
public enum TsBucketTimestamps
{
    /// <summary>
    /// Timestamp is the start time (default)
    /// </summary>
    low,

    /// <summary>
    /// Timestamp is the mid time (rounded down if not an integer)
    /// </summary>
    mid,

    /// <summary>
    /// Timestamp is the end time
    /// </summary>
    high,
}