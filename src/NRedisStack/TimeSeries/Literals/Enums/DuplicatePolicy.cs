namespace NRedisStack.Literals.Enums;

/// <summary>
/// Policy to handle duplicate samples.
/// The default policy for database-wide is BLOCK.
/// </summary>
public enum TsDuplicatePolicy
{
    /// <summary>
    /// An error will occur for any out of order sample.
    /// </summary>
    BLOCK,

    /// <summary>
    /// Ignore the new value.
    /// </summary>
    FIRST,

    /// <summary>
    /// Override with latest value.
    /// </summary>
    LAST,

    /// <summary>
    /// Only override if the value is lower than the existing value.
    /// </summary>
    MIN,

    /// <summary>
    /// Only override if the value is higher than the existing value.
    /// </summary>
    MAX,

    /// <summary>
    /// If a previous sample exists, add the new sample to it so that the updated value is equal to (previous + new).
    /// If no previous sample exists, set the updated value equal to the new value.
    /// </summary>
    SUM
}