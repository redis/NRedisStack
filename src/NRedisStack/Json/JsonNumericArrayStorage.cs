namespace NRedisStack;

/// <summary>
/// Specifies the storage type for numeric values when used in a JSON array.
/// </summary>
public enum JsonNumericArrayStorage
{
    /// <summary>
    /// Default behaviour, no FPHA usage.
    /// </summary>
    NotSpecified = 0,

    /// <summary>
    /// "Brain" floating-point 16-bit.
    /// </summary>
    BF16 = 1,

    /// <summary>
    /// IEEE 754 16-bit.
    /// </summary>
    FP16 = 2,

    /// <summary>
    /// IEEE 754 32-bit.
    /// </summary>
    FP32 = 3,

    /// <summary>
    /// IEEE 754 64-bit.
    /// </summary>
    FP64 = 4,
}
