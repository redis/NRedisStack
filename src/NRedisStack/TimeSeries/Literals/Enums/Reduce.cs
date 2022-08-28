namespace NRedisStack.Literals.Enums
{
    /// <summary>
    /// reducer type used to aggregate series that share the same label value.
    /// </summary>
    public enum TsReduce
    {
        /// <summary>
        /// A sum of all samples in the group
        /// </summary>
        Sum,

        /// <summary>
        /// A minimum sample of all samples in the group
        /// </summary>
        Min,

        /// <summary>
        /// A maximum sample of all samples in the group
        /// </summary>
        Max,
    }
}
