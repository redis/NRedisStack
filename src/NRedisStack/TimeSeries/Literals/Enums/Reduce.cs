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

        /// <summary>
        /// Arithmetic mean of all non-NaN values (since RedisTimeSeries v1.8)
        /// </summary>
        Avg,

        /// <summary>
        /// Difference between maximum non-NaN value and minimum non-NaN value (since RedisTimeSeries v1.8)
        /// </summary>
        Range,

        /// <summary>
        /// Number of non-NaN values (since RedisTimeSeries v1.8)
        /// </summary>
        Count,

        /// <summary>
        /// Population standard deviation of all non-NaN values (since RedisTimeSeries v1.8)
        /// </summary>
        StdP,

        /// <summary>
        /// Sample standard deviation of all non-NaN values (since RedisTimeSeries v1.8)
        /// </summary>
        StdS,

        /// <summary>
        /// Population variance of all non-NaN values (since RedisTimeSeries v1.8)
        /// </summary>
        VarP,

        /// <summary>
        /// Sample variance of all non-NaN values (since RedisTimeSeries v1.8)
        /// </summary>
        VarS
    }
}
