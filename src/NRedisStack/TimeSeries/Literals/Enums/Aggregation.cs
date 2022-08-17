namespace NRedisStack.Literals.Enums
{
    /// <summary>
    /// An aggregation type to be used with a time bucket.
    /// </summary>
    public enum TsAggregation
    {
        /// <summary>
        /// The average of all samples in the aggregation
        /// </summary>
        Avg,

        /// <summary>
        /// A sum of all samples in the aggregation
        /// </summary>
        Sum,

        /// <summary>
        /// A minimum sample of all samples in the aggregation
        /// </summary>
        Min,

        /// <summary>
        /// A maximum sample of all samples in the aggregation
        /// </summary>
        Max,

        /// <summary>
        /// A range of the min and max sample of all samples in the aggregation (range r = max-min)
        /// For example if the min sample was 100 and the max was 400, the range aggregation would return 300
        /// </summary>
        Range,

        /// <summary>
        /// The total number of all samples in the aggregation
        /// </summary>
        Count,

        /// <summary>
        /// The first sample in the aggregation
        /// </summary>
        First,

        /// <summary>
        /// The last sample in the aggregation
        /// </summary>
        Last,

        /// <summary>
        /// The standard deviation based on the entire population
        /// The standard deviation is a measure of how widely values are dispersed from the average sample in the aggregation
        /// </summary>
        StdP,

        /// <summary>
        /// The standard deviation based on a sample of the population
        /// The standard deviation is a measure of how widely values are dispersed from the average sample in the aggregation
        /// </summary>
        StdS,

        /// <summary>
        /// The variance based on the entire population
        /// The variance is the average of the squared differences from the mean
        /// </summary>
        VarP,

        /// <summary>
        /// The variance based on a sample of the population
        /// The variance is the average of the squared differences from the mean
        /// </summary>
        VarS,
    }
}
