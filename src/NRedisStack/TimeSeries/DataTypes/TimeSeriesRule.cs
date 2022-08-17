using System.Collections.Generic;
using NRedisStack.Literals.Enums;
using NRedisStack.Extensions;

namespace NRedisStack.DataTypes
{
    /// <summary>
    /// A class that represents time-series aggregation rule.
    /// </summary>
    public class TimeSeriesRule
    {
        /// <summary>
        /// Rule's destination key.
        /// </summary>
        public string DestKey { get; private set; }

        /// <summary>
        /// Rule's aggregation time bucket.
        /// </summary>
        public long TimeBucket { get; private set; }

        /// <summary>
        /// Rule's aggregation type.
        /// </summary>
        public TsAggregation Aggregation { get; private set; }

        /// <summary>
        /// Builds a time-series aggregation rule
        /// </summary>
        /// <param name="destKey">Rule's destination key.</param>
        /// <param name="timeBucket">Rule's aggregation time bucket.</param>
        /// <param name="aggregation">Rule's aggregation type.</param>
        public TimeSeriesRule(string destKey, long timeBucket, TsAggregation aggregation) =>
            (DestKey, TimeBucket, Aggregation) = (destKey, timeBucket, aggregation);

        /// <summary>
        /// Equality of TimeSeriesRule objects
        /// </summary>
        /// <param name="obj">Object to compare</param>
        /// <returns>If two TimeSeriesRule objects are equal</returns>
        public override bool Equals(object obj) =>
            obj is TimeSeriesRule rule &&
            DestKey == rule.DestKey &&
            TimeBucket == rule.TimeBucket &&
            Aggregation == rule.Aggregation;

        /// <summary>
        /// Implicit cast from TimeSeriesRule to string.
        /// </summary>
        /// <param name="tsr">TimeSeriesRule</param>
        public static implicit operator string(TimeSeriesRule tsr) =>
            string.Format("DestinationKey: {0}, TimeBucket: {1}, Aggregation: {2}", tsr.DestKey, tsr.TimeBucket, tsr.Aggregation.AsArg());

        /// <summary>
        /// TimeSeriesRule object hash code.
        /// </summary>
        /// <returns>TimeSeriesRule object hash code.</returns>
        public override int GetHashCode()
        {
            var hashCode = 1554951643;
            hashCode = (hashCode * -1521134295) + EqualityComparer<string>.Default.GetHashCode(DestKey);
            hashCode = (hashCode * -1521134295) + TimeBucket.GetHashCode();
            hashCode = (hashCode * -1521134295) + ((int)Aggregation).GetHashCode();
            return hashCode;
        }
    }
}
