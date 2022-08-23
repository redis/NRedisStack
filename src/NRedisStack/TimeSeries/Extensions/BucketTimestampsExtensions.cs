using System;
using NRedisStack.Literals.Enums;

namespace NRedisStack.Extensions
{
    internal static class TsBucketTimestampsExtensions
    {
        public static string AsArg(this TsBucketTimestamps bt) => bt switch
        {
            TsBucketTimestamps.low => "low",
            TsBucketTimestamps.mid => "mid",
            TsBucketTimestamps.high => "high",
            _ => throw new ArgumentOutOfRangeException(nameof(bt), "Invalid TsBucketTimestamps type"),
        };

        public static TsBucketTimestamps Asbt(string bt) => bt switch
        {
            "low" => TsBucketTimestamps.low,
            "mid" => TsBucketTimestamps.mid,
            "high" => TsBucketTimestamps.high,
            _ => throw new ArgumentOutOfRangeException(nameof(bt), $"Invalid TsBucketTimestamps type '{bt}'"),
        };
    }
}