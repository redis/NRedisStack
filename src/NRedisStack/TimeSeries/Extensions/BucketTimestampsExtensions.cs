using System;
using NRedisStack.Literals.Enums;

namespace NRedisStack.Extensions
{
    internal static class TsBucketTimestampsExtensions
    {
        public static string AsArg(this TsBucketTimestamps bt) => bt switch
        {
            TsBucketTimestamps.low => "-",
            TsBucketTimestamps.mid => "~",
            TsBucketTimestamps.high => "+",
            _ => throw new ArgumentOutOfRangeException(nameof(bt), "Invalid TsBucketTimestamps type"),
        };

        // TODO: check if this is needed:
        // public static TsBucketTimestamps Asbt(string bt) => bt switch
        // {
        //     "-" => TsBucketTimestamps.low,
        //     "~" => TsBucketTimestamps.mid,
        //     "+" => TsBucketTimestamps.high,
        //     _ => throw new ArgumentOutOfRangeException(nameof(bt), $"Invalid TsBucketTimestamps type '{bt}'"),
        // };
    }
}