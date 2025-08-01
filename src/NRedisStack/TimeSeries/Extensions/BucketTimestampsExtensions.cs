using NRedisStack.Literals.Enums;

namespace NRedisStack.Extensions;

internal static class TsBucketTimestampsExtensions
{
    public static string AsArg(this TsBucketTimestamps bt) => bt switch
    {
        TsBucketTimestamps.low => "-",
        TsBucketTimestamps.mid => "~",
        TsBucketTimestamps.high => "+",
        _ => throw new ArgumentOutOfRangeException(nameof(bt), "Invalid TsBucketTimestamps type"),
    };
}