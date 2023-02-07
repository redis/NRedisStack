using NRedisStack.Literals.Enums;

namespace NRedisStack.Extensions
{
    internal static class ReduceExtensions
    {
        public static string AsArg(this TsReduce reduce) => reduce switch
        {
            TsReduce.Sum => "SUM",
            TsReduce.Min => "MIN",
            TsReduce.Max => "MAX",
            _ => throw new ArgumentOutOfRangeException(nameof(reduce), "Invalid Reduce type"),
        };
    }
}
