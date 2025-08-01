using NRedisStack.Literals.Enums;

namespace NRedisStack.Extensions;

internal static class ReduceExtensions
{
    public static string AsArg(this TsReduce reduce) => reduce switch
    {
        TsReduce.Sum => "SUM",
        TsReduce.Min => "MIN",
        TsReduce.Max => "MAX",
        TsReduce.Avg => "AVG",
        TsReduce.Range => "RANGE",
        TsReduce.Count => "COUNT",
        TsReduce.StdP => "STD.P",
        TsReduce.StdS => "STD.S",
        TsReduce.VarP => "VAR.P",
        TsReduce.VarS => "VAR.S",
        _ => throw new ArgumentOutOfRangeException(nameof(reduce), "Invalid Reduce type"),
    };
}