using System;
using NRedisStack.Literals.Enums;

namespace NRedisStack.Extensions
{
    internal static class AggregationExtensions
    {
        public static string AsArg(this TsAggregation aggregation) => aggregation switch
        {
            TsAggregation.Avg => "AVG",
            TsAggregation.Sum => "SUM",
            TsAggregation.Min => "MIN",
            TsAggregation.Max => "MAX",
            TsAggregation.Range => "RANGE",
            TsAggregation.Count => "COUNT",
            TsAggregation.First => "FIRST",
            TsAggregation.Last => "LAST",
            TsAggregation.StdP => "STD.P",
            TsAggregation.StdS => "STD.S",
            TsAggregation.VarP => "VAR.P",
            TsAggregation.VarS => "VAR.S",
            TsAggregation.Twa => "TWA",
            _ => throw new ArgumentOutOfRangeException(nameof(aggregation), "Invalid aggregation type"),
        };

        public static TsAggregation AsAggregation(string aggregation) => aggregation switch
        {
            "AVG" => TsAggregation.Avg,
            "SUM" => TsAggregation.Sum,
            "MIN" => TsAggregation.Min,
            "MAX" => TsAggregation.Max,
            "RANGE" => TsAggregation.Range,
            "COUNT" => TsAggregation.Count,
            "FIRST" => TsAggregation.First,
            "LAST" => TsAggregation.Last,
            "STD.P" => TsAggregation.StdP,
            "STD.S" => TsAggregation.StdS,
            "VAR.P" => TsAggregation.VarP,
            "VAR.S" => TsAggregation.VarS,
            "TWA" => TsAggregation.Twa,
            _ => throw new ArgumentOutOfRangeException(nameof(aggregation), $"Invalid aggregation type '{aggregation}'"),
        };
    }
}
