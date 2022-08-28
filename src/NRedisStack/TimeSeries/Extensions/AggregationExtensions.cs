using System;
using NRedisStack.Literals.Enums;

namespace NRedisStack.Extensions
{
    internal static class AggregationExtensions
    {
        public static string AsArg(this TsAggregation aggregation) => aggregation switch
        {
            TsAggregation.Avg => "avg",
            TsAggregation.Sum => "sum",
            TsAggregation.Min => "min",
            TsAggregation.Max => "max",
            TsAggregation.Range => "range",
            TsAggregation.Count => "count",
            TsAggregation.First => "first",
            TsAggregation.Last => "last",
            TsAggregation.StdP => "std.p",
            TsAggregation.StdS => "std.s",
            TsAggregation.VarP => "var.p",
            TsAggregation.VarS => "var.s",
            TsAggregation.Twa => "twa",
            _ => throw new ArgumentOutOfRangeException(nameof(aggregation), "Invalid aggregation type"),
        };

        public static TsAggregation AsAggregation(string aggregation) => aggregation switch
        {
            "avg" => TsAggregation.Avg,
            "sum" => TsAggregation.Sum,
            "min" => TsAggregation.Min,
            "max" => TsAggregation.Max,
            "range" => TsAggregation.Range,
            "count" => TsAggregation.Count,
            "first" => TsAggregation.First,
            "last" => TsAggregation.Last,
            "std.p" => TsAggregation.StdP,
            "std.s" => TsAggregation.StdS,
            "var.p" => TsAggregation.VarP,
            "var.s" => TsAggregation.VarS,
            "twa" => TsAggregation.Twa,
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
