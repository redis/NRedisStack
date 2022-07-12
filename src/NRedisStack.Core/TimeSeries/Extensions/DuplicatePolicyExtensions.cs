using System;
using NRedisStack.Core.Commands.Enums;

namespace NRedisStack.Core.Extensions
{
    internal static class DuplicatePolicyExtensions
    {
        public static string AsArg(this TsDuplicatePolicy policy) => policy switch
        {
            TsDuplicatePolicy.BLOCK => "BLOCK",
            TsDuplicatePolicy.FIRST => "FIRST",
            TsDuplicatePolicy.LAST => "LAST",
            TsDuplicatePolicy.MIN => "MIN",
            TsDuplicatePolicy.MAX => "MAX",
            TsDuplicatePolicy.SUM => "SUM",
            _ => throw new ArgumentOutOfRangeException(nameof(policy), "Invalid policy type"),
        };

        public static TsDuplicatePolicy AsPolicy(string policy) => policy switch
        {
            "BLOCK" => TsDuplicatePolicy.BLOCK,
            "FIRST" => TsDuplicatePolicy.FIRST,
            "LAST" => TsDuplicatePolicy.LAST,
            "MIN" => TsDuplicatePolicy.MIN,
            "MAX" => TsDuplicatePolicy.MAX,
            "SUM" => TsDuplicatePolicy.SUM,
            _ => throw new ArgumentOutOfRangeException(nameof(policy), $"Invalid policy type '{policy}'"),
        };
    }
}
