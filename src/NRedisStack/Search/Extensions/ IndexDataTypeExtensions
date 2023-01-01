using System;
using NRedisStack.Literals.Enums;

namespace NRedisStack.Extensions
{
    internal static class IndexIndexDataType
    {
        public static string AsArg(this IndexDataType dataType) => dataType switch
        {
            IndexDataType.Hash => "HASH",
            IndexDataType.Json => "JSON",
            _ => throw new ArgumentOutOfRangeException(nameof(dataType), "Invalid Index DataType"),
        };

        public static IndexDataType AsDataType(string dataType) => dataType switch
        {
            "HASH" => IndexDataType.Hash,
            "JSON" => IndexDataType.Json,
            _ => throw new ArgumentOutOfRangeException(nameof(dataType), $"Invalid Index DataType '{dataType}'"),
        };
    }
}