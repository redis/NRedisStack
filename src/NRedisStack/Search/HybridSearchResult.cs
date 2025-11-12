using System.Diagnostics.CodeAnalysis;
using StackExchange.Redis;

namespace NRedisStack.Search;

[Experimental(Experiments.Server_8_4, UrlFormat = Experiments.UrlFormat)]
public sealed class HybridSearchResult
{
    private HybridSearchResult() { }
    internal static HybridSearchResult Parse(RedisResult? result)
    {
        if (result is null || result.IsNull) return null!;
        var obj = new HybridSearchResult();
        var len = result.Length / 2;
        if (len > 0)
        {
            int index = 0;
            for (int i = 0; i < len; i++)
            {
                var key = ParseKey(result[index++]);
                if (key is not ResultKey.Unknown)
                {
                    var value = result[index];
                    if (!value.IsNull)
                    {
                        switch (key)
                        {
                            case ResultKey.TotalResults:
                                obj.TotalResults = (long)value;
                                break;
                            case ResultKey.ExecutionTime:
                                obj.ExecutionTime = TimeSpan.FromSeconds((double)value);
                                break;
                            case ResultKey.Warnings when value.Length > 0:
                                var warnings = new string[value.Length];
                                for (int j = 0; j < value.Length; j++)
                                {
                                    warnings[j] = value[j].ToString();
                                }
                                obj.Warnings = warnings;
                                break;
                            case ResultKey.Results when value.Length > 0:
                                var rows = new IReadOnlyDictionary<string, object>[value.Length];
                                for (int j = 0; j < value.Length; j++)
                                {
                                    rows[j] = ParseRow(value[j]);
                                }

                                obj.Results = rows;
                                break;
                        }
                    }
                }

                index++; // move past value
            }
        }
        return obj;

        static ResultKey ParseKey(RedisResult key)
        {
            if (!key.IsNull && key.Resp2Type is ResultType.BulkString or ResultType.SimpleString)
            {
                return key.ToString().ToLowerInvariant() switch
                {
                    "total_results" => ResultKey.TotalResults,
                    "execution_time" => ResultKey.ExecutionTime,
                    "warnings" => ResultKey.Warnings,
                    "results" => ResultKey.Results,
                    _ => ResultKey.Unknown
                };
            }

            return ResultKey.Unknown;
        }
    }

    private static IReadOnlyDictionary<string, object> ParseRow(RedisResult value)
    {
        var arr = (RedisResult[])value!;
        var row = new Dictionary<string, object>(arr.Length / 2);
        for (int i = 0; i < arr.Length; i += 2)
        {
            var key = arr[i].ToString();
            var parsed = ParseValue(arr[i + 1]);
            row.Add(key, parsed);
        }

        return row;
    }

    private static object ParseValue(RedisResult? value)
    {
        if (value is null || value.IsNull) return null!;
        switch (value.Resp2Type) // for now, only use RESP2 types, to avoid unexpected changes
        {
            case ResultType.BulkString:
            case ResultType.SimpleString:
                return value.ToString();
            case ResultType.Integer:
                return (long)value;
            default:
                return value;
        }
    }

    private enum ResultKey
    {
        Unknown,
        TotalResults,
        ExecutionTime,
        Warnings,
        Results,
    }

    public long TotalResults { get; private set; } = -1; // initialize to -1 to indicate not set

    public TimeSpan ExecutionTime { get; private set; }

    public string[] Warnings { get; private set; } = [];

    public IReadOnlyDictionary<string, object>[] Results { get; private set; } = [];
}