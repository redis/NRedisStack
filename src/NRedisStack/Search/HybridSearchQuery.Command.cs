using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using NRedisStack.Search.Aggregation;
using StackExchange.Redis;

namespace NRedisStack.Search;

[Experimental(Experiments.Server_8_4, UrlFormat = Experiments.UrlFormat)]
public sealed partial class HybridSearchQuery
{
    internal string Command => "FT.HYBRID";

    internal ICollection<object> GetArgs(in RedisKey index, IDictionary<string, object>? parameters)
    {
        var count = GetOwnArgsCount(parameters);
        var args = new List<object>(count + 1);
        args.Add(index);
        AddOwnArgs(args, parameters);
        Debug.Assert(args.Count == count + 1,
            $"Arg count mismatch; check {nameof(GetOwnArgsCount)} ({count}) vs {nameof(AddOwnArgs)} ({args.Count - 1})");
        return args;
    }

    internal int GetOwnArgsCount(IDictionary<string, object>? parameters)
    {
        int count = 0; // note index is not included here
        if (_query is not null)
        {
            count += 2 + (_queryConfig?.GetOwnArgsCount() ?? 0);
        }

        if (_vectorField is not null)
        {
            count += 3 + (_vectorConfig?.GetOwnArgsCount() ?? 0);
        }

        if (_combiner is not null)
        {
            count += 1 + _combiner.GetOwnArgsCount();
            if (_combineScoreAlias != null) count += 2;
        }

        if (_loadFields is not null)
        {
            count += 2 + _loadFields.Length;
        }

        if (_groupByFieldOrFields is not null)
        {
            count += 2;
            if (_groupByFieldOrFields is string[] fields)
            {
                count += fields.Length;
            }
            else
            {
                count += 1; // single string
            }

            if (_groupByReducer is not null)
            {
                count += 3 + _groupByReducer.ArgCount();
            }
        }

        if (_applyExpression is not null)
        {
            count += 4;
        }

        if (_sortByFieldOrFields is not null)
        {
            count += 2;
            switch (_sortByFieldOrFields)
            {
                case string:
                    count += 1;
                    break;
                case string[] strings:
                    count += strings.Length;
                    break;
                case SortedField { Order: SortedField.SortOrder.ASC }:
                    count += 1;
                    break;
                case SortedField field:
                    count += 2;
                    break;
                case SortedField[] fields:
                    foreach (var field in fields)
                    {
                        if (field.Order == SortedField.SortOrder.DESC) count++;
                    }

                    count += fields.Length;
                    break;
            }
        }

        if (_filter is not null) count += 2;

        if (_pagingOffset >= 0) count += 3;

        if (parameters is not null) count += (parameters.Count + 1) * 2;

        if (_explainScore) count++;
        if (_timeout) count++;

        if (_cursorCount >= 0)
        {
            count++;
            if (_cursorCount != 0) count += 2;
            if (_cursorMaxIdle > TimeSpan.Zero) count += 2;
        }

        return count;
    }

    internal void AddOwnArgs(List<object> args, IDictionary<string, object>? parameters)
    {
        if (_query is not null)
        {
            args.Add("SEARCH");
            args.Add(_query);
            _queryConfig?.AddOwnArgs(args);
        }

        if (_vectorField is not null)
        {
            args.Add("VSIM");
            args.Add(_vectorField);
#if NET || NETSTANDARD2_1_OR_GREATER
            args.Add(Convert.ToBase64String(_vectorData.Span));
#else
            if (MemoryMarshal.TryGetArray(_vectorData, out ArraySegment<byte> segment))
            {
                args.Add(Convert.ToBase64String(segment.Array!, segment.Offset, segment.Count));
            }
            else
            {
                var span = _vectorData.Span;
                var oversized = ArrayPool<byte>.Shared.Rent(span.Length);
                span.CopyTo(oversized);
                args.Add(Convert.ToBase64String(oversized, 0, span.Length));
                ArrayPool<byte>.Shared.Return(oversized);
            }
#endif

            _vectorConfig?.AddOwnArgs(args);
        }

        if (_combiner is not null)
        {
            args.Add("COMBINE");
            _combiner.AddOwnArgs(args);

            if (_combineScoreAlias != null)
            {
                args.Add("YIELD_SCORE_AS");
                args.Add(_combineScoreAlias);
            }
        }

        if (_loadFields is not null)
        {
            args.Add("LOAD");
            args.Add(_loadFields.Length);
            args.AddRange(_loadFields);
        }

        if (_groupByFieldOrFields is not null)
        {
            args.Add("GROUPBY");
            switch (_groupByFieldOrFields)
            {
                case string field:
                    args.Add(1);
                    args.Add(field);
                    break;
                case string[] fields:
                    args.Add(fields.Length);
                    args.AddRange(fields);
                    break;
                default:
                    throw new ArgumentException("Invalid group by field or fields");
            }

            if (_groupByReducer is not null)
            {
                args.Add("REDUCE");
                args.Add(_groupByReducer.Name);
                _groupByReducer.SerializeRedisArgs(args); // includes the count
            }
        }

        if (_applyExpression is not null)
        {
            args.Add("APPLY");
            args.Add(_applyExpression);
            args.Add("AS");
            args.Add(_applyAlias!);
        }

        if (_sortByFieldOrFields is not null)
        {
            args.Add("SORTBY");
            switch (_sortByFieldOrFields)
            {
                case string field:
                    args.Add(1);
                    args.Add(field);
                    break;
                case string[] fields:
                    args.Add(fields.Length);
                    args.AddRange(fields);
                    break;
                case SortedField { Order: SortedField.SortOrder.ASC } field:
                    args.Add(1);
                    args.Add(field.FieldName);
                    break;
                case SortedField field:
                    args.Add(2);
                    args.Add(field.FieldName);
                    args.Add("DESC");
                    break;
                case SortedField[] fields:
                    var descCount = 0;
                    foreach (var field in fields)
                    {
                        if (field.Order == SortedField.SortOrder.DESC) descCount++;
                    }

                    args.Add(fields.Length + descCount);
                    foreach (var field in fields)
                    {
                        args.Add(field.FieldName);
                        if (field.Order == SortedField.SortOrder.DESC) args.Add("DESC");
                    }

                    break;
                default:
                    throw new ArgumentException("Invalid sort by field or fields");
            }
        }

        if (_filter is not null)
        {
            args.Add("FILTER");
            args.Add(_filter);
        }

        if (_pagingOffset >= 0)
        {
            args.Add("LIMIT");
            args.Add(_pagingOffset);
            args.Add(_pagingCount);
        }

        if (parameters is not null)
        {
            args.Add("PARAMS");
            args.Add(parameters.Count * 2);
            if (parameters is Dictionary<string, object> typed)
            {
                foreach (var entry in typed) // avoid allocating enumerator
                {
                    args.Add(entry.Key);
                    args.Add(entry.Value);
                }
            }
            else
            {
                foreach (var entry in parameters)
                {
                    args.Add(entry.Key);
                    args.Add(entry.Value);
                }
            }
        }

        if (_explainScore) args.Add("EXPLAINSCORE");
        if (_timeout) args.Add("TIMEOUT");

        if (_cursorCount >= 0)
        {
            args.Add("WITHCURSOR");
            if (_cursorCount != 0)
            {
                args.Add("COUNT");
                args.Add(_cursorCount);
            }

            if (_cursorMaxIdle > TimeSpan.Zero)
            {
                args.Add("MAXIDLE");
                args.Add((long)_cursorMaxIdle.TotalMilliseconds);
            }
        }
    }
}