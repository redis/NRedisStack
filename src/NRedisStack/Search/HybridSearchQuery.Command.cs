using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices;
using NRedisStack.Search.Aggregation;
using StackExchange.Redis;

namespace NRedisStack.Search;

public sealed partial class HybridSearchQuery
{
    internal string Command => "FT.HYBRID";

    internal ICollection<object> GetArgs(in RedisKey index, IReadOnlyDictionary<string, object>? parameters)
    {
        var count = GetOwnArgsCount(parameters);
        var args = new List<object>(count + 1);
        args.Add(index);
        AddOwnArgs(args, parameters);
        Debug.Assert(args.Count == count + 1,
            $"Arg count mismatch; check {nameof(GetOwnArgsCount)} ({count}) vs {nameof(AddOwnArgs)} ({args.Count - 1})");
        return args;
    }

    internal int GetOwnArgsCount(IReadOnlyDictionary<string, object>? parameters)
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

        switch (_loadFieldOrFields)
        {
            case string:
                count += 3;
                break;
            case string[] fields:
                count += 2 + fields.Length;
                break;
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

            switch (_reducerOrReducers)
            {
                case Reducer reducer:
                    count += CountReducer(reducer);
                    break;
                case Reducer[] reducers:
                    foreach (var reducer in reducers)
                    {
                        count += CountReducer(reducer);
                    }
                    break;
            }
            static int CountReducer(Reducer reducer) => 3 + reducer.ArgCount() + (reducer.Alias is null ? 0 : 2);
        }

        switch (_applyExpression)
        {
            case string expression:
                count += CountApply(new ApplyExpression(expression));
                break;
            case ApplyExpression applyExpression:
                count += CountApply(applyExpression);
                break;
            case ApplyExpression[] applyExpressions:
                foreach (var applyExpression in applyExpressions)
                {
                    count += CountApply(applyExpression);
                }
                break;
        }

        // ReSharper disable once ConditionIsAlwaysTrueOrFalseAccordingToNullableAPIContract
        static int CountApply(in ApplyExpression expr) => expr.Expression is null ? 0 : (expr.Alias is null ? 2 : 4);

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
                case SortedField:
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

    internal void AddOwnArgs(List<object> args, IReadOnlyDictionary<string, object>? parameters)
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

        switch (_loadFieldOrFields)
        {
            case string field:
                args.Add("LOAD");
                args.Add(1);
                args.Add(field);
                break;
            case string[] fields:
                args.Add("LOAD");
                args.Add(fields.Length);
                args.AddRange(fields);
                break;
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

            switch (_reducerOrReducers)
            {
                case Reducer reducer:
                    AddReducer(reducer, args);
                    break;
                case Reducer[] reducers:
                    foreach (var reducer in reducers)
                    {
                        AddReducer(reducer, args);
                    }
                    break;
            }
            static void AddReducer(Reducer reducer, List<object> args)
            {
                args.Add("REDUCE");
                args.Add(reducer.Name);
                reducer.SerializeRedisArgs(args);
                if (reducer.Alias is not null)
                {
                    args.Add("AS");
                    args.Add(reducer.Alias);
                }
            }
        }

        switch (_applyExpression)
        {
            case string expression:
                AddApply(new ApplyExpression(expression), args);
                break;
            case ApplyExpression applyExpression:
                AddApply(in applyExpression, args);
                break;
            case ApplyExpression[] applyExpressions:
                foreach (var applyExpression in applyExpressions)
                {
                    AddApply(applyExpression, args);
                }
                break;
        }

        static void AddApply(in ApplyExpression expr, List<object> args)
        {
            // ReSharper disable once ConditionIsAlwaysTrueOrFalseAccordingToNullableAPIContract
            if (expr.Expression is not null)
            {
                args.Add("APPLY");
                args.Add(expr.Expression);
                if (expr.Alias is not null)
                {
                    args.Add("AS");
                    args.Add(expr.Alias);
                }
            }
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

    internal void Validate()
    {
        if (_query is null | _vectorField is null)
        {
            throw new InvalidOperationException($"Both the query ({nameof(Query)}(...)) and vector search ({nameof(VectorSimilaritySearch)}(...))) details must be set.");
        }
    }
}