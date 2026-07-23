using NRedisStack.Literals.Enums;

namespace NRedisStack;

/// <summary>
/// Represents zero, one, or many time-series aggregations without always allocating an array.
/// </summary>
public readonly struct TsAggregations : IEquatable<TsAggregations>
{
    private readonly TsAggregation _aggregation;
    private readonly TsAggregation[]? _aggregations;

    public TsAggregations(TsAggregation aggregation)
    {
        _aggregation = Encode(aggregation);
        _aggregations = null;
    }

    public TsAggregations(params TsAggregation[] aggregations)
    {
        if (aggregations is null or { Length: 0 })
        {
            _aggregation = default;
            _aggregations = null;
        }
        else if (aggregations.Length == 1)
        {
            _aggregation = Encode(aggregations[0]);
            _aggregations = null;
        }
        else
        {
            _aggregation = default;
            _aggregations = aggregations;
        }
    }

    public bool IsEmpty => _aggregations is null && (int)_aggregation == 0;

    public int Length => _aggregations?.Length ?? (IsEmpty ? 0 : 1);

    public TsAggregation this[int index] => _aggregations is not null
        ? _aggregations[index]
        : index == 0 && !IsEmpty
            ? Decode(_aggregation)
            : throw new IndexOutOfRangeException();

    public bool Equals(TsAggregations other)
    {
        var length = Length;
        if (length != other.Length)
        {
            return false;
        }

        return length switch
        {
            0 => true,
            1 => _aggregation == other._aggregation,
            _ => _aggregations!.SequenceEqual(other._aggregations!),
        };
    }

    public override bool Equals(object? obj) => obj is TsAggregations other && Equals(other);

    public override int GetHashCode()
    {
        return Length switch
        {
            0 => 0,
            1 => _aggregation.GetHashCode(),
            _ => GetSequenceHashCode(),
        };
    }

    public static bool operator ==(TsAggregations left, TsAggregations right) => left.Equals(right);

    public static bool operator !=(TsAggregations left, TsAggregations right) => !left.Equals(right);

    public static implicit operator TsAggregations(TsAggregation aggregation) => new(aggregation);

    public static implicit operator TsAggregations(TsAggregation? aggregation) => aggregation.HasValue
        ? new(aggregation.Value)
        : default;

    public static implicit operator TsAggregations(TsAggregation[] aggregations) => new(aggregations);

    private int GetSequenceHashCode()
    {
        var hash = 17;
        foreach (var aggregation in _aggregations!)
        {
            hash = (hash * 31) + aggregation.GetHashCode();
        }

        return hash;
    }

    private static TsAggregation Encode(TsAggregation aggregation) => (TsAggregation)(~(int)aggregation);

    private static TsAggregation Decode(TsAggregation aggregation) => (TsAggregation)(~(int)aggregation);
}
