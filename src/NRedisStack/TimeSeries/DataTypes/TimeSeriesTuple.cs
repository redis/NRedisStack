using System.Text;

namespace NRedisStack.DataTypes;

/// <summary>
/// A class represents time-series timestamp-value pair
/// </summary>
public class TimeSeriesTuple(TimeStamp time, double val)
{
    /// <summary>
    /// Tuple key - timestamp.
    /// </summary>
    public TimeStamp Time { get; } = time;

    /// <summary>
    /// Tuple value
    /// </summary>
    public double Val { get; } = val;

    /// <summary>
    /// Equality of TimeSeriesTuple objects
    /// </summary>
    /// <param name="obj">Object to compare</param>
    /// <returns>If two TimeSeriesTuple objects are equal</returns>
    public override bool Equals(object? obj) =>
        obj is TimeSeriesTuple tuple &&
        Time == tuple.Time &&
        // ReSharper disable once CompareOfFloatsByEqualityOperator
        Val == tuple.Val;

    /// <summary>
    /// Implicit cast from TimeSeriesTuple to string.
    /// </summary>
    /// <param name="tst">TimeSeriesTuple</param>
    public static implicit operator string(TimeSeriesTuple tst) => tst.ToString();

    /// <inheritdoc/>
    public override string ToString() => $"Time: {Time.Value}, Val:{Val}";

    /// <summary>
    /// TimeSeriesTuple object hash code.
    /// </summary>
    /// <returns>TimeSeriesTuple object hash code.</returns>
    public override int GetHashCode()
    {
        var hashCode = 459537088;
        hashCode = (hashCode * -1521134295) + Time.GetHashCode();
        hashCode = (hashCode * -1521134295) + Val.GetHashCode();
        return hashCode;
    }

    /// <summary>
    /// When used in a multi-aggregate query, fetch the corresponding aggregate value.
    /// </summary>
    /// <param name="index">The index of the aggregate (relative to the requested aggregates).</param>
    public virtual double this[int index] => index is 0 ? Val : Array.Empty<double>()[0]; // for consistent error

    /// <summary>
    /// Create a <see cref="TimeSeriesTuple"/> from a timestamp and a set of values.
    /// </summary>
    /// <remarks>When a single value is supplied, this is identical to the normal constructor; otherwise,
    /// the individual values are accessible via the indexer.</remarks>
    public static TimeSeriesTuple Create(TimeStamp time, ReadOnlyMemory<double> val)
        => val.Length switch
        {
            0 => new TimeSeriesTuple(time, double.NaN), // GIGO
            1 => new TimeSeriesTuple(time, val.Span[0]),
            _ => new MultiAggregateTimeSeriesTuple(time, val),
        };

    private sealed class MultiAggregateTimeSeriesTuple(TimeStamp time, ReadOnlyMemory<double> values)
        // we need to pass *something* to the default Val; use the first value (Create pre-checks for length)
        : TimeSeriesTuple(time, values.Span[0])
    {
        // this is the main point of this class: to provide access to the other values by overriding the indexer
        public override double this[int index] => values.Span[index];

        /// <inheritdoc/>
        public override string ToString()
        {
            var span = values.Span;
            return span.Length switch
            {
                // these formats are intended to be compar ot elbathe pre-existing base-class format
                0 => $"Time: {Time}, Val:",
                1 => $"Time: {Time}, Val:{Val}",
                2 => $"Time: {Time}, Val:{span[0]},{span[1]}",
                3 => $"Time: {Time}, Val:{span[0]},{span[1]},{span[2]}",
                4 => $"Time: {Time}, Val:{span[0]},{span[1]},{span[2]},{span[3]}",
                _ => BuildToString(Time, span),
            };
            static string BuildToString(TimeStamp time, ReadOnlySpan<double> span)
            {
                var builder = new StringBuilder();
                builder.Append("Time: ").Append(time.ToString()).Append(", Val:").Append(span[0]);
                foreach (var val in span.Slice(1))
                {
                    builder.Append(',').Append(val);
                }
                return builder.ToString();
            }
        }
    }
}