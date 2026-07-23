using System.Text;

namespace NRedisStack.DataTypes;

/// <summary>
/// A single row of a pivot (outer-join-by-timestamp) reply from <c>TS.NRANGE</c> / <c>TS.NREVRANGE</c>:
/// a timestamp plus one cell per input key, in input-key order.
/// </summary>
public readonly struct TimeSeriesPivotRow
{
    private readonly IReadOnlyList<double>? _values;

    /// <summary>
    /// Create a <see cref="TimeSeriesPivotRow"/>.
    /// </summary>
    /// <param name="timestamp">The row timestamp.</param>
    /// <param name="values">One cell per input key, in input-key order.</param>
    public TimeSeriesPivotRow(TimeStamp timestamp, IReadOnlyList<double> values)
    {
        Timestamp = timestamp;
        _values = values;
    }

    /// <summary>
    /// The row timestamp.
    /// </summary>
    public TimeStamp Timestamp { get; }

    /// <summary>
    /// One value per input key, in input-key order (<c>Values.Count == numkeys</c>). A cell is
    /// <see cref="double.NaN"/> when the corresponding key has no raw sample at the row timestamp, or no data
    /// for the row's aggregation bucket (indistinguishable from a stored NaN) - matching how <c>TS.RANGE</c>
    /// surfaces missing values.
    /// </summary>
    /// <remarks>Never <see langword="null"/>; a <c>default(TimeSeriesPivotRow)</c> reports an empty list.</remarks>
    public IReadOnlyList<double> Values => _values ?? Array.Empty<double>();

    /// <inheritdoc/>
    public override string ToString()
    {
        var values = Values; // coalesced, never null (guards default(TimeSeriesPivotRow))
        var sb = new StringBuilder();
        sb.Append("Time: ").Append(Timestamp.ToString()).Append(", Values:");
        for (int i = 0; i < values.Count; i++)
        {
            if (i != 0) sb.Append(',');
            sb.Append(values[i]);
        }
        return sb.ToString();
    }
}
