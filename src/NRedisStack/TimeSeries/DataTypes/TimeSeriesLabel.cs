namespace NRedisStack.DataTypes;

/// <summary>
/// A key-value pair class represetns metadata label of time-series.
/// </summary>
public class TimeSeriesLabel
{
    /// <summary>
    /// Label key
    /// </summary>
    public string Key { get; }

    /// <summary>
    /// Label value
    /// </summary>
    public string Value { get; }

    /// <summary>
    /// Create a new TimeSeriesLabel out of key and value strings.
    /// </summary>
    /// <param name="key">Key string</param>
    /// <param name="value">Value string</param>
    public TimeSeriesLabel(string key, string value) => (Key, Value) = (key, value);

    /// <summary>
    /// Equality of TimeSeriesLabel objects
    /// </summary>
    /// <param name="obj">Object to compare</param>
    /// <returns>If two TimeSeriesLabel objects are equal</returns>
    public override bool Equals(object? obj) =>
        obj is TimeSeriesLabel label &&
        Key == label.Key &&
        Value == label.Value;

    /// <summary>
    /// Implicit cast from TimeSeriesLabel to string.
    /// </summary>
    /// <param name="tsl">TimeSeriesLabel</param>
    public static implicit operator string(TimeSeriesLabel tsl) => string.Format("Key: {0}, Val:{1}", tsl.Key, tsl.Value);

    /// <summary>
    /// TimeSeriesLabel object hash code.
    /// </summary>
    /// <returns>TimeSeriesLabel object hash code.</returns>
    public override int GetHashCode()
    {
        var hashCode = 206514262;
        hashCode = (hashCode * -1521134295) + EqualityComparer<string>.Default.GetHashCode(Key);
        hashCode = (hashCode * -1521134295) + EqualityComparer<string>.Default.GetHashCode(Value);
        return hashCode;
    }
}