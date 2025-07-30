namespace NRedisStack.DataTypes;

/// <summary>
/// A class represents timestamp.
/// Value can be either primitive long, DateTime or one of the strings "-", "+", "*".
/// </summary>
public readonly record struct TimeStamp
{
    private static readonly string[] constants = ["-", "+", "*"];

    /// <summary>
    /// TimeStamp value.
    /// </summary>
    public object Value { get; }

    /// <summary>
    /// Build a TimeStamp from primitive long.
    /// </summary>
    /// <param name="timestamp">long value</param>
    public TimeStamp(long timestamp) => Value = timestamp;

    /// <summary>
    /// Build a TimeStamp from DateTime.
    /// </summary>
    /// <param name="dateTime">DateTime value</param>
    public TimeStamp(DateTime dateTime) => Value = new DateTimeOffset(dateTime).ToUnixTimeMilliseconds();

    /// <summary>
    /// Build a TimeStamp from one of the strings "-", "+", "*".
    /// If the string is none of the above a NotSupportedException is thrown.
    /// </summary>
    /// <param name="timestamp">String value</param>
    public TimeStamp(string timestamp)
    {
        if (Array.IndexOf(constants, timestamp) == -1)
        {
            throw new NotSupportedException($"The string {timestamp} cannot be used");
        }
        Value = timestamp;
    }

    /// <summary>
    /// Implicit cast from long to TimeStamp.
    /// </summary>
    /// <param name="l">long value.</param>
    public static implicit operator TimeStamp(long l) => new(l);

    /// <summary>
    /// Implicit cast from TimeStamp to long.
    /// If the underlying timestamp value is not long or DateTime, an InvalidCastException is thrown.
    /// </summary>
    /// <param name="ts">TimeStamp</param>
    public static implicit operator long(TimeStamp ts) =>
        ts.Value is long value ? value : throw new InvalidCastException("Cannot convert string timestamp to long");

    /// <summary>
    /// Implicit cast from string to TimeStamp.
    /// Calls the string C'tor.
    /// </summary>
    /// <param name="s">String value</param>
    public static implicit operator TimeStamp(string s) => new(s);

    /// <summary>
    /// Implicit cast from TimeStamp to string.
    /// </summary>
    /// <param name="ts">TimeStamp</param>
    public static implicit operator string?(TimeStamp ts) => ts.Value.ToString();

    /// <summary>
    /// Implicit cast from DateTime to TimeStamp.
    /// </summary>
    /// <param name="dateTime">DateTime value</param>
    public static implicit operator TimeStamp(DateTime dateTime) => new(dateTime);

    /// <summary>
    /// Implicit cast from TimeStamp to DateTime.
    /// </summary>
    /// <param name="timeStamp">TimeStamp</param>
    public static implicit operator DateTime(TimeStamp timeStamp) => DateTimeOffset.FromUnixTimeMilliseconds(timeStamp).DateTime;

    /// <summary>
    /// TimeStamp object hash code.
    /// </summary>
    /// <returns>TimeStamp object hash code.</returns>
    public override int GetHashCode() =>
        -1937169414 + EqualityComparer<object>.Default.GetHashCode(Value);
}