namespace NRedisStack.DataTypes;

/// <summary>
/// A class represents timestamp.
/// Value can be either primitive long, DateTime or one of the strings "-", "+", "*".
/// </summary>
public readonly record struct TimeStamp
{
    internal static TimeStamp Minus => new(WellKnownTimestamp.Minus);
    internal static TimeStamp Plus => new(WellKnownTimestamp.Plus);
    internal static TimeStamp Star => new(WellKnownTimestamp.Star);
    
    
    private enum WellKnownTimestamp : byte
    {
        None,
        Minus,
        Plus,
        Star,
    }

    /// <summary>
    /// TimeStamp value.
    /// </summary>
    public object Value => _constant switch
    {
        WellKnownTimestamp.Minus => "-",
        WellKnownTimestamp.Plus => "+",
        WellKnownTimestamp.Star => "*",
        _ => _value,
    };

    /// <inheritdoc/>
    public override string ToString() => _constant switch
    {
        WellKnownTimestamp.Minus => "-",
        WellKnownTimestamp.Plus => "+",
        WellKnownTimestamp.Star => "*",
        _ => _value.ToString(),
    };

    private readonly WellKnownTimestamp _constant;
    private readonly long _value;

    /// <summary>
    /// Build a TimeStamp from primitive long.
    /// </summary>
    /// <param name="timestamp">long value</param>
    public TimeStamp(long timestamp)
    {
        _value = timestamp;
        _constant = WellKnownTimestamp.None;
    }

    private TimeStamp(WellKnownTimestamp constant)
    {
        _constant = constant;
        _value = 0;
    }

    /// <summary>
    /// Build a TimeStamp from DateTime.
    /// </summary>
    /// <param name="dateTime">DateTime value</param>
    public TimeStamp(DateTime dateTime)
    {
        _value = new DateTimeOffset(dateTime).ToUnixTimeMilliseconds();
        _constant = WellKnownTimestamp.None;
    }

    /// <summary>
    /// Build a TimeStamp from one of the strings "-", "+", "*".
    /// If the string is none of the above a NotSupportedException is thrown.
    /// </summary>
    /// <param name="timestamp">String value</param>
    public TimeStamp(string timestamp)
    {
        switch (timestamp)
        {
            case "-":
                _constant = WellKnownTimestamp.Minus;
                break;
            case "+":
                _constant = WellKnownTimestamp.Plus;
                break;
            case "*":
                _constant = WellKnownTimestamp.Star;
                break;
            default:
                throw new NotSupportedException($"The string {timestamp} cannot be used");
        }
        _value = 0;
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
        ts._constant == WellKnownTimestamp.None ? ts._value : throw new InvalidCastException("Cannot convert string timestamp to long");

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
    public static implicit operator string?(TimeStamp ts) => ts._constant switch
    {
        WellKnownTimestamp.Minus => "-",
        WellKnownTimestamp.Plus => "+",
        WellKnownTimestamp.Star => "*",
        _ => ts._value.ToString(),
    };

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
        -1937169414 + (_value.GetHashCode() ^ _constant.GetHashCode()); // only expect one of these to be non-zero, note
}