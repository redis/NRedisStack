namespace NRedisStack.Core.DataTypes;

/// <summary>
/// Constants for special stream Ids, to be used, for example, with the XREAD and XREADGROUP commands
/// </summary>
public class StreamSpecialIds
{
    /// <summary>
    /// Smallest incomplete ID, can be used for reading from the very first message in a stream.
    /// </summary>
    public const string AllMessagesId = "0";

    /// <summary>
    /// For receiving only new messages that arrive after blocking on a read.
    /// </summary>
    public const string NewMessagesId = "$";

    /// <summary>
    /// For receiving only messages that were never delivered to any other consumer.
    /// </summary>
    public const string UndeliveredMessagesId = ">";
}