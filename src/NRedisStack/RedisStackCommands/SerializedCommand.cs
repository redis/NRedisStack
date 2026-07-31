using StackExchange.Redis;

namespace NRedisStack.RedisStackCommands;

public class SerializedCommand
{
    public string Command { get; }
    public object[] Args { get; }

    /// <summary>
    /// The side-effect category of this command, telling StackExchange.Redis whether it may be replayed
    /// by <c>WithRetry</c>. Only the retry-category bits and the server-specific bit are honoured; other
    /// <see cref="CommandFlags"/> values are masked off before dispatch.
    /// </summary>
    /// <remarks>
    /// <see cref="CommandFlags.None"/> means "uncategorized", which StackExchange.Redis treats as
    /// never-retry.
    /// </remarks>
    public CommandFlags CommandCategory { get; }

#if DEBUG
    private const CommandFlags BaseFlags = CommandFlags.NoRedirect; // disable redirect, so we spot -MOVED in tests
#else
    private const CommandFlags BaseFlags = CommandFlags.None;
#endif

    /// <summary>
    /// The flags to dispatch this command with: the library-wide defaults combined with
    /// <see cref="CommandCategory"/>.
    /// </summary>
    internal CommandFlags EffectiveFlags => BaseFlags | CommandCategory;

    [Obsolete("Specify the command's side-effect category, so that it can participate in WithRetry; uncategorized commands are never retried.")]
    public SerializedCommand(string command, params object[] args)
        : this(CommandFlags.None, command, args)
    {
    }

    [Obsolete("Specify the command's side-effect category, so that it can participate in WithRetry; uncategorized commands are never retried.")]
    public SerializedCommand(string command, ICollection<object> args)
        : this(CommandFlags.None, command, args.ToArray())
    {
    }

    public SerializedCommand(CommandFlags category, string command, params object[] args)
    {
        CommandCategory = category & CommandCategories.Mask;
        Command = command;
        Args = args;
    }

    public SerializedCommand(CommandFlags category, string command, ICollection<object> args)
        : this(category, command, args.ToArray())
    {
    }

    /// <inheritdoc />
    public override string ToString() => Args is { Length: > 0 }
        ? (Command + " " + string.Join(" ", Args))
        : Command;
}
