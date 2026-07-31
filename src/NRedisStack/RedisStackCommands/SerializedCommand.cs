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

    // the unvalidated path: the obsolete ctors and Uncategorized() deliberately yield None, which the
    // validating ctors reject
    private SerializedCommand(string command, object[] args, CommandFlags category)
    {
        CommandCategory = category & CommandCategories.Mask;
        Command = command;
        Args = args;
    }

    /// <summary>
    /// Builds a command with no declared category, which StackExchange.Redis will never retry. Only for
    /// entry points that take a bare command string and so have nothing to categorize.
    /// </summary>
    internal static SerializedCommand Uncategorized(string command, params object[] args)
        => new(command, args, CommandFlags.None);

    [Obsolete("Specify the command's side-effect category, so that it can participate in WithRetry; uncategorized commands are never retried.")]
    public SerializedCommand(string command, params object[] args)
        : this(command, args, CommandFlags.None)
    {
    }

    [Obsolete("Specify the command's side-effect category, so that it can participate in WithRetry; uncategorized commands are never retried.")]
    public SerializedCommand(string command, ICollection<object> args)
        : this(command, args.ToArray(), CommandFlags.None)
    {
    }

    /// <param name="category">
    /// The command's side-effect category; see the <c>CommandFlags.CommandRetry*</c> values. Must contain
    /// at least one category bit.
    /// </param>
    /// <param name="command">The command name.</param>
    /// <param name="args">The command arguments.</param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// If <paramref name="category"/> contains no category bits.
    /// </exception>
    public SerializedCommand(CommandFlags category, string command, params object[] args)
        : this(command, args, Validate(category))
    {
    }

    /// <inheritdoc cref="SerializedCommand(CommandFlags, string, object[])"/>
    public SerializedCommand(CommandFlags category, string command, ICollection<object> args)
        : this(command, args.ToArray(), Validate(category))
    {
    }

    // A category naming no rung is a mistake worth surfacing rather than silently accepting: it means the
    // caller passed default/None, or only flags that do not belong here (e.g. FireAndForget), or only the
    // server-specific bit - and the result would quietly be "never retry" wearing the costume of a
    // declaration. Tested against the ladder rather than the full mask precisely so that the
    // server-specific bit on its own does not satisfy it.
    private static CommandFlags Validate(CommandFlags category)
        => (category & CommandCategories.LadderMask) != 0
            ? category
            : throw new ArgumentOutOfRangeException(nameof(category), category,
                "No command category specified; expected one of the CommandFlags.CommandRetry* values.");

    /// <inheritdoc />
    public override string ToString() => Args is { Length: > 0 }
        ? (Command + " " + string.Join(" ", Args))
        : Command;
}
