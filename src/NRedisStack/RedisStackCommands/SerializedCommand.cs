namespace NRedisStack.RedisStackCommands;

public class SerializedCommand(string command, params object[] args)
{
    public string Command { get; } = command;
    public object[] Args { get; } = args;

    public SerializedCommand(string command, ICollection<object> args) : this(command, args.ToArray())
    {
    }

    /// <inheritdoc />
    public override string ToString() => Args is { Length: > 0 }
        ? (Command + " " + string.Join(" ", Args))
        : Command;
}