namespace NRedisStack.RedisStackCommands
{
    public class SerializedCommand
    {
        public string Command { get; }
        public object[] Args { get; }

        public SerializedCommand(string command, params object[] args)
        {
            Command = command;
            Args = args;
        }

        public SerializedCommand(string command, ICollection<object> args)
        {
            Command = command;
            Args = args.ToArray();
        }
    }
}