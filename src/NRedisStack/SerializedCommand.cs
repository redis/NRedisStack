namespace NRedisStack.RedisStackCommands
{
    public class SerializedCommand
    {
        public SerializedCommand(string command, params object[] args)
        {
            Command = command;
            Args = args;
        }

        public string Command { get; }
        public object[] Args { get; }
    }
}