namespace NRedisStack.RedisStackCommands
{
    public class SerializedCommand
    {
        public string Command { get; }
        public object[] Args { get; }
        public RequestPolicy Policy { get; set; } = RequestPolicy.Default;

        public SerializedCommand(string command, params object[] args)
        {
            Command = command;
            Args = args;
        }

        public SerializedCommand(string command, RequestPolicy policy, params object[] args)
        {
            Command = command;
            Args = args;
            Policy = policy;
        }

        public SerializedCommand(string command, ICollection<object> args, RequestPolicy policy = RequestPolicy.Default)
        {
            Command = command;
            Args = args.ToArray();
            Policy = policy;
        }
    }
}