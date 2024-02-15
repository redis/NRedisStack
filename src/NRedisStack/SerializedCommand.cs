namespace NRedisStack.RedisStackCommands
{
    public class SerializedCommand
    {
        public string Command { get; }
        public object[] Args { get; }
        public RequestPolicy Policy { get; set; }

        public SerializedCommand(string command, params object[] args)
        {
            Command = command;
            Args = args;
            Policy = RequestPolicy.Default;
        }

        public SerializedCommand(string command, RequestPolicy policy, params object[] args)
        {
            Command = command;
            Policy = policy;
            Args = args;
        }

        public SerializedCommand(string command, ICollection<object> args)
        {
            Command = command;
            Args = args.ToArray();
            Policy = RequestPolicy.Default;
        }

        public SerializedCommand(string command, RequestPolicy policy, ICollection<object> args)
        {
            Command = command;
            Args = args.ToArray();
            Policy = policy;
        }
    }
}