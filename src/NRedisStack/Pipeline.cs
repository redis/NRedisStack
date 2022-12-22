@@ -0,0 +1,50 @@
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;

namespace NRedisStack
{
    public class Pipeline
    {
        private readonly IDatabase _db;
        private readonly List<SerializedCommand> _commands = new List<SerializedCommand>();

        public Pipeline(IDatabase db)
        {
            _db = db;
        }

        public void AddCommand(SerializedCommand command)
        {
            _commands.Add(command);
        }

        public RedisResult[] Execute()
        {
            var transaction = _db.CreateTransaction();
            var tasks = new List<Task<RedisResult>>();
            foreach (var command in _commands)
            {
                tasks.Add(transaction.ExecuteAsync(command.Command, command.Args));
            }

            transaction.Execute();
            Task.WhenAll(tasks).Wait();
            return tasks.Select(x => x.Result).ToArray();
        }

        public async Task<RedisResult[]> ExecuteAsync()
        {
            var transaction = _db.CreateTransaction();
            var tasks = new List<Task<RedisResult>>();
            foreach (var command in _commands)
            {
                tasks.Add(transaction.ExecuteAsync(command.Command, command.Args));
            }

            transaction.Execute();
            await Task.WhenAll(tasks);
            return tasks.Select(x => x.Result).ToArray();
        }

    }
}