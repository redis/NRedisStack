using System;
using StackExchange.Redis;

namespace NRedisStack.Tests
{
    public class RedisFixture : IDisposable
    {
        private readonly IDatabase _db; // TODO: Check if its right to have this here

        public RedisFixture() => Redis = ConnectionMultiplexer.Connect("localhost");

        public void Dispose()
        {
            Redis.Close();
        }

        public ConnectionMultiplexer Redis { get; private set; }

        /// <summary>
        /// Executes the contained commands within the context of a transaction.
        /// </summary>
        /// <param name="commandArgsTuples">each tuple represents a command and
        ///     it's arguments to execute inside a transaction.</param>
        /// <returns>A RedisResult.</returns>
        public RedisResult[] ExecuteInTransaction(Tuple<string, string[]>[] commandArgsTuples)
        {
            var transaction = _db.CreateTransaction();
            var tasks = new List<Task<RedisResult>>();

            foreach (var tuple in commandArgsTuples)
            {
                tasks.Add(transaction.ExecuteAsync(tuple.Item1, tuple.Item2));
            }

            transaction.Execute();
            Task.WhenAll(tasks).Wait();
            return tasks.Select(x => x.Result).ToArray();
        }
    }
}