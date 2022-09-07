using NRedisStack.Literals;
using StackExchange.Redis;
namespace NRedisStack
{
    public class CreateCommand
    {
        IDatabase _db;
        public CreateCommand(IDatabase db)
        {
            _db = db;
        }

        /// <summary>
        /// Create an index with the given specification.
        /// </summary>
        /// <returns>Array with index names.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft._list"/></remarks>
        // public RedisResult[] Create()
        // {
        //     return _db.Execute(FT._LIST).ToArray();
        // }
    }
}