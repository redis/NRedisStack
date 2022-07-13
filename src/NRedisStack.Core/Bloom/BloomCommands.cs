using NRedisStack.Core.Literals;
using StackExchange.Redis;
namespace NRedisStack.Core
{

    public class BloomCommands
    {
        IDatabase _db;
        public BloomCommands(IDatabase db)
        {
            _db = db;
        }


        public RedisResult Add(RedisKey key, string item)
        {
            return _db.Execute(BF.ADD, key, item);
        }
        public RedisResult Exists(RedisKey key, string item)
        {
            return _db.Execute(BF.EXISTS, key, item);
        }

    }
}
