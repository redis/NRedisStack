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

        public RedisResult Info(RedisKey key)
        {
            return _db.Execute(BF.INFO, key);
        }

        public RedisResult Insert(RedisKey key)
        {
            return _db.Execute(BF.INFO, key);
        }

        public RedisResult ScanDump(RedisKey key, int iterator)
        {
            return _db.Execute(BF.SCANDUMP, key, iterator);
        }

        public RedisResult LoadChunk(RedisKey key, int iterator, string data)
        {
            return _db.Execute(BF.INFO, key, iterator, data);
        }

    }
}
