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

        public bool Exists(RedisKey key, string item)
        {
            return _db.Execute(BF.EXISTS, key, item).ToString() == "1";
        }

        public RedisResult Info(RedisKey key)
        {
            return _db.Execute(BF.INFO, key);
        }

        public RedisResult Insert(RedisKey key, RedisValue[] items, int? capacity = null,
                                  double? error = null, int? expansion = null,
                                  bool nocreate = false, bool nonscaling = false) //NOT DONE
        {
            if (items == null)
                throw new ArgumentNullException(nameof(items));

            List<object> args = new List<object> { key };

            if (capacity != null)
            {
                args.Add(BloomArgs.CAPACITY);
                args.Add(capacity);
            }

            if (error != null)
            {
                args.Add(BloomArgs.ERROR);
                args.Add(error);
            }

            if (expansion != null)
            {
                args.Add(BloomArgs.EXPANSION);
                args.Add(expansion);
            }

            if (nocreate)
                args.Add(BloomArgs.NOCREATE);

            if (nonscaling)
                args.Add(BloomArgs.NONSCALING);

            args.Add(BloomArgs.ITEMS);
            foreach (var item in items)
            {
                args.Add(item);
            }

            return _db.Execute(BF.INSERT, args);
        }

        public RedisResult ScanDump(RedisKey key, int iterator)
        {
            return _db.Execute(BF.SCANDUMP, key, iterator);
        }

        public RedisResult LoadChunk(RedisKey key, int iterator, RedisValue data)
        {
            return _db.Execute(BF.LOADCHUNK, key, iterator, data);
        }

    }
}
