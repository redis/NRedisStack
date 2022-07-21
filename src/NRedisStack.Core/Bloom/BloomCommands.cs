using NRedisStack.Core.Literals;
using StackExchange.Redis;
namespace NRedisStack.Core
{

    public class BloomCommands
    {
        //TODO: Check all the returned value of the commands and change if needed (Can add more parse methods)
        IDatabase _db;
        public BloomCommands(IDatabase db)
        {
            _db = db;
        }

        public bool Add(RedisKey key, RedisValue item)
        {
            return _db.Execute(BF.ADD, key, item).ToString() == "1";
        }

        public bool Exists(RedisKey key, RedisValue item)
        {
            return _db.Execute(BF.EXISTS, key, item).ToString() == "1";
        }

        public RedisResult[]? Info(RedisKey key)
        {
            var info = _db.Execute(BF.INFO, key);
            return ResponseParser.ParseArray(info);
        }

        public RedisResult Insert(RedisKey key, RedisValue[] items, int? capacity = null,
                                  double? error = null, int? expansion = null,
                                  bool nocreate = false, bool nonscaling = false)
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
            {
                args.Add(BloomArgs.NOCREATE);

            }

            if (nonscaling)
            {
                args.Add(BloomArgs.NONSCALING);
            }

            args.Add(BloomArgs.ITEMS);
            foreach (var item in items)
            {
                args.Add(item);
            }

            return _db.Execute(BF.INSERT, args);
        }

        public RedisResult LoadChunk(RedisKey key, int iterator, RedisValue data)
        {
            return _db.Execute(BF.LOADCHUNK, key, iterator, data);
        }

        public bool[] MAdd(RedisKey key, RedisValue[] items)
        {
            if (items == null)
                throw new ArgumentNullException(nameof(items));

            List<object> args = new List<object> { key };

            foreach (var item in items)
            {
                args.Add(item);
            }

            return ResponseParser.ParseBooleanArray(_db.Execute(BF.MADD, args));

        }

        public RedisResult MExists(RedisKey key, RedisValue[] items, int? expansion = null)
        {
            if (items == null)
                throw new ArgumentNullException(nameof(items));

            List<object> args = new List<object> { key };

            foreach (var item in items)
            {
                args.Add(item);
            }

            return _db.Execute(BF.MEXISTS, args);

        }

        public RedisResult Reserve(RedisKey key, double errorRate, long capacity,
                                   int? expansion = null, bool nonscaling = false)
        {
            List<object> args = new List<object> { key, errorRate, capacity };

            if (expansion != null)
            {
                args.Add(expansion);
            }

            if (nonscaling)
            {
                args.Add(BloomArgs.NONSCALING);
            }

            return _db.Execute(BF.RESERVE, args);
        }

        public RedisResult ScanDump(RedisKey key, int iterator)
        {
            return _db.Execute(BF.SCANDUMP, key, iterator);
        }
    }
}
