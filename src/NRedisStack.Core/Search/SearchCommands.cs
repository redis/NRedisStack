using NRedisStack.Core.Literals;
using StackExchange.Redis;
namespace NRedisStack.Core
{
    public class SearchCommands
    {
        IDatabase _db;
        public SearchCommands(IDatabase db)
        {
            _db = db;
        }
        public RedisResult Info(string index)
        {
            return _db.Execute(FT.INFO, index);
        }
    }
}