using StackExchange.Redis;
namespace NRedisStack.Core.RedisStackCommands
{
    public class SearchCommands
    {
        IDatabase _db;
        public SearchCommands(IDatabase db)
        {
            _db = db;
        }
        public RedisResult FtInfo(string index)
        {
            return _db.Execute("FT.INFO", index);
        }
    }
}