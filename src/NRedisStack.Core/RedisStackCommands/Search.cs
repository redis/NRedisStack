using StackExchange.Redis;
namespace NRedisStack.Core.Search
{
    public static class FT
    {
        public static RedisResult FtInfo(this IDatabase db, string index)
        {
            return db.Execute("FT.INFO", index);
        }
    }
}