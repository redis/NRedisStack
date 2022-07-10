using StackExchange.Redis;
namespace NRedisStack.Core.RedisStackCommands
{
    public static class RedisBloomCommands
    {
        public static RedisResult BfAdd(this IDatabase db, RedisKey key, string item)
        {
            return db.Execute("BF.ADD", key, item);
        }
        public static RedisResult BfExists(this IDatabase db, RedisKey key, string item)
        {
            return db.Execute("BF.EXISTS", key, item);
        }
        /*public static string ADD => "BF.ADD";
        public static string EXISTS => "BF.EXISTS";
        public static string INFO => "BF.INFO";
        public static string INSERT => "BF.INSERT";
        public static string LOADCHUNK => "BF.LOADCHUNK";
        public static string MADD => "BF.MADD";
        public static string MEXISTS => "BF.MEXISTS";
        public static string RESERVE => "BF.RESERVE";
        public static string SCANDUMP => "BF.SCANDUMP";*/
    }
}