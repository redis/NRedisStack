using StackExchange.Redis;
namespace NRedisStack.Core.RedisStackCommands
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
            return _db.Execute("BF.ADD", key, item);
        }
        public RedisResult Exists(RedisKey key, string item)
        {
            return _db.Execute("BF.EXISTS", key, item);
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
