using StackExchange.Redis;
namespace NRedisStack.Core.RedisStackCommands
{
    public  class TimeSeriesCommands
    {
        IDatabase _db;
        public TimeSeriesCommands(IDatabase db)
        {
            _db = db;
        }
        //TODO: INFO, CREATE, ALTER
        /*public static string CREATE => "TS.CREATE";
        public static string ALTER => "TS.ALTER";
        public static string ADD => "TS.ADD";
        public static string MADD => "TS.MADD";
        public static string INCRBY => "TS.INCRBY";
        public static string DECRBY => "TS.DECRBY";
        public static string CREATERULE => "TS.CREATERULE";
        public static string DELETERULE => "TS.DELETERULE";
        public static string RANGE => "TS.RANGE";
        public static string REVRANGE => "TS.REVRANGE";
        public static string MRANGE => "TS.MRANGE";
        public static string MREVRANGE => "TS.MREVRANGE";
        public static string GET => "TS.GET";
        public static string MGET => "TS.MGET";
        public static string INFO => "TS.INFO";
        public static string QUERYINDEX => "TS.QUERYINDEX";*/
    }
}