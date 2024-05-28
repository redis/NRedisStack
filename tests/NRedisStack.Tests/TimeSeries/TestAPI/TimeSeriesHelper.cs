using StackExchange.Redis;
using NRedisStack.RedisStackCommands;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TimeSeriesHelper
    {
        public static RedisResult getInfo(IDatabase db, string key, out int j, out int k)
        {
            var cmd = new SerializedCommand("TS.INFO", key);
            RedisResult info = db.Execute(cmd);

            j = -1;
            k = -1;
            for (int i = 0; i < info.Length; i++)
            {
                if (info[i].ToString().Equals("ignoreMaxTimeDiff")) j = i;
                if (info[i].ToString().Equals("ignoreMaxValDiff")) k = i;
            }
            return info;
        }
    }
}
