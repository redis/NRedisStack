using StackExchange.Redis;

namespace NRedisStack.Tests
{
    public class RedisFixture : IDisposable
    {
        // Set the enviroment variable to specify your own alternet host and port:
        readonly string redisStandalone = Environment.GetEnvironmentVariable("REDIS") ?? "localhost:6379";
        readonly string? redisCluster = Environment.GetEnvironmentVariable("REDIS_CLUSTER");
        readonly string? numRedisClusterNodesEnv = Environment.GetEnvironmentVariable("NUM_REDIS_CLUSTER_NODES");

        public bool isOSSCluster;

        public RedisFixture()
        {
            ConfigurationOptions clusterConfig = new ConfigurationOptions
            {
                AsyncTimeout = 10000,
                SyncTimeout = 10000
            };
            Redis = Connect(clusterConfig, out isOSSCluster);
        }

        public void Dispose()
        {
            Redis.Close();
        }

        public ConnectionMultiplexer Redis { get; }

        public ConnectionMultiplexer CustomRedis(ConfigurationOptions configurationOptions, out bool isOssCluster)
        {
            return Connect(configurationOptions, out isOssCluster);
        }

        private ConnectionMultiplexer Connect(ConfigurationOptions configurationOptions, out bool isOssCluster)
        {
            // Redis Cluster
            if (redisCluster != null && numRedisClusterNodesEnv != null)
            {
                // Split to host and port
                string[] parts = redisCluster!.Split(':');
                string host = parts[0];
                int startPort = int.Parse(parts[1]);

                configurationOptions.EndPoints.Clear();
                int numRedisClusterNodes = int.Parse(numRedisClusterNodesEnv!);
                for (int i = 0; i < numRedisClusterNodes; i++)
                {
                    configurationOptions.EndPoints.Add(host, startPort + i);
                }

                isOssCluster = true;
                return ConnectionMultiplexer.Connect(configurationOptions);
            }

            // Redis Standalone
            configurationOptions.EndPoints.Clear();
            configurationOptions.EndPoints.Add($"{redisStandalone}");

            isOssCluster = false;
            return ConnectionMultiplexer.Connect(configurationOptions);
        }
    }
}