using StackExchange.Redis;
using System.Text.Json;

namespace NRedisStack.Tests;

public class EndpointConfig
{
    public List<string>? endpoints { get; set; }

    public bool tls { get; set; }

    public string? password { get; set; }

    public int? bdb_id { get; set; }

    public object? raw_endpoints { get; set; }

    public ConnectionMultiplexer CreateConnection(ConfigurationOptions configurationOptions)
    {
        configurationOptions.EndPoints.Clear();

        foreach (var endpoint in endpoints!)
        {
            configurationOptions.EndPoints.Add(endpoint);
        }

        if (password != null)
        {
            configurationOptions.Password = password;
        }

        // TODO(imalinovskiy): Add support for TLS
        // TODO(imalinovskiy): Add support for Discovery/Sentinel API

        return ConnectionMultiplexer.Connect(configurationOptions);
    }
}


public class RedisFixture : IDisposable
{
    // Set the environment variable to specify your own alternate host and port:
    private readonly string redisStandalone = Environment.GetEnvironmentVariable("REDIS") ?? "localhost:6379";
    private readonly string? redisCluster = Environment.GetEnvironmentVariable("REDIS_CLUSTER");
    private readonly string? numRedisClusterNodesEnv = Environment.GetEnvironmentVariable("NUM_REDIS_CLUSTER_NODES");

    private readonly string defaultEndpointId = Environment.GetEnvironmentVariable("REDIS_DEFAULT_ENDPOINT_ID") ?? "standalone";
    private readonly string? redisEndpointsPath = Environment.GetEnvironmentVariable("REDIS_ENDPOINTS_CONFIG_PATH");
    private Dictionary<string, EndpointConfig> redisEndpoints = new();


    public bool isEnterprise = Environment.GetEnvironmentVariable("IS_ENTERPRISE") == "true";
    public bool isOSSCluster;

    private ConnectionMultiplexer redis;
    private ConfigurationOptions defaultConfig;

    public RedisFixture()
    {
        defaultConfig = new ConfigurationOptions
        {
            AsyncTimeout = 10000,
            SyncTimeout = 10000
        };

        if (redisEndpointsPath != null && File.Exists(redisEndpointsPath))
        {
            string json = File.ReadAllText(redisEndpointsPath);
            var parsedEndpoints = JsonSerializer.Deserialize<Dictionary<string, EndpointConfig>>(json);

            redisEndpoints = parsedEndpoints ?? throw new Exception("Failed to parse the Redis endpoints configuration.");
        }
        else
        {
            redisEndpoints.Add("standalone",
                new EndpointConfig { endpoints = new List<string> { redisStandalone } });

            if (redisCluster != null)
            {
                string[] parts = redisCluster!.Split(':');
                string host = parts[0];
                int startPort = int.Parse(parts[1]);

                var endpoints = new List<string>();
                int numRedisClusterNodes = int.Parse(numRedisClusterNodesEnv!);
                for (int i = 0; i < numRedisClusterNodes; i++)
                {
                    endpoints.Add($"{host}:{startPort + i}");
                }

                redisEndpoints.Add("cluster",
                    new EndpointConfig { endpoints = endpoints });

                // Set the default endpoint to the cluster to keep the tests consistent
                defaultEndpointId = "cluster";
                isOSSCluster = true;
            }
        }
    }

    public void Dispose()
    {
        Redis.Close();
    }

    public ConnectionMultiplexer Redis
    {
        get
        {
            redis = redis ?? GetConnectionById(defaultConfig, defaultEndpointId);
            return redis;
        }
    }

    public ConnectionMultiplexer GetConnectionById(ConfigurationOptions configurationOptions, string id)
    {
        if (!redisEndpoints.ContainsKey(id))
        {
            throw new Exception($"The connection with id '{id}' is not configured.");
        }

        return redisEndpoints[id].CreateConnection(configurationOptions);
    }

    public bool IsTargetConnectionExist(string id)
    {
        return redisEndpoints.ContainsKey(id);
    }
}