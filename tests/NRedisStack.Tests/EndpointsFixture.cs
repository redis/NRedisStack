using StackExchange.Redis;
using System.Text.Json;
using Xunit;

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


public class EndpointsFixture : IDisposable
{
    public static class Env
    {
        public const string Standalone = "standalone";
        public const string Cluster = "cluster";

        public static IEnumerable<object[]> AllEnvironments()
        {
            yield return new object[] { Standalone };
            yield return new object[] { Cluster };
            // TODO(imalinovskiy): Add support for Sentinel
        }

        public static IEnumerable<object[]> StandaloneOnly()
        {
            yield return new object[] { Standalone };
        }
    }

    private readonly string? redisEndpointsPath = Environment.GetEnvironmentVariable("REDIS_ENDPOINTS_CONFIG_PATH") ?? "endpoints.json";
    private Dictionary<string, EndpointConfig> redisEndpoints = new();

    public static readonly bool IsEnterprise = Environment.GetEnvironmentVariable("IS_ENTERPRISE") == "true";

    public static Version RedisVersion = new Version(Environment.GetEnvironmentVariable("REDIS_VERSION") ?? "0.0.0");

    public EndpointsFixture()
    {
        if (redisEndpointsPath != null && File.Exists(redisEndpointsPath))
        {
            string json = File.ReadAllText(redisEndpointsPath);
            var parsedEndpoints = JsonSerializer.Deserialize<Dictionary<string, EndpointConfig>>(json);

            redisEndpoints = parsedEndpoints ?? throw new Exception("Failed to parse the Redis endpoints configuration.");
        }
        else
        {
            throw new FileNotFoundException("The Redis endpoints configuration file is not found.");
        }
    }

    public void Dispose()
    {
    }

    public ConnectionMultiplexer GetConnectionById(ConfigurationOptions configurationOptions, string id)
    {
        Skip.IfNot(redisEndpoints.ContainsKey(id), $"The connection with id '{id}' is not configured.");

        return redisEndpoints[id].CreateConnection(configurationOptions);
    }

    public bool IsTargetConnectionExist(string id)
    {
        return redisEndpoints.ContainsKey(id);
    }
}