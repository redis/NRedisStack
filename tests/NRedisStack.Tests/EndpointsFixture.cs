using System.Collections.Concurrent;
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
        public const string StandaloneEntraId = "standalone-entraid";
        public const string Cluster = "cluster";

        public static IEnumerable<object[]> AllEnvironments()
        {
            yield return [Standalone];
            yield return [Cluster];
            // TODO(imalinovskiy): Add support for Sentinel
        }

        public static IEnumerable<object[]> StandaloneOnly()
        {
            yield return [Standalone];
        }
    }

    private readonly string? redisEndpointsPath = Environment.GetEnvironmentVariable("REDIS_ENDPOINTS_CONFIG_PATH") ?? "endpoints.json";
    private readonly Dictionary<string, EndpointConfig> redisEndpoints = new();

    public static readonly bool IsEnterprise = Environment.GetEnvironmentVariable("IS_ENTERPRISE") == "true";

    public static Version RedisVersion = new(Environment.GetEnvironmentVariable("REDIS_VERSION") ?? "0.0.0");

    public EndpointsFixture()
    {
        if (redisEndpointsPath != null && File.Exists(redisEndpointsPath))
        {
            string json = File.ReadAllText(redisEndpointsPath);
            var parsedEndpoints = JsonSerializer.Deserialize<Dictionary<string, EndpointConfig>>(json);

            redisEndpoints = parsedEndpoints ?? throw new("Failed to parse the Redis endpoints configuration.");
        }
        else
        {
            throw new FileNotFoundException("The Redis endpoints configuration file is not found.");
        }
    }

    public void Dispose()
    {
    }

    public ConnectionMultiplexer GetConnectionById(ConfigurationOptions configurationOptions, string id, bool shareConnection)
    {
        Assert.SkipUnless(redisEndpoints.ContainsKey(id), $"The connection with id '{id}' is not configured.");

        var protocol = TestContext.Current.GetRunProtocol();

        if (!(shareConnection && shared.TryGetValue((id, protocol), out var connection)))
        {
            var options = configurationOptions.Clone(); // isolate before we start applying the protocol
            options.Protocol = protocol.IsResp3() ? RedisProtocol.Resp3 : RedisProtocol.Resp2;
            options.HighIntegrity = protocol.IsHighIntegrity();
            options.ConnectTimeout = 2000;
            if (shareConnection) options.AbortOnConnectFail = false;
            connection = redisEndpoints[id].CreateConnection(options);
            if (shareConnection) shared[(id, protocol)] = connection;
        }

        Assert.SkipUnless(connection.IsConnected, $"The connection with id '{id}' is not connected.");
        return connection;
    }

    // allow tests to share connections
    ConcurrentDictionary<(string, RunProtocol), ConnectionMultiplexer> shared = new();

    public bool IsTargetConnectionExist(string id)
    {
        return redisEndpoints.ContainsKey(id);
    }
}