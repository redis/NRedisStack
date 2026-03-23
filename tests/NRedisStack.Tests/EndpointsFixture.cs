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

    public static bool IsAtLeast(int major, int minor = 0, int build = 0, int revision = 0)
    {
        var version = RedisVersion;
        // note: watch out for negative numbers (means "undefined") from parsing n-part strings 
        var test = Math.Max(version.Major, 0);
        if (test > major) return true;
        if (test < major) return false;

        // if here, we're a match on major; test minor
        test = Math.Max(version.Minor, 0); // interpret "9" as "9.0"
        if (test > minor) return true;
        if (test < minor) return false;

        // if here, we're a match on minor; test build
        test = Math.Max(version.Build, 0); // interpret "9.0" as "9.0.0"
        if (test > build) return true;
        if (test < build) return false;

        // if here, we're a match on build; test revision
        test = Math.Max(version.Revision, 0); // interpret "9.0.0" as "9.0.0.0"
        return test >= revision;
    }

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
        foreach (var connection in shared.Values)
        {
            connection.Dispose();
        }
        shared.Clear();
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
            if (shareConnection)
            {
                var key = (id, protocol);
                if (!shared.TryAdd(key, connection) && shared.TryGetValue(key, out var existing))
                {
                    // prefer the existing one
                    connection.Dispose();
                    connection = existing;
                }
            }
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