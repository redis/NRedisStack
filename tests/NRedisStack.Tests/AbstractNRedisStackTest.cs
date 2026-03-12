
using NRedisStack.DataTypes;
using System.Runtime.CompilerServices;
using StackExchange.Redis;
using Xunit;

namespace NRedisStack.Tests;

public abstract class AbstractNRedisStackTest : IClassFixture<EndpointsFixture>, IAsyncLifetime
{
    private protected EndpointsFixture EndpointsFixture { get; }
    private readonly ITestOutputHelper? log;

    protected void Log(string message)
    {
        if (log is null) throw new InvalidOperationException("Log is not initialized");
        log.WriteLine(message);
    }

    protected readonly ConfigurationOptions DefaultConnectionConfig = new()
    {
        AsyncTimeout = 10000,
        SyncTimeout = 10000,
        AllowAdmin = true,
    };

    protected internal AbstractNRedisStackTest(EndpointsFixture endpointsFixture, ITestOutputHelper? log = null)
    {
        this.EndpointsFixture = endpointsFixture;
        this.log = log;
    }

    protected ConnectionMultiplexer GetConnection(string endpointId = EndpointsFixture.Env.Standalone, bool shareConnection = true) => EndpointsFixture.GetConnectionById(this.DefaultConnectionConfig, endpointId, shareConnection);

    protected ConnectionMultiplexer GetConnection(ConfigurationOptions configurationOptions, string endpointId = EndpointsFixture.Env.Standalone) => EndpointsFixture.GetConnectionById(configurationOptions, endpointId, false);

    protected IDatabase GetDatabase(string endpointId = EndpointsFixture.Env.Standalone, bool shareConnection = true)
    {
        var redis = GetConnection(endpointId, shareConnection);
        IDatabase db = redis.GetDatabase();
        return db;
    }

    protected IDatabase GetCleanDatabase(string endpointId = EndpointsFixture.Env.Standalone, bool shareConnection = true)
    {
        var redis = GetConnection(endpointId, shareConnection);

        if (endpointId == EndpointsFixture.Env.Cluster)
        {
            foreach (var endPoint in redis.GetEndPoints())
            {
                var server = redis.GetServer(endPoint);

                if (server.IsReplica || !server.IsConnected) continue;

                server.Execute("FLUSHALL");
            }
        }

        IDatabase db = redis.GetDatabase();
        db.Execute("FLUSHALL");
        return db;
    }

    protected void SkipIfTargetConnectionDoesNotExist(string id)
    {
        Assert.SkipUnless(EndpointsFixture.IsTargetConnectionExist(id), $"The connection with id '{id}' is not configured.");
    }

    private List<string> keyNames = [];

    protected internal string CreateKeyName([CallerMemberName] string memberName = "") => CreateKeyNames(1, memberName)[0];

    protected internal string[] CreateKeyNames(int count, [CallerMemberName] string memberName = "")
    {
        if (count < 1) throw new ArgumentOutOfRangeException(nameof(count), "Must be greater than zero.");

        var newKeys = new string[count];
        for (var i = 0; i < count; i++)
        {
            newKeys[i] = $"{GetType().Name}:{memberName}:{i}";
        }

        keyNames.AddRange(newKeys);

        return newKeys;
    }

    protected internal static List<TimeSeriesTuple> ReverseData(List<TimeSeriesTuple> data)
    {
        var tuples = new List<TimeSeriesTuple>(data.Count);
        for (var i = data.Count - 1; i >= 0; i--)
        {
            tuples.Add(data[i]);
        }

        return tuples;
    }

    public ValueTask InitializeAsync() => default;

    public void Dispose()
    {
        //Redis.GetDatabase().ExecuteBroadcast("FLUSHALL");
    }

    public ValueTask DisposeAsync() => default;
    /*{
        var redis = Redis.GetDatabase();
         await redis.KeyDeleteAsync(keyNames.Select(i => (RedisKey)i).ToArray());
        await redis.ExecuteBroadcastAsync("FLUSHALL");
    }*/
}