using StackExchange.Redis;

namespace NRedisStack.Tests.Search;

public static class SearchTestUtils
{
    // Sets the global search-on-timeout policy on every primary. On a cluster the policy must be
    // present on all shards (and the coordinator) for the timeout behaviour to be consistent, so we
    // broadcast rather than target a single node.
    public static void SetSearchOnTimeout(IConnectionMultiplexer muxer, string value)
    {
        foreach (var endpoint in muxer.GetEndPoints())
        {
            var server = muxer.GetServer(endpoint);
            if (!server.IsReplica)
            {
                server.ConfigSet("search-on-timeout", value);
            }
        }
    }
}
