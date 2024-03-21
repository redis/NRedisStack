namespace NRedisStack;

/// <summary>
/// Determine the shards to send the command in clustering mode
/// </summary>
public enum RequestPolicy
{
    /// <summary>
    /// The command should be executed on an arbitrary shard when it doesn't accept key name arguments.
    /// </summary>
    Default,

    /// <summary>
    /// The command should be executed on all nodes (masters and replicas).
    /// Used for commands that don't accept key name arguments and operate atomically per shard.
    /// Example: CONFIG SET.
    /// </summary>
    AllNodes,

    /// <summary>
    /// The command should be executed on all master shards.
    /// Used for commands that don't accept key name arguments and operate atomically per shard.
    /// Example: DBSIZE.
    /// </summary>
    AllShards,

    /// <summary>
    /// The command should be executed on any master shard.
    /// Useful for commands that should be executed on different master shards to spread the load.
    /// Examples: FT.SEARCH, FT.AGGREGATE, TS.MGET.
    /// </summary>
    AnyShard,

    /// <summary>
    /// The command should be executed on several shards determined by the hash slots of its input key name arguments.
    /// Examples: MSET, MGET, DEL. Note: SUNIONSTORE isn't considered multi_shard.
    /// </summary>
    MultiShard,

    /// <summary>
    /// Indicates a non-trivial form of the client's request policy.
    /// Example: SCAN command.
    /// </summary>
    Special
}