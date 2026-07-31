using NRedisStack.DataTypes;
using NRedisStack.Literals.Enums;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;

namespace NRedisStack;

public class TsBaseParams
{
    protected IList<object> parameters;

    internal TsBaseParams()
    {
        parameters = new List<object>();
    }

    internal TsBaseParams(IList<object> parameters)
    {
        this.parameters = parameters;
    }

    internal object[] ToArray(RedisKey key)
    {
        parameters.Insert(0, key);
        return parameters.ToArray();
    }
}

public class TsCreateParams : TsBaseParams
{
    internal TsCreateParams(IList<object> parameters) : base(parameters) { }

    internal TsCreateParams(long? retentionTime, IReadOnlyCollection<TimeSeriesLabel>? labels, bool? uncompressed,
        long? chunkSizeBytes, TsDuplicatePolicy? policy)
    {
        parameters.AddRetentionTime(retentionTime);
        parameters.AddChunkSize(chunkSizeBytes);
        parameters.AddLabels(labels);
        parameters.AddUncompressed(uncompressed);
        parameters.AddDuplicatePolicy(policy);
    }
}

public class TsAlterParams : TsBaseParams
{
    internal TsAlterParams(IList<object> parameters) : base(parameters) { }

    internal TsAlterParams(long? retentionTime, long? chunkSizeBytes, TsDuplicatePolicy? policy, IReadOnlyCollection<TimeSeriesLabel>? labels)
    {
        parameters.AddRetentionTime(retentionTime);
        parameters.AddChunkSize(chunkSizeBytes);
        parameters.AddDuplicatePolicy(policy);
        parameters.AddLabels(labels);
    }
}

public class TsAddParams : TsBaseParams
{
    /// <summary>
    /// The side-effect category of the TS.ADD this describes; see <see cref="ResolveCategory"/>.
    /// </summary>
    internal CommandFlags Category { get; }

    internal TsAddParams(IList<object> parameters, CommandFlags category) : base(parameters)
        => Category = category;

    internal TsAddParams(TimeStamp timestamp, double value, long? retentionTime, IReadOnlyCollection<TimeSeriesLabel>? labels, bool? uncompressed, long? chunkSizeBytes, TsDuplicatePolicy? policy)
    {
        parameters.Add(timestamp.Value);
        parameters.Add(value);
        parameters.AddRetentionTime(retentionTime);
        parameters.AddChunkSize(chunkSizeBytes);
        parameters.AddLabels(labels);
        parameters.AddUncompressed(uncompressed);
        parameters.AddOnDuplicate(policy);
        Category = ResolveCategory(timestamp, policy);
    }

    /// <summary>
    /// How a replayed TS.ADD behaves, which depends on the timestamp and on the *effective* duplicate
    /// policy for the sample.
    /// </summary>
    /// <remarks>
    /// Only an explicit <c>ON_DUPLICATE</c> lets us reason about this, because it overrides whatever the
    /// series was created with. Absent one, the series' stored <c>DUPLICATE_POLICY</c> (or the
    /// database-wide default) decides, and that is server-side state we cannot see from here - it could
    /// be <c>SUM</c> - so we have to assume the worst.
    /// </remarks>
    internal static CommandFlags ResolveCategory(TimeStamp timestamp, TsDuplicatePolicy? policy)
    {
        // "*" means the server assigns the timestamp, so each attempt appends a *new* sample
        if (timestamp.IsStar) return CommandCategories.WriteAccumulating;

        return policy switch
        {
            // re-applying the same value at the same timestamp is a no-op under all of these
            TsDuplicatePolicy.LAST or TsDuplicatePolicy.FIRST
                or TsDuplicatePolicy.MIN or TsDuplicatePolicy.MAX => CommandCategories.WriteLastWins,

            // BLOCK deliberately stays out of WriteLastWins: it does not double-apply, but it *errors* on
            // a duplicate, so replaying a write that actually succeeded (and whose reply was merely lost)
            // would surface a spurious error rather than succeeding idempotently. That is worse for the
            // caller than not retrying at all.
            //
            // SUM adds the value again, and null means the series' stored policy governs - which may be SUM.
            _ => CommandCategories.WriteAccumulating,
        };
    }
}

public class TsIncrByParams : TsBaseParams
{
    internal TsIncrByParams(IList<object> parameters) : base(parameters) { }

    internal TsIncrByParams(double value, TimeStamp? timestampMaybe, long? retentionTime,
        IReadOnlyCollection<TimeSeriesLabel>? labels, bool? uncompressed, long? chunkSizeBytes)
    {
        parameters.Add(value);
        if (timestampMaybe is { } timestamp) parameters.AddTimeStamp(timestamp);
        parameters.AddRetentionTime(retentionTime);
        parameters.AddChunkSize(chunkSizeBytes);
        if (labels != null) parameters.AddLabels(labels);
        parameters.AddUncompressed(uncompressed);
    }
}

public class TsDecrByParams : TsIncrByParams
{
    internal TsDecrByParams(IList<object> parameters) : base(parameters) { }

    internal TsDecrByParams(double value, TimeStamp? timestampMaybe, long? retentionTime, IReadOnlyCollection<TimeSeriesLabel>? labels, bool? uncompressed, long? chunkSizeBytes)
        : base(value, timestampMaybe, retentionTime, labels, uncompressed, chunkSizeBytes)
    { }
}