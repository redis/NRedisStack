using NRedisStack.DataTypes;
using NRedisStack.Literals.Enums;
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
    internal TsAddParams(IList<object> parameters) : base(parameters) { }

    internal TsAddParams(TimeStamp timestamp, double value, long? retentionTime, IReadOnlyCollection<TimeSeriesLabel>? labels, bool? uncompressed, long? chunkSizeBytes, TsDuplicatePolicy? policy)
    {
        parameters.Add(timestamp.Value);
        parameters.Add(value);
        parameters.AddRetentionTime(retentionTime);
        parameters.AddChunkSize(chunkSizeBytes);
        parameters.AddLabels(labels);
        parameters.AddUncompressed(uncompressed);
        parameters.AddOnDuplicate(policy);
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