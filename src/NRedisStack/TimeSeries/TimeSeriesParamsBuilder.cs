using NRedisStack.DataTypes;
using NRedisStack.Extensions;
using NRedisStack.Literals;
using NRedisStack.Literals.Enums;

namespace NRedisStack;

public abstract class TsBaseParamsBuilder<T> where T : TsBaseParamsBuilder<T>
{
    internal protected long? retentionTime;
    internal protected IReadOnlyCollection<TimeSeriesLabel>? labels;
    internal protected long? chunkSizeBytes;
    internal protected long? ignoreMaxTimeDiff;
    internal protected long? ignoreMaxValDiff;


    internal protected double? value;
    internal protected TimeStamp? timestamp;
    internal protected bool? uncompressed;
    internal protected TsDuplicatePolicy? duplicatePolicy;

    internal TsBaseParamsBuilder()
    { }

    public T AddRetentionTime(long retentionTime)
    {
        this.retentionTime = retentionTime;
        return (T)this;
    }
    public T AddLabels(IReadOnlyCollection<TimeSeriesLabel> labels)
    {
        this.labels = labels;
        return (T)this;
    }
    public T AddChunkSizeBytes(long chunkSizeBytes)
    {
        this.chunkSizeBytes = chunkSizeBytes;
        return (T)this;
    }

    public T AddIgnoreValues(long ignoreMaxTimeDiff, long ignoreMaxValDiff)
    {
        this.ignoreMaxTimeDiff = ignoreMaxTimeDiff;
        this.ignoreMaxValDiff = ignoreMaxValDiff;
        return (T)this;
    }

    protected T AddValue(double value)
    {
        this.value = value;
        return (T)this;
    }

    protected T AddTimestamp(TimeStamp timestamp)
    {
        this.timestamp = timestamp;
        return (T)this;
    }

    protected T AddUncompressed(bool uncompressed)
    {
        this.uncompressed = uncompressed;
        return (T)this;
    }

    protected T AddDuplicatePolicy(TsDuplicatePolicy duplicatePolicy)
    {
        this.duplicatePolicy = duplicatePolicy;
        return (T)this;
    }

}

public static class TsParamsHelper
{
    public static void AddRetentionTime(this IList<object> args, long? retentionTime)
    {
        if (retentionTime.HasValue)
        {
            args.Add(TimeSeriesArgs.RETENTION);
            args.Add(retentionTime);
        }
    }

    public static void AddChunkSize(this IList<object> args, long? chunkSize)
    {
        if (chunkSize.HasValue)
        {
            args.Add(TimeSeriesArgs.CHUNK_SIZE);
            args.Add(chunkSize);
        }
    }

    public static void AddLabels(this IList<object> args, IReadOnlyCollection<TimeSeriesLabel>? labels)
    {
        if (labels != null)
        {
            args.Add(TimeSeriesArgs.LABELS);
            foreach (var label in labels)
            {
                args.Add(label.Key);
                args.Add(label.Value);
            }
        }
    }

    public static void AddUncompressed(this IList<object> args, bool? uncompressed)
    {
        if (uncompressed.HasValue)
        {
            args.Add(uncompressed.Value ? TimeSeriesArgs.UNCOMPRESSED : TimeSeriesArgs.COMPRESSED);
        }
    }

    public static void AddIgnoreValues(this IList<object> args, long? ignoreMaxTimeDiff, long? ignoreMaxValDiff)
    {
        if (ignoreMaxTimeDiff != null || ignoreMaxValDiff != null)
        {
            args.Add(TimeSeriesArgs.IGNORE);
            args.Add(ignoreMaxTimeDiff ?? 0);
            args.Add(ignoreMaxValDiff ?? 0);
        }
    }

    public static void AddDuplicatePolicy(this IList<object> args, TsDuplicatePolicy? policy)
    {
        if (policy.HasValue)
        {
            args.Add(TimeSeriesArgs.DUPLICATE_POLICY);
            args.Add(policy.Value.AsArg());
        }
    }

    public static void AddOnDuplicate(this IList<object> args, TsDuplicatePolicy? policy)
    {
        if (policy.HasValue)
        {
            args.Add(TimeSeriesArgs.ON_DUPLICATE);
            args.Add(policy.Value.AsArg());
        }
    }
}

public class TsCreateParamsBuilder : TsBaseParamsBuilder<TsCreateParamsBuilder>
{
    public TsCreateParams build()
    {
        var args = new List<object>();
        args.AddRetentionTime(retentionTime);
        args.AddChunkSize(chunkSizeBytes);
        args.AddLabels(labels);
        args.AddUncompressed(uncompressed);
        args.AddDuplicatePolicy(duplicatePolicy);
        args.AddIgnoreValues(ignoreMaxTimeDiff, ignoreMaxValDiff);
        return new(args);
    }

    public new TsCreateParamsBuilder AddUncompressed(bool uncompressed) => base.AddUncompressed(uncompressed);
    public new TsCreateParamsBuilder AddDuplicatePolicy(TsDuplicatePolicy duplicatePolicy) => base.AddDuplicatePolicy(duplicatePolicy);

}

public class TsAlterParamsBuilder : TsBaseParamsBuilder<TsAlterParamsBuilder>
{
    public TsAlterParams build()
    {
        var args = new List<object>();
        args.AddRetentionTime(retentionTime);
        args.AddChunkSize(chunkSizeBytes);
        args.AddDuplicatePolicy(duplicatePolicy);
        args.AddLabels(labels);
        args.AddIgnoreValues(ignoreMaxTimeDiff, ignoreMaxValDiff);
        return new(args);
    }

    public new TsAlterParamsBuilder AddDuplicatePolicy(TsDuplicatePolicy duplicatePolicy) => base.AddDuplicatePolicy(duplicatePolicy);
}

public class TsAddParamsBuilder : TsBaseParamsBuilder<TsAddParamsBuilder>
{
    public TsAddParams build()
    {
        if (timestamp == null) throw new NotSupportedException("Operation without 'timestamp' is not supported!");
        if (value == null) throw new NotSupportedException("Operation without 'value' is not supported!");

        var args = new List<object> { timestamp.Value.Value, value };
        args.AddRetentionTime(retentionTime);
        args.AddChunkSize(chunkSizeBytes);
        args.AddLabels(labels);
        args.AddUncompressed(uncompressed);
        args.AddOnDuplicate(duplicatePolicy);
        args.AddIgnoreValues(ignoreMaxTimeDiff, ignoreMaxValDiff);
        return new(args);
    }

    public new TsAddParamsBuilder AddValue(double value) => base.AddValue(value);
    public new TsAddParamsBuilder AddTimestamp(TimeStamp timestamp) => base.AddTimestamp(timestamp);
    public new TsAddParamsBuilder AddUncompressed(bool uncompressed) => base.AddUncompressed(uncompressed);
    public TsAddParamsBuilder AddOnDuplicate(TsDuplicatePolicy duplicatePolicy) => AddDuplicatePolicy(duplicatePolicy);
}

public class TsIncrByParamsBuilder : TsBaseParamsBuilder<TsIncrByParamsBuilder>
{
    public TsIncrByParams build()
    {
        if (value == null) throw new NotSupportedException("Operation without 'value' is not supported!");

        var args = new List<object> { value };
        if (timestamp != null) args.AddTimeStamp(timestamp.Value);
        args.AddRetentionTime(retentionTime);
        args.AddChunkSize(chunkSizeBytes);
        if (labels != null) args.AddLabels(labels);
        args.AddUncompressed(uncompressed);
        args.AddIgnoreValues(ignoreMaxTimeDiff, ignoreMaxValDiff);
        return new(args);
    }

    public new TsIncrByParamsBuilder AddValue(double value) => base.AddValue(value);
    public new TsIncrByParamsBuilder AddTimestamp(TimeStamp timestamp) => base.AddTimestamp(timestamp);
    public new TsIncrByParamsBuilder AddUncompressed(bool uncompressed) => base.AddUncompressed(uncompressed);
    public new TsIncrByParamsBuilder AddDuplicatePolicy(TsDuplicatePolicy duplicatePolicy) => base.AddDuplicatePolicy(duplicatePolicy);
}

public class TsDecrByParamsBuilder : TsBaseParamsBuilder<TsDecrByParamsBuilder>
{
    public TsDecrByParams build()
    {
        if (value == null) throw new NotSupportedException("Operation without 'value' is not supported!");

        var args = new List<object> { value };
        if (timestamp != null) args.AddTimeStamp(timestamp.Value);
        args.AddRetentionTime(retentionTime);
        args.AddChunkSize(chunkSizeBytes);
        if (labels != null) args.AddLabels(labels);
        args.AddUncompressed(uncompressed);
        args.AddIgnoreValues(ignoreMaxTimeDiff, ignoreMaxValDiff);
        return new(args);
    }

    public new TsDecrByParamsBuilder AddValue(double value) => base.AddValue(value);
    public new TsDecrByParamsBuilder AddTimestamp(TimeStamp timestamp) => base.AddTimestamp(timestamp);
    public new TsDecrByParamsBuilder AddUncompressed(bool uncompressed) => base.AddUncompressed(uncompressed);
    public new TsDecrByParamsBuilder AddDuplicatePolicy(TsDuplicatePolicy duplicatePolicy) => base.AddDuplicatePolicy(duplicatePolicy);
}