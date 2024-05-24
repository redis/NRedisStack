using NRedisStack.Literals;
using NRedisStack.Literals.Enums;
using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using System.Collections.ObjectModel;

namespace NRedisStack
{

    public abstract class TsBaseParamsBuilder<T> where T : TsBaseParamsBuilder<T>
    {
        internal string key;
        internal long? retentionTime;
        internal IReadOnlyCollection<TimeSeriesLabel>? labels;
        internal long? chunkSizeBytes;
        internal long? ignoreMaxTimeDiff;
        internal long? ignoreMaxValDiff;
        internal TsBaseParamsBuilder() { }

        public T addKey(string key)
        {
            this.key = key;
            return (T)this;
        }
        public T addRetentionTime(long? retentionTime)
        {
            this.retentionTime = retentionTime;
            return (T)this;
        }
        public T addLabels(ReadOnlyCollection<TimeSeriesLabel>? labels)
        {
            this.labels = labels;
            return (T)this;
        }
        public T addChunkSizeBytes(long? chunkSizeBytes)
        {
            this.chunkSizeBytes = chunkSizeBytes;
            return (T)this;
        }

        public T addIgnoreMaxTimeDiff(long? ignoreMaxTimeDiff)
        {
            this.ignoreMaxTimeDiff = ignoreMaxTimeDiff;
            return (T)this;
        }
        public T addIgnoreMaxValDiff(long? ignoreMaxValDiff)
        {
            this.ignoreMaxValDiff = ignoreMaxValDiff;
            return (T)this;
        }
    }

    public class TsCreateParamsBuilder : TsBaseParamsBuilder<TsCreateParamsBuilder>
    {
        private bool? uncompressed;
        private TsDuplicatePolicy? duplicatePolicy;
        public TsCreateParams build()
        {
            if (String.IsNullOrWhiteSpace(key)) throw new NotSupportedException("Operation without a key is not supported!");

            var args = new List<object> { key };
            args.AddRetentionTime(retentionTime);
            args.AddChunkSize(chunkSizeBytes);
            args.AddLabels(labels);
            args.AddUncompressed(uncompressed);
            args.AddDuplicatePolicy(duplicatePolicy);
            args.AddIgnoreValues(ignoreMaxTimeDiff, ignoreMaxValDiff);
            return new TsCreateParams(args.ToArray());
        }
    }

    public class TsAlterParamsBuilder : TsBaseParamsBuilder<TsAlterParamsBuilder>
    {
        private TsDuplicatePolicy? duplicatePolicy;
        public TsAlterParams build()
        {
            if (String.IsNullOrWhiteSpace(key)) throw new NotSupportedException("Operation without a key is not supported!");

            var args = new List<object> { key };
            args.AddRetentionTime(retentionTime);
            args.AddChunkSize(chunkSizeBytes);
            args.AddDuplicatePolicy(duplicatePolicy);
            args.AddLabels(labels);
            args.AddIgnoreValues(ignoreMaxTimeDiff, ignoreMaxValDiff);
            return new TsAlterParams(args.ToArray());
        }
    }

    public class TsAddParamsBuilder : TsBaseParamsBuilder<TsAddParamsBuilder>
    {
        private double value;
        private TimeStamp timestamp;
        private bool? uncompressed;
        private TsDuplicatePolicy? duplicatePolicy;
        public TsAddParams build()
        {
            if (String.IsNullOrWhiteSpace(key)) throw new NotSupportedException("Operation without a key is not supported!");

            var args = new List<object> { key, timestamp.Value, value };
            args.AddRetentionTime(retentionTime);
            args.AddChunkSize(chunkSizeBytes);
            args.AddLabels(labels);
            args.AddUncompressed(uncompressed);
            args.AddOnDuplicate(duplicatePolicy);
            args.AddIgnoreValues(ignoreMaxTimeDiff, ignoreMaxValDiff);
            return new TsAddParams(args.ToArray());
        }
    }

    public class TsIncrByParamsBuilder : TsBaseParamsBuilder<TsIncrByParamsBuilder>
    {
        private double value;
        private TimeStamp? timestamp;
        private bool? uncompressed;
        private TsDuplicatePolicy? duplicatePolicy;

        public TsIncrByParams build()
        {
            if (String.IsNullOrWhiteSpace(key)) throw new NotSupportedException("Operation without a key is not supported!");

            var args = new List<object> { key, value };
            if (timestamp != null) args.AddTimeStamp(timestamp.Value);
            args.AddRetentionTime(retentionTime);
            args.AddChunkSize(chunkSizeBytes);
            if (labels != null) args.AddLabels(labels);
            args.AddUncompressed(uncompressed);
            args.AddIgnoreValues(ignoreMaxTimeDiff, ignoreMaxValDiff);
            return new TsIncrByParams(args.ToArray());
        }
    }

    public class TsDecrByParamsBuilder : TsBaseParamsBuilder<TsDecrByParamsBuilder>
    {
        private double value;
        private TimeStamp? timestamp;
        private bool? uncompressed;
        public TsDecrByParams build()
        {
            if (String.IsNullOrWhiteSpace(key)) throw new NotSupportedException("Operation without a key is not supported!");

            var args = new List<object> { key, value };
            if (timestamp != null) args.AddTimeStamp(timestamp.Value);
            args.AddRetentionTime(retentionTime);
            args.AddChunkSize(chunkSizeBytes);
            if (labels != null) args.AddLabels(labels);
            args.AddUncompressed(uncompressed);
            args.AddIgnoreValues(ignoreMaxTimeDiff, ignoreMaxValDiff);
            return new TsDecrByParams(args.ToArray());
        }
    }

}