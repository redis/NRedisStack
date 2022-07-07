using NRedisStack.Core.TimeSeries.Commands;

namespace NRedisStack.Core.TimeSeries
{
    /*public static partial class TimeSeriesCommands
    {
        private static void AddRetentionTime(this IList<object> args, long? retentionTime)
        {
            if (retentionTime.HasValue)
            {
                args.Add(CommandArgs.RETENTION);
                args.Add(retentionTime);
            }
        }

        private static void AddChunkSize(this IList<object> args, long? chunkSize)
        {
            if (chunkSize.HasValue)
            {
                args.Add(CommandArgs.CHUNK_SIZE);
                args.Add(chunkSize);
            }
        }

        private static void AddLabels(this IList<object> args, IReadOnlyCollection<TimeSeriesLabel> labels)
        {
            if (labels != null)
            {
                args.Add(CommandArgs.LABELS);
                foreach (var label in labels)
                {
                    args.Add(label.Key);
                    args.Add(label.Value);
                }
            }
        }

        private static void AddUncompressed(this IList<object> args, bool? uncompressed)
        {
            if (uncompressed.HasValue)
            {
                args.Add(CommandArgs.UNCOMPRESSED);
            }
        }

        private static void AddCount(this IList<object> args, long? count)
        {
            if (count.HasValue)
            {
                args.Add(CommandArgs.COUNT);
                args.Add(count.Value);
            }
        }

        private static void AddDuplicatePolicy(this IList<object> args, TsDuplicatePolicy? policy)
        {
            if (policy.HasValue)
            {
                args.Add(CommandArgs.DUPLICATE_POLICY);
                args.Add(policy.Value.AsArg());
            }
        }


        private static void AddOnDuplicate(this IList<object> args, TsDuplicatePolicy? policy)
        {
            if (policy.HasValue)
            {
                args.Add(CommandArgs.ON_DUPLICATE);
                args.Add(policy.Value.AsArg());
            }
        }

        private static void AddAlign(this IList<object> args, TimeStamp align)
        {
            if(align != null)
            {
                args.Add(CommandArgs.ALIGN);
                args.Add(align.Value);
            }
        }

        private static void AddAggregation(this IList<object> args, TsAggregation? aggregation, long? timeBucket)
        {
            if(aggregation != null)
            {
                args.Add(CommandArgs.AGGREGATION);
                args.Add(aggregation.Value.AsArg());
                if (!timeBucket.HasValue)
                {
                    throw new ArgumentException("RANGE Aggregation should have timeBucket value");
                }
                args.Add(timeBucket.Value);
            }
        }

        private static void AddFilters(this List<object> args, IReadOnlyCollection<string> filter)
        {
            if(filter == null || filter.Count == 0)
            search

        prsearch  {
                args.Add(CommandArgs.FILTER_BY_VALUE);
                args.Add(filter.Value.Item1);
                args.Add(filter.Value.Item2);
            }
        }
search
            if(selectLabels != null){
                args.Add(CommandArgs.SELECTEDLABELS);
                foreach(string label in selectLabels){
                    args.Add(label);
                }
            }
        }

        private static void AddGroupby(this IList<object> args, (string groupby, TsReduce reduce)? groupbyTuple)
        {
            if (groupbyTuple.HasValue)
            {
                args.Add(CommandArgs.GROPUBY);
                args.Add(groupbyTuple.Value.groupby);
                args.Add(CommandArgs.REDUCE);
                args.Add(groupbyTuple.Value.reduce.AsArg());
            }
        }

        private static void AddTimeStamp(this IList<object> args, TimeStamp timeStamp)
        {
            if(timeStamp != null)
            {
                args.Add(CommandArgs.TIMESTAMP);
                args.Add(timeStamp.Value);
            }
        }

        private static void AddRule(this IList<object> args, TimeSeriesRule rule)
        {
            args.Add(rule.DestKey);
            args.Add(CommandArgs.AGGREGATION);
            args.Add(rule.Aggregation.AsArg());
            args.Add(rule.TimeBucket);
        }

        private static List<object> BuildTsCreateArgs(string key, long? retentionTime, IReadOnlyCollection<TimeSeriesLabel> labels, bool? uncompressed,
            long? chunkSizeBytes, TsDuplicatePolicy? policy)
        {
            var args = new List<object> {key};
            args.AddRetentionTime(retentionTime);
            args.AddChunkSize(chunkSizeBytes);
            args.AddLabels(labels);
            args.AddUncompressed(uncompressed);
            args.AddDuplicatePolicy(policy);
            return args;
        }

        private static List<object> BuildTsAlterArgs(string key, long? retentionTime, IReadOnlyCollection<TimeSeriesLabel> labels)
        {
            var args = new List<object> {key};
            args.AddRetentionTime(retentionTime);
            args.AddLabels(labels);
            return args;
        }

        private static List<object> BuildTsAddArgs(string key, TimeStamp timestamp, double value, long? retentionTime,
            IReadOnlyCollection<TimeSeriesLabel> labels, bool? uncompressed, long? chunkSizeBytes, TsDuplicatePolicy? policy)
        {
            var args = new List<object> {key, timestamp.Value, value};
            args.AddRetentionTime(retentionTime);
            args.AddChunkSize(chunkSizeBytes);
            args.AddLabels(labels);
            args.AddUncompressed(uncompressed);
            args.AddOnDuplicate(policy);
            return args;
        }

        private static List<object> BuildTsIncrDecrByArgs(string key, double value, TimeStamp timestamp, long? retentionTime,
            IReadOnlyCollection<TimeSeriesLabel> labels, bool? uncompressed, long? chunkSizeBytes)
        {
            var args = new List<object> {key, value};
            args.AddTimeStamp(timestamp);
            args.AddRetentionTime(retentionTime);
            args.AddChunkSize(chunkSizeBytes);
            args.AddLabels(labels);
            args.AddUncompressed(uncompressed);
            return args;
        }
    }*/
}