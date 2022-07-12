using StackExchange.Redis;
using System;
using System.Collections.Generic;
using NRedisStack.Core.Commands;
using NRedisStack.Core.Commands.Enums;
using NRedisStack.Core.DataTypes;
namespace NRedisStack.Core
{
    public class TimeSeriesCommands
    {
        IDatabase _db;
        public TimeSeriesCommands(IDatabase db)
        {
            _db = db;
        }

        public bool Create(string key, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel> labels = null, bool? uncompressed = null, long? chunkSizeBytes = null, TsDuplicatePolicy? duplicatePolicy = null)
        {
            var args = TimeSeriesAux.BuildTsCreateArgs(key, retentionTime, labels, uncompressed, chunkSizeBytes, duplicatePolicy);
            return ResponseParser.ParseBoolean(_db.Execute(TS.CREATE, args));
        }

        public TimeSeriesInformation Info(string key)
        {
            return ResponseParser.ParseInfo(_db.Execute(TS.INFO, key));
        }

        public bool TimeSeriesAlter(string key, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel> labels = null)
        {
            var args = TimeSeriesAux.BuildTsAlterArgs(key, retentionTime, labels);
            return ResponseParser.ParseBoolean(_db.Execute(TS.ALTER, args));
        }

    }


}



