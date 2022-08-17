using StackExchange.Redis;
using System;
using System.Collections.Generic;
using NRedisStack.Literals;
using NRedisStack.Literals.Enums;
using NRedisStack.DataTypes;
namespace NRedisStack
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
            return ResponseParser.OKtoBoolean(_db.Execute(TS.CREATE, args));
        }

        public TimeSeriesInformation Info(string key)
        {
            return ResponseParser.ToTimeSeriesInfo(_db.Execute(TS.INFO, key));
        }

        public bool TimeSeriesAlter(string key, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel> labels = null)
        {
            var args = TimeSeriesAux.BuildTsAlterArgs(key, retentionTime, labels);
            return ResponseParser.OKtoBoolean(_db.Execute(TS.ALTER, args));
        }

    }


}



