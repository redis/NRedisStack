namespace NRedisStack.Graph
{
    internal sealed class GraphCacheList
    {
        private readonly string _graphName;
        private readonly string _procedure;

        private string[]? _data;

        private readonly GraphCommandsAsync _redisGraph;

        private readonly object _locker = new object();

        internal GraphCacheList(string graphName, string procedure, GraphCommands redisGraph)
        {
            _graphName = graphName;
            _procedure = procedure;

            _redisGraph = redisGraph;
        }

        internal GraphCacheList(string graphName, string procedure, GraphCommandsAsync redisGraphAsync)
        {
            _graphName = graphName;
            _procedure = procedure;

            _redisGraph = redisGraphAsync;
        }

        // TODO: Change this to use Lazy<T>?
        internal string GetCachedData(int index)
        {
            if (_data == null || index >= _data.Length)
            {
                lock (_locker)
                {
                    if (_data == null || index >= _data.Length)
                    {
                        _data = GetProcedureInfo();
                    }
                }
            }

            return _data?.ElementAtOrDefault(index) ?? string.Empty;
        }

        private string[] GetProcedureInfo()
        {
            var resultSet = CallProcedure();
            return resultSet
                .Select(r => r.GetString(0))
                .ToArray();
        }

        private ResultSet CallProcedure()
        {
            return _redisGraph is GraphCommands graphSync
                ? graphSync.CallProcedure(_graphName, _procedure, ProcedureMode.Read)
                : _redisGraph.CallProcedureAsync(_graphName, _procedure, ProcedureMode.Read).Result;
        }
    }
}