namespace NRedisStack.Graph
{
    internal sealed class GraphCacheList
    {
        private readonly string _graphName;
        private readonly string _procedure;

        private string[] _data = Array.Empty<string>();
        [Obsolete]
        private readonly GraphCommandsAsync _redisGraph;

        private readonly object _locker = new object();

        /// <summary>
        /// Constructs a <see cref="GraphCacheList"/> for providing cached information about the graph.
        /// </summary>
        /// <param name="graphName">The name of the graph to cache information for.</param>
        /// <param name="procedure">The saved procedure to call to populate cache. Must be a `read` procedure.</param>
        /// <param name="redisGraph">The graph used for the calling the <paramref name="procedure"/>.</param>
        [Obsolete]
        internal GraphCacheList(string graphName, string procedure, GraphCommands redisGraph)
        {
            _graphName = graphName;
            _procedure = procedure;

            _redisGraph = redisGraph;
        }

        /// <summary>
        /// Constructs a <see cref="GraphCacheList"/> for providing cached information about the graph.
        /// </summary>
        /// <param name="graphName">The name of the graph to cache information for.</param>
        /// <param name="procedure">The saved procedure to call to populate cache. Must be a `read` procedure.</param>
        /// <param name="redisGraph">The graph used for the calling the <paramref name="procedure"/>.</param>
        [Obsolete]
        internal GraphCacheList(string graphName, string procedure, GraphCommandsAsync redisGraphAsync)
        {
            _graphName = graphName;
            _procedure = procedure;

            _redisGraph = redisGraphAsync;
        }

        // TODO: Change this to use Lazy<T>?
        [Obsolete]
        internal string? GetCachedData(int index)
        {
            if (index >= _data.Length)
            {
                lock (_locker)
                {
                    if (index >= _data.Length)
                    {
                        _data = GetProcedureInfo();
                    }
                }
            }

            return _data.ElementAtOrDefault(index);
        }

        [Obsolete]
        private string[] GetProcedureInfo()
        {
            var resultSet = CallProcedure();
            return resultSet
                .Select(r => r.GetString(0))
                .ToArray();
        }

        [Obsolete]
        private ResultSet CallProcedure()
        {
            return _redisGraph is GraphCommands graphSync
                ? graphSync.CallProcedure(_graphName, _procedure, ProcedureMode.Read)
                : _redisGraph.CallProcedureAsync(_graphName, _procedure, ProcedureMode.Read).Result;
        }
    }
}