namespace NRedisStack.Graph
{
    internal class GraphCacheList
    {
        protected readonly string GraphName;
        protected readonly string Procedure;
        private string[] _data;

        protected readonly GraphCommands graph;
        protected readonly GraphCommandsAsync asyncGraph;
        private bool asyncGraphUsed;

        private readonly object _locker = new object();

        internal GraphCacheList(string graphName, string procedure, GraphCommands redisGraph) : this(graphName, procedure)
        {
            graph = redisGraph;
            asyncGraphUsed = false;
        }

        internal GraphCacheList(string graphName, string procedure, GraphCommandsAsync redisGraph) : this(graphName, procedure)
        {
            asyncGraph = redisGraph;
            asyncGraphUsed = true;
        }

        private GraphCacheList(string graphName, string procedure)
        {
            GraphName = graphName;
            Procedure = procedure;
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
                        GetProcedureInfo();
                    }
                }
            }

            return _data.ElementAtOrDefault(index);
        }

        private void GetProcedureInfo()
        {
            var resultSet = CallProcedure(asyncGraphUsed);
            var newData = new string[resultSet.Count];
            var i = 0;

            foreach (var record in resultSet)
            {
                newData[i++] = record.GetString(0);
            }

            _data = newData;
        }

        protected virtual ResultSet CallProcedure(bool asyncGraphUsed = false)
        {
            return asyncGraphUsed
                ? asyncGraph.CallProcedureAsync(GraphName, Procedure).Result
                : graph.CallProcedure(GraphName, Procedure);
        }
    }
}