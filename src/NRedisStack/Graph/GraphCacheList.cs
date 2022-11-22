namespace NRedisStack.Graph
{
    internal class GraphCacheList
    {
        protected readonly string GraphName;
        protected readonly string Procedure;
        private string[] _data;

        protected readonly GraphCommands graph;

        private readonly object _locker = new object();


        internal GraphCacheList(string graphName, string procedure, GraphCommands redisGraph)
        {
            GraphName = graphName;
            Procedure = procedure;
            graph = redisGraph;
        }

        // TODO: Change this to use Lazy<T>?
        internal string GetCachedData(int index)
        {
            if (_data == null || index >= _data.Length)
            {
                lock(_locker)
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
            var resultSet = CallProcedure();
            var newData = new string[resultSet.Count];
            var i = 0;

            foreach (var record in resultSet)
            {
                newData[i++] = record.GetString(0);
            }

            _data = newData;
        }

        protected virtual ResultSet CallProcedure() =>
            graph.CallProcedure(GraphName, Procedure);
    }

    internal class ReadOnlyGraphCacheList : GraphCacheList
    {
        internal ReadOnlyGraphCacheList(string graphName, string procedure, GraphCommands redisGraph) :
            base(graphName, procedure, redisGraph)
        {
        }

        protected override ResultSet CallProcedure() =>
            graph.CallProcedureReadOnly(GraphName, Procedure);
    }
}