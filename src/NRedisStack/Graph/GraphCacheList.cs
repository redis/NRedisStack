using System.Linq;

namespace NRedisStack.Graph
{
    internal class GraphCacheList
    {
        protected readonly string GraphId;
        protected readonly string Procedure;
        protected readonly GraphCommands RedisGraph;

        private readonly object _locker = new object();

        private string[] _data;

        internal GraphCacheList(string graphId, string procedure, GraphCommands redisGraph)
        {
            GraphId = graphId;
            Procedure = procedure;
            RedisGraph = redisGraph;
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
            RedisGraph.CallProcedure(GraphId, Procedure);
    }

    internal class ReadOnlyGraphCacheList : GraphCacheList
    {
        internal ReadOnlyGraphCacheList(string graphId, string procedure, GraphCommands redisGraph) :
            base(graphId, procedure, redisGraph)
        {
        }

        protected override ResultSet CallProcedure() =>
            RedisGraph.CallProcedureReadOnly(GraphId, Procedure);
    }
}