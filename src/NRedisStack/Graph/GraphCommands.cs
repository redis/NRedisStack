using System.Text;
using NRedisStack.Graph;
using StackExchange.Redis;
using static NRedisStack.Graph.RedisGraphUtilities;


namespace NRedisStack
{
    public class GraphCommands : GraphCommandsAsync, IGraphCommands
    {
        IDatabase _db;

        public GraphCommands(IDatabase db) : base(db)
        {
            _db = db;
        }

        private readonly IDictionary<string, GraphCache> _graphCaches = new Dictionary<string, GraphCache>();

        /// <inheritdoc/>
        public ResultSet Query(string graphName, string query, IDictionary<string, object> parameters, long? timeout = null)
        {
            var preparedQuery = PrepareQuery(query, parameters);

            return Query(graphName, preparedQuery, timeout);
        }

        /// <inheritdoc/>
        public ResultSet Query(string graphName, string query, long? timeout = null)
        {
            if (!_graphCaches.ContainsKey(graphName))
            {
                _graphCaches.Add(graphName, new GraphCache(graphName, this));
            }

            return new ResultSet(_db.Execute(GraphCommandBuilder.Query(graphName, query, timeout)), _graphCaches[graphName]);
        }

        /// <inheritdoc/>
        public ResultSet RO_Query(string graphName, string query, IDictionary<string, object> parameters, long? timeout = null)
        {
            var preparedQuery = PrepareQuery(query, parameters);

            return RO_Query(graphName, preparedQuery, timeout);
        }

        /// <inheritdoc/>
        public ResultSet RO_Query(string graphName, string query, long? timeout = null)
        {
            if (!_graphCaches.ContainsKey(graphName))
            {
                _graphCaches.Add(graphName, new GraphCache(graphName, this));
            }

            return new ResultSet(_db.Execute(GraphCommandBuilder.RO_Query(graphName, query, timeout)), _graphCaches[graphName]);
        }

        internal static readonly Dictionary<string, List<string>> EmptyKwargsDictionary =
            new Dictionary<string, List<string>>();

        // TODO: Check if this is needed:
        /// <inheritdoc/>
        public ResultSet CallProcedure(string graphName, string procedure) =>
        CallProcedure(graphName, procedure, Enumerable.Empty<string>(), EmptyKwargsDictionary);

        /// <inheritdoc/>
        public ResultSet CallProcedure(string graphName, string procedure, IEnumerable<string> args) =>
        CallProcedure(graphName, procedure, args, EmptyKwargsDictionary);

        /// <inheritdoc/>
        public ResultSet CallProcedure(string graphName, string procedure, IEnumerable<string> args, Dictionary<string, List<string>> kwargs)
        {
            args = args.Select(a => QuoteString(a));

            var queryBody = new StringBuilder();

            queryBody.Append($"CALL {procedure}({string.Join(",", args)})");

            if (kwargs.TryGetValue("y", out var kwargsList))
            {
                queryBody.Append(string.Join(",", kwargsList));
            }

            return Query(graphName, queryBody.ToString());
        }

        /// <inheritdoc/>
        public bool Delete(string graphName)
        {

            var result = _db.Execute(GraphCommandBuilder.Delete(graphName)).OKtoBoolean();
            // var processedResult = new ResultSet(result, _graphCaches[graphName]);

            _graphCaches.Remove(graphName);

            //return processedResult;
            return result;
        }

        /// <inheritdoc/>
        public IReadOnlyList<string> Explain(string graphName, string query)
        {
            return _db.Execute(GraphCommandBuilder.Explain(graphName, query)).ToStringList();
        }

        /// <inheritdoc/>
        public IReadOnlyList<string> Profile(string graphName, string query, long? timeout = null)
        {
            return _db.Execute(GraphCommandBuilder.Profile(graphName, query, timeout)).ToStringList();
        }

        /// <inheritdoc/>
        public IReadOnlyList<string> List()
        {
            return _db.Execute(GraphCommandBuilder.List()).ToStringList();
        }

        /// <inheritdoc/>
        public bool ConfigSet(string configName, object value)
        {
            return _db.Execute(GraphCommandBuilder.ConfigSet(configName, value)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public Dictionary<string, RedisResult> ConfigGet(string configName)
        {
            return _db.Execute(GraphCommandBuilder.ConfigGet(configName)).ToDictionary();
        }

        /// <inheritdoc/>
        public List<List<string>> Slowlog(string graphName)
        {
            var result = _db.Execute(GraphCommandBuilder.Slowlog(graphName)).ToArray();
            List<List<string>> slowlog = new List<List<string>>(result.Length);
            foreach (var item in result)
            {
                slowlog.Add(item.ToStringList());
            }

            return slowlog;
        }
    }
}