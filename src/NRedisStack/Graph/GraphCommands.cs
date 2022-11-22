using System.Text;
using NRedisStack.Graph;
using NRedisStack.Literals;
using StackExchange.Redis;
using static NRedisStack.Graph.RedisGraphUtilities;


namespace NRedisStack
{

    public class GraphCommands : IGraphCommands
    {
        IDatabase _db;

        public GraphCommands(IDatabase db)
        {
            _db = db;
        }

        internal static readonly object CompactQueryFlag = "--COMPACT";

        private readonly IDictionary<string, GraphCache> _graphCaches = new Dictionary<string, GraphCache>();

        private GraphCache GetGraphCache(string graphName)
        {
            if (!_graphCaches.ContainsKey(graphName))
            {
                _graphCaches.Add(graphName, new GraphCache(graphName, this));
            }

            return _graphCaches[graphName];
        }

        /// <inheritdoc/>
        public ResultSet Query(string graphName, string query, IDictionary<string, object> parameters, long? timeout = null)
        {
            var preparedQuery = PrepareQuery(query, parameters);

            return Query(graphName, preparedQuery, timeout);
        }

        /// <inheritdoc/>
        public async Task<ResultSet> QueryAsync(string graphName, string query, IDictionary<string, object> parameters, long? timeout = null)
        {
            var preparedQuery = PrepareQuery(query, parameters);

            return await QueryAsync(graphName, preparedQuery, timeout);
        }

        /// <inheritdoc/>
        public ResultSet Query(string graphName, string query, long? timeout = null)
        {
            if (!_graphCaches.ContainsKey(graphName))
            {
                _graphCaches.Add(graphName, new GraphCache(graphName, this));
            }

            var args = (timeout == null) ? new List<object>(3) { graphName, query, CompactQueryFlag }
                                         : new List<object>(5) { graphName, query, CompactQueryFlag, GraphArgs.TIMEOUT, timeout };

            return new ResultSet(_db.Execute(GRAPH.QUERY, args), _graphCaches[graphName]);
        }

        /// <inheritdoc/>
        public async Task<ResultSet> QueryAsync(string graphName, string query, long? timeout = null)
        {
            if (!_graphCaches.ContainsKey(graphName))
            {
                _graphCaches.Add(graphName, new GraphCache(graphName, this));
            }

            var args = (timeout == null) ? new List<object>(3) { graphName, query, CompactQueryFlag }
                                         : new List<object>(5) { graphName, query, CompactQueryFlag, GraphArgs.TIMEOUT, timeout };

            return new ResultSet(await _db.ExecuteAsync(GRAPH.QUERY, args), _graphCaches[graphName]);
        }

        /// <inheritdoc/>
        public ResultSet RO_Query(string graphName, string query, IDictionary<string, object> parameters, long? timeout = null)
        {
            var preparedQuery = PrepareQuery(query, parameters);

            return RO_Query(graphName, preparedQuery, timeout);
        }

        /// <inheritdoc/>
        public async Task<ResultSet> RO_QueryAsync(string graphName, string query, IDictionary<string, object> parameters, long? timeout = null)
        {
            var preparedQuery = PrepareQuery(query, parameters);

            return await RO_QueryAsync(graphName, preparedQuery, timeout);
        }

        /// <inheritdoc/>
        public ResultSet RO_Query(string graphName, string query, long? timeout = null)
        {
            if (!_graphCaches.ContainsKey(graphName))
            {
                _graphCaches.Add(graphName, new GraphCache(graphName, this));
            }

            var args = (timeout == null) ? new List<object>(3) { graphName, query, CompactQueryFlag }
                                         : new List<object>(5) { graphName, query, CompactQueryFlag, GraphArgs.TIMEOUT, timeout };

            return new ResultSet(_db.Execute(GRAPH.RO_QUERY, args), _graphCaches[graphName]);
        }

        /// <inheritdoc/>
        public async Task<ResultSet> RO_QueryAsync(string graphName, string query, long? timeout = null)
        {
            if (!_graphCaches.ContainsKey(graphName))
            {
                _graphCaches.Add(graphName, new GraphCache(graphName, this));
            }

            var args = (timeout == null) ? new List<object>(3) { graphName, query, CompactQueryFlag }
                                         : new List<object>(5) { graphName, query, CompactQueryFlag, GraphArgs.TIMEOUT, timeout };

            return new ResultSet(await _db.ExecuteAsync(GRAPH.RO_QUERY, args), _graphCaches[graphName]);
        }

        internal static readonly Dictionary<string, List<string>> EmptyKwargsDictionary =
            new Dictionary<string, List<string>>();

        // TODO: Check if needed
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
        public ResultSet Delete(string graphName)
        {
            var result = _db.Execute(GRAPH.DELETE, graphName);

            var processedResult = new ResultSet(result, _graphCaches[graphName]);

            _graphCaches.Remove(graphName);

            return processedResult;
        }

        /// <inheritdoc/>
        public async Task<ResultSet> DeleteAsync(string graphName)
        {
            var result = await _db.ExecuteAsync(GRAPH.DELETE, graphName);

            var processedResult = new ResultSet(result, _graphCaches[graphName]);

            _graphCaches.Remove(graphName);

            return processedResult;
        }

        // TODO: Check if this (CallProcedure) is needed
        /// <inheritdoc/>
        public ResultSet CallProcedureReadOnly(string graphName, string procedure) =>
        CallProcedureReadOnly(graphName, procedure, Enumerable.Empty<string>(), EmptyKwargsDictionary);

        /// <inheritdoc/>
        public ResultSet CallProcedureReadOnly(string graphName, string procedure, IEnumerable<string> args) =>
        CallProcedureReadOnly(graphName, procedure, args, EmptyKwargsDictionary);

        /// <inheritdoc/>
        public ResultSet CallProcedureReadOnly(string graphName, string procedure, IEnumerable<string> args, Dictionary<string, List<string>> kwargs)
        {
            args = args.Select(a => QuoteString(a));

            var queryBody = new StringBuilder();

            queryBody.Append($"CALL {procedure}({string.Join(",", args)})");

            if (kwargs.TryGetValue("y", out var kwargsList))
            {
                queryBody.Append(string.Join(",", kwargsList));
            }

            return RO_Query(graphName, queryBody.ToString());
        }

        /// <inheritdoc/>
        public IReadOnlyList<string> Explain(string graphName, string query)
        {
            return _db.Execute(GRAPH.EXPLAIN, graphName, query).ToStringList();
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<string>> ExplainAsync(string graphName, string query)
        {
            return (await _db.ExecuteAsync(GRAPH.EXPLAIN, graphName, query)).ToStringList();
        }

        /// <inheritdoc/>
        public IReadOnlyList<string> Profile(string graphName, string query, long? timeout = null)
        {
            var args = (timeout == null) ? new List<object>(2) { graphName, query }
                                         : new List<object>(4) { graphName, query, GraphArgs.TIMEOUT, timeout };

            return _db.Execute(GRAPH.PROFILE, args).ToStringList();
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<string>> ProfileAsync(string graphName, string query, long? timeout = null)
        {
            var args = (timeout == null) ? new List<object>(2) { graphName, query }
                                         : new List<object>(4) { graphName, query, GraphArgs.TIMEOUT, timeout };

            return (await _db.ExecuteAsync(GRAPH.PROFILE, args)).ToStringList();
        }

        /// <inheritdoc/>
        public IReadOnlyList<string> List()
        {
            return _db.Execute(GRAPH.LIST).ToStringList();
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<string>> ListAsync()
        {
            return (await _db.ExecuteAsync(GRAPH.LIST)).ToStringList();
        }

        /// <inheritdoc/>
        public bool ConfigSet(string configName, object value)
        {
            return _db.Execute(GRAPH.CONFIG, GraphArgs.SET, configName, value).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> ConfigSetAsync(string configName, object value)
        {
            return (await _db.ExecuteAsync(GRAPH.CONFIG, GraphArgs.SET, configName, value)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public Dictionary<string, RedisResult> ConfigGet(string configName)
        {
            return _db.Execute(GRAPH.CONFIG, GraphArgs.GET, configName).ToDictionary();
        }

        /// <inheritdoc/>
        public async Task<Dictionary<string, RedisResult>> ConfigGetAsync(string configName)
        {
            return (await _db.ExecuteAsync(GRAPH.CONFIG, GraphArgs.GET, configName)).ToDictionary();
        }

        /// <inheritdoc/>
        public List<List<string>> Slowlog(string graphName)
        {
            var result = _db.Execute(GRAPH.SLOWLOG, graphName).ToArray();
            List<List<string>> slowlog = new List<List<string>>(result.Length);
            foreach (var item in result)
            {
                slowlog.Add(item.ToStringList());
            }

            return slowlog;
        }

        /// <inheritdoc/>
        public async Task<List<List<string>>> SlowlogAsync(string graphName)
        {
            var result = (await _db.ExecuteAsync(GRAPH.SLOWLOG, graphName)).ToArray();
            List<List<string>> slowlog = new List<List<string>>(result.Length);
            foreach (var item in result)
            {
                slowlog.Add(item.ToStringList());
            }

            return slowlog;
        }
    }
}