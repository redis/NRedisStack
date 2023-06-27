using System.Text;
using NRedisStack.Graph;
using NRedisStack.Graph.Literals;
using StackExchange.Redis;
using static NRedisStack.Graph.RedisGraphUtilities;


namespace NRedisStack
{
    public class GraphCommandsAsync : IGraphCommandsAsync
    {
        IDatabaseAsync _db;

        public GraphCommandsAsync(IDatabaseAsync db)
        {
            _db = db;
        }

        private readonly IDictionary<string, GraphCache> _graphCaches = new Dictionary<string, GraphCache>();

        /// <inheritdoc/>
        public async Task<ResultSet> QueryAsync(string graphName, string query, IDictionary<string, object> parameters, long? timeout = null)
        {
            var preparedQuery = PrepareQuery(query, parameters);

            return await QueryAsync(graphName, preparedQuery, timeout);
        }

        /// <inheritdoc/>
        public async Task<ResultSet> QueryAsync(string graphName, string query, long? timeout = null)
        {
            if (!_graphCaches.ContainsKey(graphName))
            {
                _graphCaches.Add(graphName, new GraphCache(graphName, this));
            }

            return new ResultSet(await _db.ExecuteAsync(GraphCommandBuilder.Query(graphName, query, timeout)), _graphCaches[graphName]);
        }

        /// <inheritdoc/>
        public async Task<ResultSet> RO_QueryAsync(string graphName, string query, IDictionary<string, object> parameters, long? timeout = null)
        {
            var preparedQuery = PrepareQuery(query, parameters);

            return await RO_QueryAsync(graphName, preparedQuery, timeout);
        }

        /// <inheritdoc/>
        public async Task<ResultSet> RO_QueryAsync(string graphName, string query, long? timeout = null)
        {
            if (!_graphCaches.ContainsKey(graphName))
            {
                _graphCaches.Add(graphName, new GraphCache(graphName, this));
            }

            return new ResultSet(await _db.ExecuteAsync(GraphCommandBuilder.RO_Query(graphName, query, timeout)), _graphCaches[graphName]);
        }

        internal static readonly Dictionary<string, List<string>> EmptyKwargsDictionary =
            new Dictionary<string, List<string>>();

        // TODO: Check if this is needed:
        /// <inheritdoc/>
        public async Task<ResultSet> CallProcedureAsync(string graphName, string procedure) =>
        await CallProcedureAsync(graphName, procedure, Enumerable.Empty<string>(), EmptyKwargsDictionary);

        /// <inheritdoc/>
        public async Task<ResultSet> CallProcedureAsync(string graphName, string procedure, IEnumerable<string> args) =>
        await CallProcedureAsync(graphName, procedure, args, EmptyKwargsDictionary);

        /// <inheritdoc/>
        public async Task<ResultSet> CallProcedureAsync(string graphName, string procedure, IEnumerable<string> args, Dictionary<string, List<string>> kwargs)
        {
            args = args.Select(a => QuoteString(a));

            var queryBody = new StringBuilder();

            queryBody.Append($"CALL {procedure}({string.Join(",", args)})");

            if (kwargs.TryGetValue("y", out var kwargsList))
            {
                queryBody.Append(string.Join(",", kwargsList));
            }

            return await QueryAsync(graphName, queryBody.ToString());
        }


        /// <inheritdoc/>
        public async Task<bool> DeleteAsync(string graphName)
        {
            var result = (await _db.ExecuteAsync(GraphCommandBuilder.Delete(graphName))).OKtoBoolean();
            _graphCaches.Remove(graphName);
            return result;
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<string>> ExplainAsync(string graphName, string query)
        {
            return (await _db.ExecuteAsync(GRAPH.EXPLAIN, graphName, query)).ToStringList();
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<string>> ProfileAsync(string graphName, string query, long? timeout = null)
        {
            return (await _db.ExecuteAsync(GraphCommandBuilder.Profile(graphName, query, timeout))).ToStringList();
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<string>> ListAsync()
        {
            return (await _db.ExecuteAsync(GraphCommandBuilder.List())).ToStringList();
        }

        /// <inheritdoc/>
        public async Task<bool> ConfigSetAsync(string configName, object value)
        {
            return (await _db.ExecuteAsync(GraphCommandBuilder.ConfigSet(configName, value))).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<Dictionary<string, RedisResult>> ConfigGetAsync(string configName)
        {
            return (await _db.ExecuteAsync(GRAPH.CONFIG, GraphArgs.GET, configName)).ToDictionary();
        }

        /// <inheritdoc/>
        public async Task<List<List<string>>> SlowlogAsync(string graphName)
        {
            var result = (await _db.ExecuteAsync(GraphCommandBuilder.Slowlog(graphName))).ToArray();
            List<List<string>> slowlog = new List<List<string>>(result.Length);
            foreach (var item in result)
            {
                slowlog.Add(item.ToStringList());
            }

            return slowlog;
        }
    }
}