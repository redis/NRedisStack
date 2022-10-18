using System.Collections.ObjectModel;
using System.Text;
using NRedisStack.Graph;
using NRedisStack.Graph.DataTypes;
using NRedisStack.Literals;
using StackExchange.Redis;
using static NRedisStack.Graph.RedisGraphUtilities;


namespace NRedisStack
{

    public class GraphCommands
    {
        IDatabase _db;

        /// <summary>
        /// Creates a RedisGraph client that leverages a specified instance of `IDatabase`.
        /// </summary>
        /// <param name="db"></param>
        public GraphCommands(IDatabase db)
        {
            _db = db;
        }
        /*
        TODO:
        GRAPH.QUERY
        GRAPH.RO_QUERY
        GRAPH.DELETE
        GRAPH.EXPLAIN
        GRAPH.PROFILE
        GRAPH.SLOWLOG
        GRAPH.CONFIG GET
        GRAPH.CONFIG SET
        GRAPH.LIST
        */

        internal static readonly object CompactQueryFlag = "--COMPACT";
        // private readonly IDatabase _db;
        private readonly IDictionary<string, IGraphCache> _graphCaches = new Dictionary<string, IGraphCache>();

        private IGraphCache GetGraphCache(string graphId)
        {
            if (!_graphCaches.ContainsKey(graphId))
            {
                _graphCaches.Add(graphId, new GraphCache(graphId, this));
            }

            return _graphCaches[graphId];
        }

        // /// <summary>
        // /// Execute a Cypher query with parameters.
        // /// </summary>
        // /// <param name="graphId">A graph to perform the query on.</param>
        // /// <param name="query">The Cypher query.</param>
        // /// <param name="parameters">Parameters map.</param>
        // /// <returns>A result set.</returns>
        // public ResultSet GraphQuery(string graphId, string query, IDictionary<string, object> parameters) =>
        //     Query(graphId, query, parameters);

        /// <summary>
        /// Execute a Cypher query with parameters.
        /// </summary>
        /// <param name="graphId">A graph to perform the query on.</param>
        /// <param name="query">The Cypher query.</param>
        /// <param name="parameters">Parameters map.</param>
        /// <param name="timeout">Timeout (optional).</param>
        /// <returns>A result set.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.query"/></remarks>
        public ResultSet Query(string graphId, string query, IDictionary<string, object> parameters, long? timeout = null)
        {
            var preparedQuery = PrepareQuery(query, parameters);

            return Query(graphId, preparedQuery, timeout);
        }

        // /// <summary>
        // /// Execute a Cypher query.
        // /// </summary>
        // /// <param name="graphId">A graph to perform the query on.</param>
        // /// <param name="query">The Cypher query.</param>
        // /// <returns>A result set.</returns>
        // public ResultSet GraphQuery(string graphId, string query) =>
        //     Query(graphId, query);

        /// <summary>
        /// Execute a Cypher query.
        /// </summary>
        /// <param name="graphId">A graph to perform the query on.</param>
        /// <param name="query">The Cypher query.</param>
        /// <param name="timeout">Timeout (optional).</param>
        /// <returns>A result set.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.query"/></remarks>
        public ResultSet Query(string graphId, string query, long? timeout = null)
        {
            _graphCaches.PutIfAbsent(graphId, new GraphCache(graphId, this));

            var args = (timeout == null) ? new List<object> { graphId, query, CompactQueryFlag }
                                         : new List<object> { graphId, query, CompactQueryFlag, GraphArgs.TIMEOUT, timeout };

            return new ResultSet(_db.Execute(GRAPH.QUERY, args), _graphCaches[graphId]);
        }

        /// <summary>
        /// Execute a Cypher query with parameters.
        /// </summary>
        /// <param name="graphId">A graph to perform the query on.</param>
        /// <param name="query">The Cypher query.</param>
        /// <param name="parameters">Parameters map.</param>
        /// <param name="timeout">Timeout (optional).</param>
        /// <returns>A result set.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.query"/></remarks>
        public ResultSet RO_Query(string graphId, string query, IDictionary<string, object> parameters, long? timeout = null)
        {
            var preparedQuery = PrepareQuery(query, parameters);

            return RO_Query(graphId, preparedQuery, timeout);
        }

        /// <summary>
        /// Execute a Cypher query.
        /// </summary>
        /// <param name="graphId">A graph to perform the query on.</param>
        /// <param name="query">The Cypher query.</param>
        /// <param name="timeout">Timeout (optional).</param>
        /// <returns>A result set.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.query"/></remarks>
        public ResultSet RO_Query(string graphId, string query, long? timeout = null)
        {
            _graphCaches.PutIfAbsent(graphId, new GraphCache(graphId, this));

            var args = (timeout == null) ? new List<object> { graphId, query, CompactQueryFlag }
                                         : new List<object> { graphId, query, CompactQueryFlag, GraphArgs.TIMEOUT, timeout };

            return new ResultSet(_db.Execute(GRAPH.RO_QUERY, args), _graphCaches[graphId]);
        }

        // // TODO: Check if this and the "CommandFlags flags" is needed
        // /// <summary>
        // /// Execute a Cypher query, preferring a read-only node.
        // /// </summary>
        // /// <param name="graphId">A graph to perform the query on.</param>
        // /// <param name="query">The Cypher query.</param>
        // /// <param name="parameters">Parameters map.</param>
        // /// <param name="flags">Optional command flags. `PreferReplica` is set for you here.</param>
        // /// <returns>A result set.</returns>
        // public ResultSet RO_Query(string graphId, string query, IDictionary<string, object> parameters, CommandFlags flags = CommandFlags.None)
        // {
        //     var preparedQuery = PrepareQuery(query, parameters);

        //     return RO_Query(graphId, preparedQuery, flags);
        // }

        // /// <summary>
        // /// Execute a Cypher query, preferring a read-only node.
        // /// </summary>
        // /// <param name="graphId">A graph to perform the query on.</param>
        // /// <param name="query">The Cypher query.</param>
        // /// <param name="flags">Optional command flags. `PreferReplica` is set for you here.</param>
        // /// <returns>A result set.</returns>
        // public ResultSet RO_Query(string graphId, string query, CommandFlags flags = CommandFlags.None)
        // {
        //     _graphCaches.PutIfAbsent(graphId, new ReadOnlyGraphCache(graphId, this));

        //     var parameters = new Collection<object>
        //     {
        //         graphId,
        //         query,
        //         CompactQueryFlag
        //     };

        //     var result = _db.Execute(GRAPH.RO_QUERY, parameters, (flags | CommandFlags.PreferReplica));

        //     return new ResultSet(result, _graphCaches[graphId]);
        // }

        internal static readonly Dictionary<string, List<string>> EmptyKwargsDictionary =
            new Dictionary<string, List<string>>();

        /// <summary>
        /// Call a saved procedure.
        /// </summary>
        /// <param name="graphId">The graph containing the saved procedure.</param>
        /// <param name="procedure">The procedure name.</param>
        /// <returns>A result set.</returns>
        public ResultSet CallProcedure(string graphId, string procedure) =>
            CallProcedure(graphId, procedure, Enumerable.Empty<string>(), EmptyKwargsDictionary);

        /// <summary>
        /// Call a saved procedure with parameters.
        /// </summary>
        /// <param name="graphId">The graph containing the saved procedure.</param>
        /// <param name="procedure">The procedure name.</param>
        /// <param name="args">A collection of positional arguments.</param>
        /// <returns>A result set.</returns>
        public ResultSet CallProcedure(string graphId, string procedure, IEnumerable<string> args) =>
            CallProcedure(graphId, procedure, args, EmptyKwargsDictionary);

        /// <summary>
        /// Call a saved procedure with parameters.
        /// </summary>
        /// <param name="graphId">The graph containing the saved procedure.</param>
        /// <param name="procedure">The procedure name.</param>
        /// <param name="args">A collection of positional arguments.</param>
        /// <param name="kwargs">A collection of keyword arguments.</param>
        /// <returns>A result set.</returns>
        public ResultSet CallProcedure(string graphId, string procedure, IEnumerable<string> args, Dictionary<string, List<string>> kwargs)
        {
            args = args.Select(a => QuoteString(a));

            var queryBody = new StringBuilder();

            queryBody.Append($"CALL {procedure}({string.Join(",", args)})");

            if (kwargs.TryGetValue("y", out var kwargsList))
            {
                queryBody.Append(string.Join(",", kwargsList));
            }

            return Query(graphId, queryBody.ToString());
        }

        /// <summary>
        /// Create a RedisGraph transaction.
        ///
        /// This leverages the "Transaction" support present in StackExchange.Redis.
        /// </summary>
        /// <returns></returns>
        public RedisGraphTransaction Multi() =>
            new RedisGraphTransaction(_db.CreateTransaction(), this, _graphCaches);

        /// <summary>
        /// Delete an existing graph.
        /// </summary>
        /// <param name="graphId">The graph to delete.</param>
        /// <returns>A result set.</returns>
        public ResultSet DeleteGraph(string graphId)
        {
            var result = _db.Execute(GRAPH.DELETE, graphId);

            var processedResult = new ResultSet(result, _graphCaches[graphId]);

            _graphCaches.Remove(graphId);

            return processedResult;
        }


        /// <summary>
        /// Call a saved procedure against a read-only node.
        /// </summary>
        /// <param name="graphId">The graph containing the saved procedure.</param>
        /// <param name="procedure">The procedure name.</param>
        /// <returns>A result set.</returns>
        public ResultSet CallProcedureReadOnly(string graphId, string procedure) =>
            CallProcedureReadOnly(graphId, procedure, Enumerable.Empty<string>(), EmptyKwargsDictionary);

        /// <summary>
        /// Call a saved procedure with parameters against a read-only node.
        /// </summary>
        /// <param name="graphId">The graph containing the saved procedure.</param>
        /// <param name="procedure">The procedure name.</param>
        /// <param name="args">A collection of positional arguments.</param>
        /// <returns>A result set.</returns>
        public ResultSet CallProcedureReadOnly(string graphId, string procedure, IEnumerable<string> args) =>
            CallProcedureReadOnly(graphId, procedure, args, EmptyKwargsDictionary);

        /// <summary>
        /// Call a saved procedure with parameters against a read-only node.
        /// </summary>
        /// <param name="graphId">The graph containing the saved procedure.</param>
        /// <param name="procedure">The procedure name.</param>
        /// <param name="args">A collection of positional arguments.</param>
        /// <param name="kwargs">A collection of keyword arguments.</param>
        /// <returns>A result set.</returns>
        public ResultSet CallProcedureReadOnly(string graphId, string procedure, IEnumerable<string> args, Dictionary<string, List<string>> kwargs)
        {
            args = args.Select(a => QuoteString(a));

            var queryBody = new StringBuilder();

            queryBody.Append($"CALL {procedure}({string.Join(",", args)})");

            if (kwargs.TryGetValue("y", out var kwargsList))
            {
                queryBody.Append(string.Join(",", kwargsList));
            }

            return RO_Query(graphId, queryBody.ToString());
        }
    }
}

// /// <summary>
// ///  Execute a Cypher query.
// ///
// ///  <param name="name a graph to perform the query on
// ///  <param name="query Cypher query
// ///  <returns> a result set
// ///
// ResultSet graphQuery(string name, string query);

// /// <summary>
// ///  Execute a Cypher read-only query.
// ///
// ///  <param name="name a graph to perform the query on
// ///  <param name="query Cypher query
// ///  <returns> a result set
// ///
// ResultSet graphReadonlyQuery(string name, string query);

// /// <summary>
// ///  Execute a Cypher query with timeout.
// ///
// ///  <param name="name a graph to perform the query on
// ///  <param name="query Cypher query
// ///  <param name="timeout
// ///  <returns> a result set
// ///
// ResultSet graphQuery(string name, string query, long timeout);

// /// <summary>
// ///  Execute a Cypher read-only query with timeout.
// ///
// ///  <param name="name a graph to perform the query on
// ///  <param name="query Cypher query
// ///  <param name="timeout
// ///  <returns> a result set
// ///
// ResultSet graphReadonlyQuery(string name, string query, long timeout);

// /// <summary>
// ///  Executes a cypher query with parameters.
// ///
// ///  <param name="name a graph to perform the query on.
// ///  <param name="query Cypher query.
// ///  <param name="params parameters map.
// ///  <returns> a result set.
// ///
// ResultSet graphQuery(string name, string query, Dictionary<string, object> params);

// /// <summary>
// ///  Executes a cypher read-only query with parameters.
// ///
// ///  <param name="name a graph to perform the query on.
// ///  <param name="query Cypher query.
// ///  <param name="params parameters map.
// ///  <returns> a result set.
// ///
// ResultSet graphReadonlyQuery(string name, string query, Dictionary<string, object> params);

// /// <summary>
// ///  Executes a cypher query with parameters and timeout.
// ///
// ///  <param name="name a graph to perform the query on.
// ///  <param name="query Cypher query.
// ///  <param name="params parameters map.
// ///  <param name="timeout
// ///  <returns> a result set.
// ///
// ResultSet graphQuery(string name, string query, Dictionary<string, object> params, long timeout);

// /// <summary>
// ///  Executes a cypher read-only query with parameters and timeout.
// ///
// ///  <param name="name a graph to perform the query on.
// ///  <param name="query Cypher query.
// ///  <param name="params parameters map.
// ///  <param name="timeout
// ///  <returns> a result set.
// ///
// ResultSet graphReadonlyQuery(string name, string query, Dictionary<string, object> params, long timeout);

// /// <summary>
// ///  Deletes the entire graph
// ///
// ///  <param name="name graph to delete
// ///  <returns> delete running time statistics
// ///
// string graphDelete(string name);

// /// <summary>
// ///  Lists all graph keys in the keyspace.
// ///  <returns> graph keys
// ///
// List<string> graphList();

// /// <summary>
// ///  Executes a query and produces an execution plan augmented with metrics for each operation's execution.
// ///
// List<string> graphProfile(string graphName, string query);

// /// <summary>
// ///  Constructs a query execution plan but does not run it. Inspect this execution plan to better understand how your
// ///  query will get executed.
// ///
// List<string> graphExplain(string graphName, string query);

// /// <summary>
// ///  Returns a list containing up to 10 of the slowest queries issued against the given graph ID.
// ///
// List<List<string>> graphSlowlog(string graphName);

// string graphConfigSet(string configName, object value);

// Dictionary<string, object> graphConfigGet(string configName);




