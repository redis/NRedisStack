using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NRedisStack.Literals;
using StackExchange.Redis;
using static NRedisStack.Graph.RedisGraphUtilities;

namespace NRedisStack.Graph
{
    /// <summary>
    /// Allows for executing a series of RedisGraph queries as a single unit.
    /// </summary>
    public class RedisGraphTransaction
    {
        private class TransactionResult
        {
            public string GraphId { get; }

            public Task<RedisResult> PendingTask { get; }

            public TransactionResult(string graphId, Task<RedisResult> pendingTask)
            {
                GraphId = graphId;
                PendingTask = pendingTask;
            }
        }

        private readonly ITransaction _transaction;
        private readonly IDictionary<string, GraphCache> _graphCaches;
        private readonly GraphCommands _redisGraph;
        private readonly List<TransactionResult> _pendingTasks = new List<TransactionResult>();
        private readonly List<string> _graphCachesToRemove = new List<string>();

        internal RedisGraphTransaction(ITransaction transaction, GraphCommands redisGraph, IDictionary<string, GraphCache> graphCaches)
        {
            _graphCaches = graphCaches;
            _redisGraph = redisGraph;
            _transaction = transaction;
        }

        /// <summary>
        /// Execute a RedisGraph query with parameters.
        /// </summary>
        /// <param name="graphId">A graph to execute the query against.</param>
        /// <param name="query">The Cypher query.</param>
        /// <param name="parameters">The parameters for the query.</param>
        /// <returns>A ValueTask, the actual result isn't known until `Exec` or `ExecAsync` is invoked.</returns>
        public ValueTask QueryAsync(string graphId, string query, IDictionary<string, object> parameters)
        {
            var preparedQuery = PrepareQuery(query, parameters);

            return QueryAsync(graphId, preparedQuery);
        }

        /// <summary>
        /// Execute a RedisGraph query with parameters.
        /// </summary>
        /// <param name="graphId">A graph to execute the query against.</param>
        /// <param name="query">The Cypher query.</param>
        /// <returns>A ValueTask, the actual result isn't known until `Exec` or `ExecAsync` is invoked.</returns>
        public ValueTask QueryAsync(string graphId, string query)
        {
            if(!_graphCaches.ContainsKey(graphId))
            {
                _graphCaches.Add(graphId, new GraphCache(graphId, _redisGraph));
            }

            _pendingTasks.Add(new TransactionResult(graphId, _transaction.ExecuteAsync(GRAPH.QUERY, graphId, query, GraphCommands.CompactQueryFlag)));

            return default(ValueTask);
        }

        /// <summary>
        /// Execute a saved procedure.
        /// </summary>
        /// <param name="graphId">The graph containing the saved procedure.</param>
        /// <param name="procedure">The procedure name.</param>
        /// <returns>A ValueTask, the actual result isn't known until `Exec` or `ExecAsync` is invoked.</returns>
        public ValueTask CallProcedureAsync(string graphId, string procedure) =>
            CallProcedureAsync(graphId, procedure, Enumerable.Empty<string>(), GraphCommands.EmptyKwargsDictionary);

        /// <summary>
        /// Execute a saved procedure with parameters.
        /// </summary>
        /// <param name="graphId">The graph containing the saved procedure.</param>
        /// <param name="procedure">The procedure name.</param>
        /// <param name="args">A collection of positional arguments.</param>
        /// <param name="kwargs">A collection of keyword arguments.</param>
        /// <returns>A ValueTask, the actual result isn't known until `Exec` or `ExecAsync` is invoked.</returns>
        public ValueTask CallProcedureAsync(string graphId, string procedure, IEnumerable<string> args, Dictionary<string, List<string>> kwargs)
        {
            args = args.Select(QuoteString);

            var queryBody = new StringBuilder();

            queryBody.Append($"CALL {procedure}({string.Join(",", args)})");

            if (kwargs.TryGetValue("y", out var kwargsList))
            {
                queryBody.Append(string.Join(",", kwargsList));
            }

            return QueryAsync(graphId, queryBody.ToString());
        }

        /// <summary>
        /// Delete a graph.
        /// </summary>
        /// <param name="graphId">The name of the graph to delete.</param>
        /// <returns>A ValueTask, the actual result isn't known until `Exec` or `ExecAsync` is invoked.</returns>
        public ValueTask DeleteGraphAsync(string graphId)
        {
            _pendingTasks.Add(new TransactionResult(graphId, _transaction.ExecuteAsync(GRAPH.DELETE, graphId)));

            _graphCachesToRemove.Add(graphId);

            return default(ValueTask);
        }

        /// <summary>
        /// Execute all of the commands that have been invoked on the transaction.
        /// </summary>
        /// <returns>A collection of results for all of the commands invoked before calling `Exec`.</returns>
        public ResultSet[] Exec()
        {
            var results = new ResultSet[_pendingTasks.Count];

            var success = _transaction.Execute(); // TODO: Handle false (which means the transaction didn't succeed.)

            for (var i = 0; i < _pendingTasks.Count; i++)
            {
                var result = _pendingTasks[i].PendingTask.Result;
                var graphId = _pendingTasks[i].GraphId;

                results[i] = new ResultSet(result, _graphCaches[graphId]);
            }

            ProcessPendingGraphCacheRemovals();

            return results;
        }

        /// <summary>
        /// Execute all of the commands that have been invoked on the transaction.
        /// </summary>
        /// <returns>A collection of results for all of the commands invoked before calling `ExecAsync`.</returns>
        public async Task<ResultSet[]> ExecAsync()
        {
            var results = new ResultSet[_pendingTasks.Count];

            var success = await _transaction.ExecuteAsync();

            for (var i = 0; i < _pendingTasks.Count; i++)
            {
                var result = _pendingTasks[i].PendingTask.Result;
                var graphId = _pendingTasks[i].GraphId;

                results[i] = new ResultSet(result, _graphCaches[graphId]);
            }

            ProcessPendingGraphCacheRemovals();

            return results;
        }

        private void ProcessPendingGraphCacheRemovals()
        {
            foreach(var graph in _graphCachesToRemove)
            {
                _graphCaches.Remove(graph);
            }
        }
    }
}