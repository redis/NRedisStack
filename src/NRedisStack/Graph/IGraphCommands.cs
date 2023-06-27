using NRedisStack.Graph;
using StackExchange.Redis;

namespace NRedisStack
{
    public interface IGraphCommands
    {
        /// <summary>
        /// Execute a Cypher query with parameters.
        /// </summary>
        /// <param name="graphName">A graph to perform the query on.</param>
        /// <param name="query">The Cypher query.</param>
        /// <param name="parameters">Parameters map.</param>
        /// <param name="timeout">Timeout (optional).</param>
        /// <returns>A result set.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.query"/></remarks>
        ResultSet Query(string graphName, string query, IDictionary<string, object> parameters, long? timeout = null);

        /// <summary>
        /// Execute a Cypher query.
        /// </summary>
        /// <param name="graphName">A graph to perform the query on.</param>
        /// <param name="query">The Cypher query.</param>
        /// <param name="timeout">Timeout (optional).</param>
        /// <returns>A result set.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.query"/></remarks>
        ResultSet Query(string graphName, string query, long? timeout = null);

        /// <summary>
        /// Execute a Cypher query with parameters.
        /// </summary>
        /// <param name="graphName">A graph to perform the query on.</param>
        /// <param name="query">The Cypher query.</param>
        /// <param name="parameters">Parameters map.</param>
        /// <param name="timeout">Timeout (optional).</param>
        /// <returns>A result set.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.ro_query"/></remarks>
        ResultSet RO_Query(string graphName, string query, IDictionary<string, object> parameters, long? timeout = null);

        /// <summary>
        /// Execute a Cypher query.
        /// </summary>
        /// <param name="graphName">A graph to perform the query on.</param>
        /// <param name="query">The Cypher query.</param>
        /// <param name="timeout">Timeout (optional).</param>
        /// <returns>A result set.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.ro_query"/></remarks>
        ResultSet RO_Query(string graphName, string query, long? timeout = null);

        // TODO: Check if needed
        /// <summary>
        /// Call a saved procedure.
        /// </summary>
        /// <param name="graphName">The graph containing the saved procedure.</param>
        /// <param name="procedure">The procedure name.</param>
        /// <returns>A result set.</returns>
        ResultSet CallProcedure(string graphName, string procedure);

        /// <summary>
        /// Call a saved procedure with parameters.
        /// </summary>
        /// <param name="graphName">The graph containing the saved procedure.</param>
        /// <param name="procedure">The procedure name.</param>
        /// <param name="args">A collection of positional arguments.</param>
        /// <returns>A result set.</returns>
        ResultSet CallProcedure(string graphName, string procedure, IEnumerable<string> args);

        /// <summary>
        /// Call a saved procedure with parameters.
        /// </summary>
        /// <param name="graphName">The graph containing the saved procedure.</param>
        /// <param name="procedure">The procedure name.</param>
        /// <param name="args">A collection of positional arguments.</param>
        /// <param name="kwargs">A collection of keyword arguments.</param>
        /// <returns>A result set.</returns>
        ResultSet CallProcedure(string graphName, string procedure, IEnumerable<string> args, Dictionary<string, List<string>> kwargs);

        /// <summary>
        /// Delete an existing graph.
        /// </summary>
        /// <param name="graphName">The graph to delete.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise/></returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.delete"/></remarks>
        bool Delete(string graphName);

        /// <summary>
        /// Constructs a query execution plan but does not run it. Inspect this execution plan to better understand how your
        /// query will get executed.
        /// </summary>
        /// <param name="graphName">The graph name.</param>
        /// <param name="query">The query.</param>
        /// <returns>String representation of a query execution plan.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.explain"/></remarks>
        IReadOnlyList<string> Explain(string graphName, string query);

        /// <summary>
        /// Executes a query and produces an execution plan augmented with metrics for each operation's execution.
        /// </summary>
        /// <param name="graphName">The graph name.</param>
        /// <param name="query">The query.</param>
        /// <param name="timeout">Timeout (optional).</param>
        /// <returns>String representation of a query execution plan,
        /// with details on results produced by and time spent in each operation.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.profile"/></remarks>
        IReadOnlyList<string> Profile(string graphName, string query, long? timeout = null);

        /// <summary>
        /// Lists all graph keys in the keyspace.
        /// </summary>
        /// <returns>List of all graph keys in the keyspace.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.list"/></remarks>
        IReadOnlyList<string> List();

        /// <summary>
        /// Set the value of a RedisGraph configuration parameter.
        /// </summary>
        /// <param name="configName">The config name.</param>
        /// <param name="value">Value to set.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.config-set"/></remarks>
        bool ConfigSet(string configName, object value);

        /// <summary>
        /// Set the value of a RedisGraph configuration parameter.
        /// </summary>
        /// <param name="configName">The config name.</param>
        /// <returns>Dictionary of <string, object>.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.config-get"/></remarks>
        Dictionary<string, RedisResult> ConfigGet(string configName);

        /// <summary>
        /// Returns a list containing up to 10 of the slowest queries issued against the given graph Name.
        /// </summary>
        /// <param name="graphName">The graph name.</param>
        /// <returns>Dictionary of <string, object>.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.slowlog"/></remarks>
        List<List<string>> Slowlog(string graphName);
    }
}