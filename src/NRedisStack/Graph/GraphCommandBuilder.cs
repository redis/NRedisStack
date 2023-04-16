using NRedisStack.Graph.Literals;
using NRedisStack.RedisStackCommands;

namespace NRedisStack
{
    public static class GraphCommandBuilder
    {
        internal static readonly object CompactQueryFlag = "--COMPACT";

        /// <inheritdoc/>
        public static SerializedCommand Query(string graphName, string query, long? timeout = null)
        {
            var args = (timeout == null) ? new List<object>(3) { graphName, query, CompactQueryFlag }
                                         : new List<object>(5) { graphName, query, CompactQueryFlag, GraphArgs.TIMEOUT, timeout };

            return new SerializedCommand(GRAPH.QUERY, args);
        }

        /// <inheritdoc/>
        public static SerializedCommand RO_Query(string graphName, string query, long? timeout = null)
        {
            var args = (timeout == null) ? new List<object>(3) { graphName, query, CompactQueryFlag }
                                         : new List<object>(5) { graphName, query, CompactQueryFlag, GraphArgs.TIMEOUT, timeout };

            return new SerializedCommand(GRAPH.RO_QUERY, args);
        }
        internal static readonly Dictionary<string, List<string>> EmptyKwargsDictionary =
            new Dictionary<string, List<string>>();

        /// <inheritdoc/>
        public static SerializedCommand Delete(string graphName)
        {
            return new SerializedCommand(GRAPH.DELETE, graphName);
        }

        /// <inheritdoc/>
        public static SerializedCommand Explain(string graphName, string query)
        {
            return new SerializedCommand(GRAPH.EXPLAIN, graphName, query);
        }

        /// <inheritdoc/>
        public static SerializedCommand Profile(string graphName, string query, long? timeout = null)
        {
            var args = (timeout == null) ? new List<object>(2) { graphName, query }
                                         : new List<object>(4) { graphName, query, GraphArgs.TIMEOUT, timeout };

            return new SerializedCommand(GRAPH.PROFILE, args);
        }

        /// <inheritdoc/>
        public static SerializedCommand List()
        {
            return new SerializedCommand(GRAPH.LIST);
        }

        /// <inheritdoc/>
        public static SerializedCommand ConfigSet(string configName, object value)
        {
            return new SerializedCommand(GRAPH.CONFIG, GraphArgs.SET, configName, value);
        }

        /// <inheritdoc/>
        public static SerializedCommand ConfigGet(string configName)
        {
            return new SerializedCommand(GRAPH.CONFIG, GraphArgs.GET, configName);
        }

        /// <inheritdoc/>
        public static SerializedCommand Slowlog(string graphName)
        {
            return new SerializedCommand(GRAPH.SLOWLOG, graphName);
        }
    }
}