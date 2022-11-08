using System;
using System.Collections.Generic;
using System.Linq;
using StackExchange.Redis;
namespace NRedisStack.Graph
{
    /// <summary>
    /// Query result statistics are encapsulated by this class.
    /// </summary>
    public sealed class Statistics
    {
        private IDictionary<string, string> _statistics;

        internal Statistics(Dictionary<string, string> statistics)
        {
            _statistics = statistics;
        }

        /// <summary>
        /// Retrieves the relevant statistic.
        /// </summary>
        /// <param name="label">The requested statistic label.</param>
        /// <returns>A string representation of the specific statistic or null</returns>
        public string? GetStringValue(string label) =>
                _statistics.TryGetValue(label, out string? value) ? value : null;


        private int GetIntValue(string label)
        {
            string value = GetStringValue(label);
            return int.TryParse(value, out var result) ? result : 0;
        }

        /// <summary>
        /// Number of nodes created.
        /// </summary>
        /// <returns></returns>
        public int NodesCreated => GetIntValue("Nodes created");

        /// <summary>
        /// Number of nodes deleted.
        /// </summary>
        /// <returns></returns>
        public int NodesDeleted => GetIntValue("Nodes deleted");

        /// <summary>
        /// Number of indices added.
        /// </summary>
        /// <returns></returns>
        public int IndicesAdded => GetIntValue("Indices added");

        /// <summary>
        /// Number of indices created.
        /// </summary>
        /// <returns></returns>
        public int IndicesCreated => GetIntValue("Indices created");

        /// <summary>
        /// Number of indices deleted.
        /// </summary>
        public int IndicesDeleted => GetIntValue("Indices deleted");

        /// <summary>
        /// Number of labels added.
        /// </summary>
        /// <returns></returns>
        public int LabelsAdded => GetIntValue("Labels added");

        /// <summary>
        /// Number of relationships deleted.
        /// </summary>
        /// <returns></returns>
        public int RelationshipsDeleted => GetIntValue("Relationships deleted");

        /// <summary>
        /// Number of relationships created.
        /// </summary>
        /// <returns></returns>
        public int RelationshipsCreated => GetIntValue("Relationships created");

        /// <summary>
        /// Number of properties set.
        /// </summary>
        /// <returns></returns>
        public int PropertiesSet => GetIntValue("Properties set");

        /// <summary>
        /// How long the query took to execute.
        /// </summary>
        /// <returns></returns>
        public string QueryInternalExecutionTime => GetStringValue("Query internal execution time");

        /// <summary>
        /// How long it took to remove a graph.
        /// </summary>
        /// <returns></returns>
        public string GraphRemovedInternalExecutionTime => GetStringValue("Graph removed, internal execution time");

        /// <summary>
        /// The execution plan was cached on RedisGraph.
        /// </summary>
        public bool CachedExecution => GetIntValue("Cached execution") == 1;
    }
}