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

        NodesCreated = GetIntValue("Nodes created");
        NodesDeleted = GetIntValue("Nodes deleted");
        IndicesAdded = GetIntValue("Indices added");
        IndicesCreated = GetIntValue("Indices created");
        IndicesDeleted = GetIntValue("Indices deleted");
        LabelsAdded = GetIntValue("Labels added");
        RelationshipsDeleted = GetIntValue("Relationships deleted");
        RelationshipsCreated = GetIntValue("Relationships created");
        PropertiesSet = GetIntValue("Properties set");
        QueryInternalExecutionTime = GetStringValue("Query internal execution time");
        GraphRemovedInternalExecutionTime = GetStringValue("Graph removed, internal execution time");
        CachedExecution = (GetIntValue("Cached execution") == 1);

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
        public int NodesCreated { get; }

        /// <summary>
        /// Number of nodes deleted.
        /// </summary>
        /// <returns></returns>
        public int NodesDeleted { get; }

        /// <summary>
        /// Number of indices added.
        /// </summary>
        /// <returns></returns>
        public int IndicesAdded { get; }

        /// <summary>
        /// Number of indices created.
        /// </summary>
        /// <returns></returns>
        public int IndicesCreated { get; }

        /// <summary>
        /// Number of indices deleted.
        /// </summary>
        public int IndicesDeleted { get; }

        /// <summary>
        /// Number of labels added.
        /// </summary>
        /// <returns></returns>
        public int LabelsAdded { get; }

        /// <summary>
        /// Number of relationships deleted.
        /// </summary>
        /// <returns></returns>
        public int RelationshipsDeleted { get; }

        /// <summary>
        /// Number of relationships created.
        /// </summary>
        /// <returns></returns>
        public int RelationshipsCreated { get; }

        /// <summary>
        /// Number of properties set.
        /// </summary>
        /// <returns></returns>
        public int PropertiesSet { get; }

        /// <summary>
        /// How long the query took to execute.
        /// </summary>
        /// <returns></returns>
        public string QueryInternalExecutionTime { get; }

        /// <summary>
        /// How long it took to remove a graph.
        /// </summary>
        /// <returns></returns>
        public string GraphRemovedInternalExecutionTime { get; }

        /// <summary>
        /// The execution plan was cached on RedisGraph.
        /// </summary>
        public bool CachedExecution { get; }
    }
}