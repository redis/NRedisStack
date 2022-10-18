using System;
using System.Collections.Generic;
using System.Linq;
using StackExchange.Redis;
// TODO: check if all of these are needed
namespace NRedisStack.Graph
{
    /// <summary>
    /// Query result statistics are encapsulated by this class.
    /// </summary>
    public sealed class Statistics
    {
        /// <summary>
        /// A class that represents the various kinds of statistics labels.
        ///
        /// In JRedisGraph this was represented by using an `enum`, here we're using the "smart enum"
        /// pattern to replicate the logic.
        /// </summary>
        public sealed class Label
        {

            private const string LABELS_ADDED = "Labels added";
            private const string INDICES_ADDED = "Indices added";
            private const string INDICES_CREATED = "Indices created";
            private const string INDICES_DELETED = "Indices deleted";
            private const string NODES_CREATED = "Nodes created";
            private const string NODES_DELETED = "Nodes deleted";
            private const string RELATIONSHIPS_DELETED = "Relationships deleted";
            private const string PROPERTIES_SET = "Properties set";
            private const string RELATIONSHIPS_CREATED = "Relationships created";
            private const string QUERY_INTERNAL_EXECUTION_TIME = "Query internal execution time";
            private const string GRAPH_REMOVED_INTERNAL_EXECUTION_TIME = "Graph removed, internal execution time";
            private const string CACHED_EXECUTION = "Cached execution";

            /// <summary>
            /// The string value of this label.
            /// </summary>
            /// <value></value>
            public string Value { get; }

            private Label(string value) => Value = value;

            /// <summary>
            /// Get a "Labels Added" statistics label.
            /// </summary>
            /// <returns></returns>
            public static readonly Label LabelsAdded = new Label(LABELS_ADDED);

            /// <summary>
            /// Get an "Indices Added" statistics label.
            /// </summary>
            /// <returns></returns>
            public static readonly Label IndicesAdded = new Label(INDICES_ADDED);

            /// <summary>
            /// Get an "Indices Created" statistics label.
            /// </summary>
            /// <returns></returns>
            public static readonly Label IndicesCreated = new Label(INDICES_CREATED);

            /// <summary>
            /// Get an "Indices Deleted" statistics label.
            /// </summary>
            /// <returns></returns>
            public static readonly Label IndicesDeleted = new Label(INDICES_DELETED);

            /// <summary>
            /// Get a "Nodes Created" statistics label.
            /// </summary>
            /// <returns></returns>
            public static readonly Label NodesCreated = new Label(NODES_CREATED);

            /// <summary>
            /// Get a "Nodes Deleted" statistics label.
            /// </summary>
            /// <returns></returns>
            public static readonly Label NodesDeleted = new Label(NODES_DELETED);

            /// <summary>
            /// Get a "Relationships Deleted" statistics label.
            /// </summary>
            /// <returns></returns>
            public static readonly Label RelationshipsDeleted = new Label(RELATIONSHIPS_DELETED);

            /// <summary>
            /// Get a "Properties Set" statistics label.
            /// </summary>
            /// <returns></returns>
            public static readonly Label PropertiesSet = new Label(PROPERTIES_SET);

            /// <summary>
            /// Get a "Relationships Created" statistics label.
            /// </summary>
            /// <returns></returns>
            public static readonly Label RelationshipsCreated = new Label(RELATIONSHIPS_CREATED);

            /// <summary>
            /// Get a "Query Internal Execution Time" statistics label.
            /// </summary>
            /// <returns></returns>
            public static readonly Label QueryInternalExecutionTime = new Label(QUERY_INTERNAL_EXECUTION_TIME);

            /// <summary>
            /// Get a "Graph Removed Internal Execution Time" statistics label.
            /// </summary>
            /// <returns></returns>
            public static readonly Label GraphRemovedInternalExecutionTime = new Label(GRAPH_REMOVED_INTERNAL_EXECUTION_TIME);

            /// <summary>
            /// Get a "Cached execution" statistics label.
            /// </summary>
            public static readonly Label CachedExecution = new Label(CACHED_EXECUTION);

            /// <summary>
            /// Return an Label based on a string value provided.
            /// </summary>
            /// <param name="labelValue">String value to map to a statistics label.</param>
            /// <returns></returns>
            public static Label FromString(string labelValue)
            {
                switch (labelValue)
                {
                    case LABELS_ADDED:
                        return LabelsAdded;
                    case INDICES_ADDED:
                        return IndicesAdded;
                    case INDICES_CREATED:
                        return IndicesCreated;
                    case INDICES_DELETED:
                        return IndicesDeleted;
                    case NODES_CREATED:
                        return NodesCreated;
                    case NODES_DELETED:
                        return NodesDeleted;
                    case RELATIONSHIPS_DELETED:
                        return RelationshipsDeleted;
                    case PROPERTIES_SET:
                        return PropertiesSet;
                    case RELATIONSHIPS_CREATED:
                        return RelationshipsCreated;
                    case QUERY_INTERNAL_EXECUTION_TIME:
                        return QueryInternalExecutionTime;
                    case GRAPH_REMOVED_INTERNAL_EXECUTION_TIME:
                        return GraphRemovedInternalExecutionTime;
                    case CACHED_EXECUTION:
                        return CachedExecution;
                    default:
                        return new Label(labelValue);
                }
            }
        }

        private readonly RedisResult[] _statistics;

        internal Statistics(RedisResult statistics)
        {
            if (statistics.Type == ResultType.MultiBulk)
            {
                _statistics = (RedisResult[])statistics;
            }
            else
            {
                _statistics = new[] { statistics };
            }
        }


        private IDictionary<Label, string> _statisticsValues;

        /// <summary>
        /// Retrieves the relevant statistic.
        /// </summary>
        /// <param name="label">The requested statistic label.</param>
        /// <returns>A string representation of the specific statistic or null</returns>
        public string GetStringValue(Label label)
        {
            if (_statisticsValues == default)
            {
                _statisticsValues = _statistics
                    .Select(x =>
                    {
                        var s = ((string)x).Split(':');

                        return new
                        {
                            Label = Label.FromString(s[0].Trim()),
                            Value = s[1].Trim()
                        };
                    }).ToDictionary(k => k.Label, v => v.Value);
            }

            return _statisticsValues.TryGetValue(label, out var value) ? value : default;
        }

        /// <summary>
        /// Number of nodes created.
        /// </summary>
        /// <returns></returns>
        public int NodesCreated => int.TryParse(GetStringValue(Label.NodesCreated), out var result) ? result : 0;

        /// <summary>
        /// Number of nodes deleted.
        /// </summary>
        /// <returns></returns>
        public int NodesDeleted => int.TryParse(GetStringValue(Label.NodesDeleted), out var result) ? result : 0;

        /// <summary>
        /// Number of indices added.
        /// </summary>
        /// <returns></returns>
        public int IndicesAdded => int.TryParse(GetStringValue(Label.IndicesAdded), out var result) ? result : 0;

        /// <summary>
        /// Number of indices created.
        /// </summary>
        /// <returns></returns>
        public int IndicesCreated => int.TryParse(GetStringValue(Label.IndicesCreated), out var result) ? result : 0;

        /// <summary>
        /// Number of indices deleted.
        /// </summary>
        public int IndicesDeleted => int.TryParse(GetStringValue(Label.IndicesDeleted), out var result) ? result : 0;

        /// <summary>
        /// Number of labels added.
        /// </summary>
        /// <returns></returns>
        public int LabelsAdded => int.TryParse(GetStringValue(Label.LabelsAdded), out var result) ? result : 0;

        /// <summary>
        /// Number of relationships deleted.
        /// </summary>
        /// <returns></returns>
        public int RelationshipsDeleted => int.TryParse(GetStringValue(Label.RelationshipsDeleted), out var result) ? result : 0;

        /// <summary>
        /// Number of relationships created.
        /// </summary>
        /// <returns></returns>
        public int RelationshipsCreated => int.TryParse(GetStringValue(Label.RelationshipsCreated), out var result) ? result : 0;

        /// <summary>
        /// Number of properties set.
        /// </summary>
        /// <returns></returns>
        public int PropertiesSet => int.TryParse(GetStringValue(Label.PropertiesSet), out var result) ? result : 0;

        /// <summary>
        /// How long the query took to execute.
        /// </summary>
        /// <returns></returns>
        public string QueryInternalExecutionTime => GetStringValue(Label.QueryInternalExecutionTime);

        /// <summary>
        /// How long it took to remove a graph.
        /// </summary>
        /// <returns></returns>
        public string GraphRemovedInternalExecutionTime => GetStringValue(Label.GraphRemovedInternalExecutionTime);

        /// <summary>
        /// The execution plan was cached on RedisGraph.
        /// </summary>
        public bool CachedExecution => int.TryParse(GetStringValue(Label.CachedExecution), out var result) && result == 1;
    }
}