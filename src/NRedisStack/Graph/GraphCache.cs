namespace NRedisStack.Graph
{
    internal sealed class GraphCache
    {
        public GraphCacheList Labels { get; set; }
        public GraphCacheList PropertyNames { get; set; }
        public GraphCacheList RelationshipTypes { get; set; }

        [Obsolete]
        public GraphCache(string graphName, GraphCommands redisGraph)
        {
            Labels = new GraphCacheList(graphName, "db.labels", redisGraph);
            PropertyNames = new GraphCacheList(graphName, "db.propertyKeys", redisGraph);
            RelationshipTypes = new GraphCacheList(graphName, "db.relationshipTypes", redisGraph);
        }

        [Obsolete]
        public GraphCache(string graphName, GraphCommandsAsync redisGraph)
        {
            Labels = new GraphCacheList(graphName, "db.labels", redisGraph);
            PropertyNames = new GraphCacheList(graphName, "db.propertyKeys", redisGraph);
            RelationshipTypes = new GraphCacheList(graphName, "db.relationshipTypes", redisGraph);
        }

        [Obsolete]
        public string GetLabel(int index) => Labels.GetCachedData(index)!;

        [Obsolete]
        public string GetRelationshipType(int index) => RelationshipTypes.GetCachedData(index)!;

        [Obsolete]
        public string GetPropertyName(int index) => PropertyNames.GetCachedData(index)!;

    }
}