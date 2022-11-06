namespace NRedisStack.Graph
{
    internal sealed class GraphCache
    {

        public GraphCacheList Labels { get; set; }
        public GraphCacheList PropertyNames { get; set; }
        public GraphCacheList RelationshipTypes { get; set; }
        
        public GraphCache(string graphId, GraphCommands redisGraph)
        {
            Labels = new GraphCacheList(graphId, "db.labels", redisGraph);
            PropertyNames = new GraphCacheList(graphId, "db.propertyKeys", redisGraph);
            RelationshipTypes = new GraphCacheList(graphId, "db.relationshipTypes", redisGraph);
        }

        public string GetLabel(int index) => Labels.GetCachedData(index);

        public string GetRelationshipType(int index) => RelationshipTypes.GetCachedData(index);

        public string GetPropertyName(int index) => PropertyNames.GetCachedData(index);

    }
}
