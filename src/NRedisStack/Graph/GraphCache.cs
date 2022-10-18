namespace NRedisStack.Graph
{
    internal interface IGraphCache
    {
        string GetLabel(int index);
        string GetRelationshipType(int index);
        string GetPropertyName(int index);
    }

    internal abstract class BaseGraphCache : IGraphCache
    {
        protected GraphCacheList Labels { get; set; }
        protected GraphCacheList PropertyNames { get; set; }
        protected GraphCacheList RelationshipTypes { get; set; }

        public string GetLabel(int index) => Labels.GetCachedData(index);

        public string GetRelationshipType(int index) => RelationshipTypes.GetCachedData(index);

        public string GetPropertyName(int index) => PropertyNames.GetCachedData(index);
    }

    internal sealed class GraphCache : BaseGraphCache
    {
        public GraphCache(string graphId, GraphCommands redisGraph)
        {
            Labels = new GraphCacheList(graphId, "db.labels", redisGraph);
            PropertyNames = new GraphCacheList(graphId, "db.propertyKeys", redisGraph);
            RelationshipTypes = new GraphCacheList(graphId, "db.relationshipTypes", redisGraph);
        }
    }

    internal sealed class ReadOnlyGraphCache : BaseGraphCache
    {
        public ReadOnlyGraphCache(string graphId, GraphCommands redisGraph)
        {
            Labels = new ReadOnlyGraphCacheList(graphId, "db.labels", redisGraph);
            PropertyNames = new ReadOnlyGraphCacheList(graphId, "db.propertyKeys", redisGraph);
            RelationshipTypes = new ReadOnlyGraphCacheList(graphId, "db.relationshipTypes", redisGraph);
        }
    }
}