using NRedisStack.Graph.DataTypes;
namespace NRedisStack.Tests.Graph
{
    public sealed class PathBuilder
    {
        private readonly List<Node> _nodes;
        private readonly List<Edge> _edges;
        private Type _currentAppendClass;

        public PathBuilder()
        {
            _nodes = new List<Node>();
            _edges = new List<Edge>();

            _currentAppendClass = typeof(Node);
        }

        public PathBuilder(int nodesCount)
        {
            _nodes = new List<Node>(nodesCount);
            _edges = new List<Edge>(nodesCount - 1 >= 0 ? nodesCount - 1 : 0);

            _currentAppendClass = typeof(Node);
        }

        public PathBuilder Append(Edge edge)
        {
            if (_currentAppendClass != typeof(Edge))
            {
                throw new ArgumentException("Path builder expected Node but was Edge.");
            }

            _edges.Add(edge);

            _currentAppendClass = typeof(Node);

            return this;
        }

        public PathBuilder Append(Node node)
        {
            if (_currentAppendClass != typeof(Node))
            {
                throw new ArgumentException("Path builder expected Edge but was Node.");
            }

            _nodes.Add(node);

            _currentAppendClass = typeof(Edge);

            return this;
        }

        public NRedisStack.Graph.DataTypes.Path Build()
        {
            if (_nodes.Count != _edges.Count + 1)
            {
                throw new ArgumentException("Path builder nodes count should be edge count + 1");
            }

            return new NRedisStack.Graph.DataTypes.Path(_nodes, _edges);
        }
    }
}