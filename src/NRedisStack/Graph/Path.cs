using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

[assembly: InternalsVisibleTo("NRedisGraph.Tests")]

namespace NRedisStack.Graph
{
    /// <summary>
    /// This class represents a path in the graph.
    /// </summary>
    public class Path
    {
        private readonly ReadOnlyCollection<Node> _nodes;
        private readonly ReadOnlyCollection<Edge> _edges;

        internal Path(IList<Node> nodes, IList<Edge> edges)
        {
            _nodes = new ReadOnlyCollection<Node>(nodes);
            _edges = new ReadOnlyCollection<Edge>(edges);
        }

        /// <summary>
        /// Nodes that exist on this path.
        /// </summary>
        public IEnumerable<Node> Nodes => _nodes;

        /// <summary>
        /// Edges that exist on this path.
        /// </summary>
        public IEnumerable<Edge> Edges => _edges;

        /// <summary>
        /// How many edges exist on this path.
        /// </summary>
        public int Length => _edges.Count;

        /// <summary>
        /// How many nodes exist on this path.
        /// </summary>
        public int NodeCount => _nodes.Count;

        /// <summary>
        /// Get the first node on this path.
        /// </summary>
        public Node FirstNode => _nodes[0];

        /// <summary>
        /// Get the last node on this path.
        /// </summary>
        /// <returns></returns>
        public Node LastNode => _nodes.Last();

        /// <summary>
        /// Get a node by index.
        /// </summary>
        /// <param name="index">The index of the node that you want to get.</param>
        /// <returns></returns>
        public Node GetNode(int index) => _nodes[index];

        /// <summary>
        /// Get an edge by index.
        /// </summary>
        /// <param name="index">The index of the edge that you want to get.</param>
        /// <returns></returns>
        public Edge GetEdge(int index) => _edges[index];

        /// <summary>
        /// Overriden `Equals` method that will consider the equality of the Nodes and Edges between two paths.
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            if (this == obj)
            {
                return true;
            }

            if (!(obj is Path path))
            {
                return false;
            }

            return Enumerable.SequenceEqual(Nodes, path.Nodes) && Enumerable.SequenceEqual(Edges, path.Edges);
        }

        /// <summary>
        /// Overridden `GetHashCode` method that will compute a hash code using the hash code of each node and edge on
        /// the path.
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            unchecked
            {
                int hash = 17;

                foreach (var node in Nodes)
                {
                    hash = hash * 31 + node.GetHashCode();
                }

                foreach (var edge in Edges)
                {
                    hash = hash * 31 + edge.GetHashCode();
                }

                return hash;
            }
        }

        /// <summary>
        /// Overridden `ToString` method that will emit a string based on the string values of the nodes and edges
        /// on the path.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            var sb = new StringBuilder();

            sb.Append("Path{");
            sb.Append($"nodes={Nodes}");
            sb.Append($", edges={Edges}");
            sb.Append("}");

            return sb.ToString();
        }
    }
}