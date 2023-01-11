using System.Collections.ObjectModel;
using System.Runtime.CompilerServices;
using System.Text;

[assembly: InternalsVisibleTo("NRedisStack.Tests.Graph")]

namespace NRedisStack.Graph.DataTypes
{
    /// <summary>
    /// This class represents a path in the graph.
    /// </summary>
    public class Path
    {
        public ReadOnlyCollection<Node> Nodes { get;}
        public ReadOnlyCollection<Edge> Edges { get;}

        public Path(IList<Node> nodes, IList<Edge> edges)
        {
            Nodes = new ReadOnlyCollection<Node>(nodes);
            Edges = new ReadOnlyCollection<Edge>(edges);
        }

        /// <summary>
        /// Overriden `Equals` method that will consider the equality of the Nodes and Edges between two paths.
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object? obj)
        {
            if (obj == null) return this == null;

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