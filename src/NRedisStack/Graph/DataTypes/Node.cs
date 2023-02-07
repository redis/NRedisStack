using System.Text;

namespace NRedisStack.Graph.DataTypes
{
    /// <summary>
    /// A class representing a node (graph entity). In addition to the base class ID and properties, a node has labels.
    /// </summary>
    public sealed class Node : GraphEntity
    {
        public List<string> Labels { get; }

        public Node()
        {
            Labels = new List<string>();
        }

        /// <summary>
        /// Overriden member that checks to see if the names of the labels of a node are equal
        /// (in addition to base `Equals` functionality).
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

            if (!(obj is Node that))
            {
                return false;
            }

            if (!base.Equals(obj))
            {
                return false;
            }

            return Enumerable.SequenceEqual(Labels, that.Labels);
        }

        /// <summary>
        /// Overridden member that computes a hash code based on the base `GetHashCode` implementation
        /// as well as the hash codes of all labels.
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            unchecked
            {
                int hash = 17;

                foreach(var label in Labels)
                {
                    hash = hash * 31 + label.GetHashCode();
                }

                hash = hash * 31 + base.GetHashCode();

                return hash;
            }
        }

        /// <summary>
        /// Overridden member that emits a string containing the labels, ID, and property map of a node.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            var sb = new StringBuilder();

            sb.Append("Node{labels=");
            sb.Append($"[{string.Join(", ", Labels)}]");
            sb.Append($", id={Id}");
            sb.Append($", {PropertyMapToString()}");
            sb.Append("}");

            return sb.ToString();
        }
    }
}