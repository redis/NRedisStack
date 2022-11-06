using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NRedisStack.Graph.DataTypes
{
    /// <summary>
    /// A class representing a node (graph entity). In addition to the base class ID and properties, a node has labels.
    /// </summary>
    public sealed class Node : GraphEntity
    {
        private readonly List<string> _labels = new List<string>();

        /// <summary>
        /// Adds a label to a node.
        /// </summary>
        /// <param name="label">Name of the label.</param>
        public void AddLabel(string label) => _labels.Add(label);

        /// <summary>
        /// Remove a label by name.
        /// </summary>
        /// <param name="label">Name of the label to remove.</param>
        public void RemoveLabel(string label) => _labels.Remove(label);

        /// <summary>
        /// Get a label by index.
        /// </summary>
        /// <param name="index">Index of the label to get.</param>
        /// <returns></returns>
        public string GetLabel(int index) => _labels[index];

        /// <summary>
        /// Get the count of labels on the node.
        /// </summary>
        /// <returns>Number of labels on a node.</returns>
        public int GetNumberOfLabels() => _labels.Count;

        /// <summary>
        /// Overriden member that checks to see if the names of the labels of a node are equal
        /// (in addition to base `Equals` functionality).
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
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

            return Enumerable.SequenceEqual(_labels, that._labels);
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

                foreach(var label in _labels)
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
            sb.Append($"[{string.Join(", ", _labels)}]");
            sb.Append(", id=");
            sb.Append(Id);
            sb.Append(", propertyMap={");
            sb.Append(string.Join(", ", PropertyMap.Select(pm => $"{pm.Key}={pm.Value}")));
            sb.Append("}}");

            return sb.ToString();
        }
    }
}