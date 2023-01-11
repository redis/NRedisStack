using System.Text;

namespace NRedisStack.Graph.DataTypes
{
    /// <summary>
    /// A class reprenting an edge (graph entity). In addition to the base class properties, an edge shows its source,
    /// destination, and relationship type.
    /// </summary>
    public class Edge : GraphEntity
    {
        /// <summary>
        /// The relationship type.
        /// </summary>
        /// <value></value>
        public string RelationshipType { get; set; }

        /// <summary>
        /// The ID of the source node.
        /// </summary>
        /// <value></value>
        public long Source { get; set; }

        /// <summary>
        /// The ID of the desination node.
        /// </summary>
        /// <value></value>
        public long Destination { get; set; }

        /// <summary>
        /// Overriden from the base `Equals` implementation. In addition to the expected behavior of checking
        /// reference equality, we'll also fall back and check to see if the: Source, Destination, and RelationshipType
        /// are equal.
        /// </summary>
        /// <param name="obj">Another `Edge` object to compare to.</param>
        /// <returns>True if the two instances are equal, false if not.</returns>
        public override bool Equals(object? obj)
        {
            if (obj == null) return this == null;

            if (this == obj)
            {
                return true;
            }

            if (!(obj is Edge that))
            {
                return false;
            }

            if (!base.Equals(obj))
            {
                return false;
            }

            return Source == that.Source && Destination == that.Destination && RelationshipType == that.RelationshipType;
        }

        /// <summary>
        /// Overriden from base to compute a deterministic hashcode based on RelationshipType, Source, and Destination.
        /// </summary>
        /// <returns>An integer representing the hash code for this instance.</returns>
        public override int GetHashCode()
        {
            unchecked
            {
                int hash = 17;

                hash = hash * 31 + base.GetHashCode();
                hash = hash * 31 + RelationshipType.GetHashCode();
                hash = hash * 31 + Source.GetHashCode();
                hash = hash * 31 + Destination.GetHashCode();

                return hash;
            }
        }

        /// <summary>
        /// Override from base to emit a string that contains: RelationshipType, Source, Destination, Id, and PropertyMap.
        /// </summary>
        /// <returns>A string containing a description of the Edge containing a RelationshipType, Source, Destination, Id, and PropertyMap.</returns>
        public override string ToString()
        {
            var sb = new StringBuilder();

            sb.Append("Edge{");
            sb.Append($"relationshipType='{RelationshipType}'");
            sb.Append($", source={Source}");
            sb.Append($", destination={Destination}");
            sb.Append($", id={Id}");
            sb.Append($", {PropertyMapToString()}");
            sb.Append("}");

            return sb.ToString();
        }
    }
}