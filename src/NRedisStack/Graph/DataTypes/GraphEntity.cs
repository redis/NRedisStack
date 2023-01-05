using System.Text;

namespace NRedisStack.Graph.DataTypes
{
    /// <summary>
    /// An abstract representation of a graph entity.
    /// A graph entity has an ID and a set of properties. The properties are mapped and accessed by their names.
    /// </summary>
    public abstract class GraphEntity
    {
        public long Id { get; set; }

        public IDictionary<string, object> PropertyMap = new Dictionary<string, object>();

        // TODO: check if this is needed:
        /// <summary>
        /// Overriden Equals that considers the equality of the entity ID as well as the equality of the
        /// properties that each entity has.
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

            if (!(obj is GraphEntity that))
            {
                return false;
            }

            return Id == that.Id && (PropertyMap.SequenceEqual(that.PropertyMap));
        }

        /// <summary>
        /// Overriden GetHashCode that computes a deterministic hash code based on the value of the ID
        /// and the name/value of each of the associated properties.
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            unchecked
            {
                int hash = 17;

                hash = hash * 31 + Id.GetHashCode();

                foreach(var prop in PropertyMap)
                {
                    hash = hash * 31 + prop.Key.GetHashCode();
                    hash = hash * 31 + prop.Value.GetHashCode();
                }

                return hash;
            }
        }

        // TODO: Delete it?
        /// <summary>
        /// Overriden ToString that emits a string containing the ID and property map of the entity.
        /// </summary>
        /// <returns></returns>
        // public override string ToString()
        // {
        //     var sb = new StringBuilder();

        //     sb.Append("GraphEntity{id=");
        //     sb.Append(Id);
        //     sb.Append(", propertyMap=");
        //     sb.Append(PropertyMap);
        //     sb.Append('}');

        //     return sb.ToString();
        // }

        public string PropertyMapToString()
        {
            var sb = new StringBuilder();

            sb.Append("propertyMap={");
            sb.Append(string.Join(", ", PropertyMap.Select(pm => $"{pm.Key}={pm.Value}")));
            sb.Append("}");

            return sb.ToString();
        }
    }
}