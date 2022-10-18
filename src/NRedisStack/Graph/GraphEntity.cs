using System.Text;
using System.Collections.Generic;

namespace NRedisStack.Graph
{
    /// <summary>
    /// An abstract representation of a graph entity.
    ///
    /// A graph entity has an ID and a set of properties. The properties are mapped and accessed by their names.
    /// </summary>
    public abstract class GraphEntity
    {
        /// <summary>
        /// The ID of the entity.
        /// </summary>
        /// <value></value>
        public int Id { get; set; }

        /// <summary>
        /// The collection of properties associated with an entity.
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, Property> PropertyMap = new Dictionary<string, Property>();

        /// <summary>
        /// Add a property to the entity.
        /// </summary>
        /// <param name="name">Name of the property.</param>
        /// <param name="value">Value of the property.</param>
        public void AddProperty(string name, object value) =>
            AddProperty(new Property(name, value));

        /// <summary>
        /// Add a property to the entity.
        /// </summary>
        /// <param name="property">The property to add.</param>
        public void AddProperty(Property property) => PropertyMap.Add(property.Name, property);

        /// <summary>
        /// Remove a property from the entity by name.
        /// </summary>
        /// <param name="name"></param>
        public void RemoveProperty(string name) => PropertyMap.Remove(name);

        /// <summary>
        /// How many properties does this entity have?
        /// </summary>
        public int NumberOfProperties => PropertyMap.Count;

        /// <summary>
        /// Overriden Equals that considers the equality of the entity ID as well as the equality of the
        /// properties that each entity has.
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            if (this == obj)
            {
                return true;
            }

            if (!(obj is GraphEntity that))
            {
                return false;
            }

            return Id == that.Id && PropertyMap.SequenceEqual(that.PropertyMap);
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

        /// <summary>
        /// Overriden ToString that emits a string containing the ID and property map of the entity.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            var sb = new StringBuilder();

            sb.Append("GraphEntity{id=");
            sb.Append(Id);
            sb.Append(", propertyMap=");
            sb.Append(PropertyMap);
            sb.Append('}');

            return sb.ToString();
        }
    }
}