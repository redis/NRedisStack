using System.Collections;
using System.Text;

namespace NRedisStack.Graph
{
    /// <summary>
    /// A graph entity property.
    /// </summary>
    public class Property
    {
        /// <summary>
        /// Name of the property.
        /// </summary>
        /// <value></value>
        public string Name { get; set; }

        /// <summary>
        /// Value of the property.
        /// </summary>
        /// <value></value>
        public object Value { get; set; }

        internal Property()
        { }

        /// <summary>
        /// Create a property by specifying a name and a value.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="value"></param>
        public Property(string name, object value)
        {
            Name = name;
            Value = value;
        }

        /// <summary>
        /// Overridden method that considers the equality of the name and the value of two property instances.
        /// </summary>
        /// <param name="obj">Another instance of the property class.</param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            if (this == obj)
            {
                return true;
            }

            if (!(obj is Property that))
            {
                return false;
            }

            return Name == that.Name && Object.Equals(Value, that.Value);
        }

        /// <summary>
        /// Overridden method that computes the hash code of the class using the name and value of the property.
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            unchecked
            {
                int hash = 17;

                hash = hash * 31 + Name.GetHashCode();

                if (Value is IEnumerable enumerableValue)
                {
                    foreach(var value in enumerableValue)
                    {
                        hash = hash * 31 + value.GetHashCode();
                    }
                }
                else
                {
                    hash = hash * 31 + Value.GetHashCode();
                }

                return hash;
            }
        }

        /// <summary>
        /// Overridden method that emits a string containing the name and value of the property.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            var stringResult = new StringBuilder();

            stringResult.Append("Property{");
            stringResult.Append("name='");
            stringResult.Append(Name);
            stringResult.Append('\'');
            stringResult.Append(", value=");
            stringResult.Append(RedisGraphUtilities.ValueToStringNoQuotes(Value));



            stringResult.Append('}');

            return stringResult.ToString();
        }
    }
}