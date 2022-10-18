using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NRedisStack.Graph
{
    /// <summary>
    /// Container for RedisGraph result values.
    /// </summary>
    public sealed class Record
    {
        /// <summary>
        /// Keys associated with a record.
        /// </summary>
        /// <value></value>
        public List<string> Keys { get; }

        /// <summary>
        /// Values associated with a record.
        /// </summary>
        /// <value></value>
        public List<object> Values { get; }

        internal Record(List<string> header, List<object> values)
        {
            Keys = header;
            Values = values;
        }

        /// <summary>
        /// Get a value by index.
        /// </summary>
        /// <param name="index">The index of the value you want to get.</param>
        /// <typeparam name="T">The type of the value at the index that you want to get.</typeparam>
        /// <returns>The value at the index that you specified.</returns>
        public T GetValue<T>(int index) => (T)Values[index];

        /// <summary>
        /// Get a value by key name.
        /// </summary>
        /// <param name="key">The key of the value you want to get.</param>
        /// <typeparam name="T">The type of the value that corresponds to the key that you specified.</typeparam>
        /// <returns>The value that corresponds to the key that you specified.</returns>
        public T GetValue<T>(string key) => (T)Values[Keys.IndexOf(key)];

        /// <summary>
        /// Gets the string representation of a value at the given index.
        /// </summary>
        /// <param name="index">The index of the value that you want to get.</param>
        /// <returns>The string value at the index that you specified.</returns>
        public string GetString(int index) => Values[index].ToString();

        /// <summary>
        /// Gets the string representation of a value by key.
        /// </summary>
        /// <param name="key">The key of the value that you want to get.</param>
        /// <returns>The string value at the key that you specified.</returns>
        public string GetString(string key) => Values[Keys.IndexOf(key)].ToString();

        /// <summary>
        /// Does the key exist in the record?
        /// </summary>
        /// <param name="key">The key to check.</param>
        /// <returns></returns>
        public bool ContainsKey(string key) => Keys.Contains(key);

        /// <summary>
        /// How many keys are in the record?
        /// </summary>
        public int Size => Keys.Count;

        /// <summary>
        /// Overridden method that compares the keys and values of a record with another record.
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            if (this == obj)
            {
                return true;
            }

            if (!(obj is Record that))
            {
                return false;
            }

            return Enumerable.SequenceEqual(Keys, that.Keys) && Enumerable.SequenceEqual(Values, that.Values);
        }

        /// <summary>
        /// Overridden method that generates a hash code based on the hash codes of the keys and values.
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            unchecked
            {
                int hash = 17;

                hash = hash * 31 + Keys.GetHashCode();
                hash = hash * 31 + Values.GetHashCode();

                return hash;
            }
        }

        /// <summary>
        /// Overridden method that emits a string of representing all of the values in a record.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            var sb = new StringBuilder();

            sb.Append("Record{values=");
            sb.Append(string.Join(",", Values));
            sb.Append('}');

            return sb.ToString();
        }
    }
}