using System.Collections.Generic;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("NRedisGraph.Tests")]

namespace NRedisStack.Graph
{
    internal static class IDictionaryExtensions
    {
        internal static void PutIfAbsent<TKey, TValue>(this IDictionary<TKey, TValue> @this, TKey key, TValue value)
        {
            if (!@this.ContainsKey(key))
            {
                @this.Add(key, value);
            }
        }

        internal static void Put<TKey, TValue>(this IDictionary<TKey, TValue> @this, TKey key, TValue value)
        {
            if (@this.ContainsKey(key))
            {
                @this[key] = value;
            }
            else
            {
                @this.Add(key, value);
            }
        }

        internal static bool SequenceEqual<TKey, TValue>(this IDictionary<TKey, TValue> @this, IDictionary<TKey, TValue> that)
        {
            if (@this == default(IDictionary<TKey, TValue>) || that == default(IDictionary<TKey, TValue>))
            {
                return false;
            }

            if (@this.Count != that.Count)
            {
                return false;
            }

            foreach (var key in @this.Keys)
            {
                var thisValue = @this[key];
                var thatValue = that[key];

                if (!thisValue.Equals(thatValue))
                {
                    return false;
                }
            }

            return true;
        }
    }
}