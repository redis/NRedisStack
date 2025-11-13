using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;

namespace NRedisStack.Search;

/// <summary>
/// Create query parameters from an object template.
/// </summary>
public static class Parameters
{
    /// <summary>
    /// Create parameters from an object template.
    /// </summary>
    public static IReadOnlyDictionary<string, object> From<T>(T obj)
        => new TypedParameters<T>(obj);

    private sealed class TypedParameters<T>(T obj) : IReadOnlyDictionary<string, object>
    {
        // ReSharper disable once InconsistentNaming
        private static readonly PropertyInfo[] s_properties =
            typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanRead
                            && p.GetGetMethod() is not null
                            && !p.PropertyType.IsByRef
#if NET || NETSTANDARD2_1_OR_GREATER
                            && !p.PropertyType.IsByRefLike
#else
                            && !p.PropertyType.GetCustomAttributes().Any(x => x.GetType().FullName == "System.Runtime.CompilerServices.IsByRefLikeAttribute")
#endif
                            ).ToArray();

        public IEnumerator<KeyValuePair<string, object>> GetEnumerator()
        {
            foreach (var prop in s_properties)
            {
                var value = prop.GetValue(obj);
                if (value is not null)
                {
                    yield return new KeyValuePair<string, object>(prop.Name, value);
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public int Count => s_properties.Length;
        public bool ContainsKey(string key) => TryGetValue(key, out _); // because we need the null-check

        public bool TryGetValue(string key, out object value)
        {
            foreach (var prop in s_properties)
            {
                if (prop.Name == key)
                {
                    value = prop.GetValue(obj)!;
                    // ReSharper disable once ConditionIsAlwaysTrueOrFalseAccordingToNullableAPIContract
                    return value is not null;
                }
            }

            value = null!;
            return false;
        }

        public object this[string key] => TryGetValue(key, out var value) ? value : throw new KeyNotFoundException();

        public IEnumerable<string> Keys
        {
            get
            {
                foreach (var prop in s_properties)
                {
                    var value = prop.GetValue(obj);
                    if (value is not null)
                    {
                        yield return prop.Name;
                    }
                }
            }
        }

        public IEnumerable<object> Values
        {
            get
            {
                foreach (var prop in s_properties)
                {
                    var value = prop.GetValue(obj);
                    if (value is not null)
                    {
                        yield return value;
                    }
                }
            }
        }
    }
}
