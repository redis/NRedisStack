using System.Text;
using System.Collections;
using System.Globalization;

namespace NRedisStack.Graph
{
    internal static class RedisGraphUtilities
    {
        internal static string PrepareQuery(string query, IDictionary<string, object> parms)
        {
            var preparedQuery = new StringBuilder();

            preparedQuery.Append("CYPHER ");

            foreach (var param in parms)
            {
                preparedQuery.Append($"{param.Key}={ValueToString(param.Value)} ");
            }

            preparedQuery.Append(query);

            return preparedQuery.ToString();
        }

        // TODO: Check if this is needed:
        // public static string ValueToStringNoQuotes(object value)
        // {
        //     if (value == null)
        //     {
        //         return "null";
        //     }

        //     if (value is IConvertible floatValue)
        //     {
        //         return ConvertibleToString(floatValue);
        //     }

        //     return value.ToString();
        // }

        public static string ValueToString(object value)
        {
            if (value == null)
            {
                return "null";
            }

            if (value is string stringValue)
            {
                return QuoteString(stringValue);
            }

            if (value is char charValue)
            {
                return QuoteCharacter(charValue);
            }

            if (value.GetType().IsArray)
            {
                if (value is IEnumerable arrayValue)
                {
                    var values = new List<object>();

                    foreach (var v in arrayValue)
                    {
                        values.Add(v);
                    }

                    return ArrayToString(values.ToArray());
                }
            }

            if ((value is IList valueList) && value.GetType().IsGenericType)
            {
                var objectValueList = new List<object>();

                foreach (var val in valueList)
                {
                    objectValueList.Add((object) val);
                }

                return ArrayToString(objectValueList.ToArray());
            }

            if (value is bool boolValue)
            {
                return boolValue.ToString().ToLowerInvariant();
            }

            if (value is IConvertible floatValue)
            {
                return ConvertibleToString(floatValue);
            }

            return value.ToString();
        }

        private static string ConvertibleToString(IConvertible floatValue)
        {
            return floatValue.ToString(CultureInfo.InvariantCulture);
        }

        private static string ArrayToString(object[] array)
        {
            var arrayElements = array.Select(x =>
            {
                if (x.GetType().IsArray)
                {
                    return ArrayToString((object[]) x);
                }
                else
                {
                    return ValueToString(x);
                }
            });

            var arrayToString = new StringBuilder();

            arrayToString.Append('[');
            arrayToString.Append(string.Join(", ", arrayElements));
            arrayToString.Append(']');

            return arrayToString.ToString();
        }

        internal static string QuoteCharacter(char character) =>
            $"\"{character}\"";

        internal static string QuoteString(string unquotedString)
        {
            var quotedString = new StringBuilder(unquotedString.Length + 12);

            quotedString.Append('"');
            quotedString.Append(unquotedString.Replace("\"", "\\\""));
            quotedString.Append('"');

            return quotedString.ToString();
        }
    }
}