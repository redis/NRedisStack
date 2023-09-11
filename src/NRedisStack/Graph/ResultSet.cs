using System.Collections;
using NRedisStack.Graph.DataTypes;
using StackExchange.Redis;

namespace NRedisStack.Graph
{
    /// <summary>
    /// Represents the result from a RedisGraph query.
    /// </summary>
    public sealed class ResultSet : IReadOnlyCollection<Record>
    {
        internal enum ResultSetScalarType
        {
            VALUE_UNKNOWN,
            VALUE_NULL,
            VALUE_STRING,
            VALUE_INT64,
            VALUE_BOOLEAN,
            VALUE_DOUBLE,
            VALUE_ARRAY,
            VALUE_EDGE,
            VALUE_NODE,
            VALUE_PATH,
            VALUE_MAP,
            VALUE_POINT
        }

        private readonly RedisResult[]? _rawResults;
        private readonly GraphCache? _graphCache;

        public Statistics Statistics { get; }
        public Header? Header { get; }
        public int Count { get; }

        internal ResultSet(RedisResult result, GraphCache graphCache)
        {
            if (result.Type == ResultType.MultiBulk)
            {
                var resultArray = (RedisResult[])result!;

                ScanForErrors(resultArray);

                _graphCache = graphCache;

                if (resultArray.Length == 3)
                {
                    Header = new Header(resultArray[0]);
                    Statistics = ParseStatistics(resultArray[2]);

                    _rawResults = (RedisResult[])resultArray[1]!;

                    Count = _rawResults.Length;
                }
                else
                {
                    Statistics = ParseStatistics(resultArray[resultArray.Length - 1]);
                    Count = 0;
                }
            }
            else
            {
                if (result.Type == ResultType.Error)
                {
                    throw new RedisServerException(result.ToString());
                }

                Statistics = ParseStatistics(result);
                Count = 0;
            }
        }

        /// <summary>
        /// Get the enumerator for this result set.
        /// </summary>
        /// <returns></returns>
        [Obsolete]
        public IEnumerator<Record> GetEnumerator() => RecordIterator().GetEnumerator();

        /// <summary>
        /// Get the enumerator for this result set.
        /// </summary>
        /// <returns></returns>
        [Obsolete]
        IEnumerator IEnumerable.GetEnumerator() => RecordIterator().GetEnumerator();

        [Obsolete]
        private IEnumerable<Record> RecordIterator()
        {
            if (_rawResults == default)
            {
                yield break;
            }
            else
            {
                foreach (RedisResult[]? row in _rawResults)
                {
                    var parsedRow = new List<object?>(row!.Length);

                    for (int i = 0; i < row.Length; i++)
                    {
                        var obj = (RedisResult[])row[i]!;
                        var objType = Header!.SchemaTypes[i];

                        switch (objType)
                        {
                            case Header.ResultSetColumnTypes.NODE:
                                parsedRow.Add(DeserializeNode(obj));
                                break;
                            case Header.ResultSetColumnTypes.RELATION:
                                parsedRow.Add(DeserializeEdge(obj));
                                break;
                            case Header.ResultSetColumnTypes.SCALAR:
                                parsedRow.Add(DeserializeScalar(obj));
                                break;
                            default:
                                parsedRow.Add(null);
                                break;
                        }
                    }

                    yield return new Record(Header!.SchemaNames, parsedRow!);
                }

                yield break;
            }
        }

        private Node DeserializeNode(RedisResult[] rawNodeData)
        {
            var node = new Node();

            DeserializeGraphEntityId(node, rawNodeData[0]);

            var labelIndices = (int[])rawNodeData[1];

            foreach (var labelIndex in labelIndices)
            {
                var label = _graphCache.GetLabel(labelIndex);

                node.Labels.Add(label);
            }

            DeserializeGraphEntityProperties(node, (RedisResult[])rawNodeData[2]);

            return node;
        }

        private Edge DeserializeEdge(RedisResult[] rawEdgeData)
        {
            var edge = new Edge();

            DeserializeGraphEntityId(edge, rawEdgeData[0]);

            edge.RelationshipType = _graphCache.GetRelationshipType((int)rawEdgeData[1]);
            edge.Source = (int)rawEdgeData[2];
            edge.Destination = (int)rawEdgeData[3];

            DeserializeGraphEntityProperties(edge, (RedisResult[])rawEdgeData[4]);

            return edge;
        }

        private object DeserializeScalar(RedisResult[] rawScalarData)
        {
            var type = GetValueTypeFromObject(rawScalarData[0]);

            switch (type)
            {
                case ResultSetScalarType.VALUE_NULL:
                    return null;
                case ResultSetScalarType.VALUE_BOOLEAN:
                    return bool.Parse((string)rawScalarData[1]);
                case ResultSetScalarType.VALUE_DOUBLE:
                    return (double)rawScalarData[1];
                case ResultSetScalarType.VALUE_INT64:
                    return (long)rawScalarData[1];
                case ResultSetScalarType.VALUE_STRING:
                    return (string)rawScalarData[1];
                case ResultSetScalarType.VALUE_ARRAY:
                    return DeserializeArray((RedisResult[])rawScalarData[1]);
                case ResultSetScalarType.VALUE_NODE:
                    return DeserializeNode((RedisResult[])rawScalarData[1]);
                case ResultSetScalarType.VALUE_EDGE:
                    return DeserializeEdge((RedisResult[])rawScalarData[1]);
                case ResultSetScalarType.VALUE_PATH:
                    return DeserializePath((RedisResult[])rawScalarData[1]);
                case ResultSetScalarType.VALUE_MAP:
                    return DeserializeDictionary(rawScalarData[1]);
                case ResultSetScalarType.VALUE_POINT:
                    return DeserializePoint((RedisResult[])rawScalarData[1]);
                case ResultSetScalarType.VALUE_UNKNOWN:
                default:
                    return (object)rawScalarData[1];
            }
        }

        private static void DeserializeGraphEntityId(GraphEntity graphEntity, RedisResult rawEntityId) =>
            graphEntity.Id = (int)rawEntityId;

        private void DeserializeGraphEntityProperties(GraphEntity graphEntity, RedisResult[] rawProperties)
        {
            foreach (RedisResult[] rawProperty in rawProperties)
            {
                var Key = _graphCache.GetPropertyName((int)rawProperty[0]);
                var Value = DeserializeScalar(rawProperty.Skip(1).ToArray());

                graphEntity.PropertyMap.Add(Key, Value);

            }
        }

        private object[] DeserializeArray(RedisResult[] serializedArray)
        {
            var result = new object[serializedArray.Length];

            for (var i = 0; i < serializedArray.Length; i++)
            {
                result[i] = DeserializeScalar((RedisResult[])serializedArray[i]);
            }

            return result;
        }

        private DataTypes.Path DeserializePath(RedisResult[] rawPath)
        {
            var deserializedNodes = (object[])DeserializeScalar((RedisResult[])rawPath[0]);
            var nodes = Array.ConvertAll(deserializedNodes, n => (Node)n);

            var deserializedEdges = (object[])DeserializeScalar((RedisResult[])rawPath[1]);
            var edges = Array.ConvertAll(deserializedEdges, p => (Edge)p);

            return new DataTypes.Path(nodes, edges);
        }

        private object DeserializePoint(RedisResult[] rawPath) // Should return Point?
        {
            if (null == rawPath)
            {
                return null;
            }
            // List<object> values = (List<object>)rawPath;
            List<Double> doubles = new List<Double>(rawPath.Count());
            foreach (var value in rawPath)
            {
                doubles.Add(((double)value));
            }
            return new Point(doubles);
        }

        // @SuppressWarnings("unchecked")
        private Dictionary<string, object> DeserializeDictionary(RedisResult rawPath)
        {
            RedisResult[] keyTypeValueEntries = (RedisResult[])rawPath;

            int size = keyTypeValueEntries.Length;
            Dictionary<string, object> dict = new Dictionary<string, object>(size / 2); // set the capacity to half of the list

            for (int i = 0; i < size; i += 2)
            {
                string key = keyTypeValueEntries[i].ToString();
                object value = DeserializeScalar((RedisResult[])keyTypeValueEntries[i + 1]);
                dict.Add(key, value);
            }
            return dict;
        }

        private static ResultSetScalarType GetValueTypeFromObject(RedisResult rawScalarType) =>
        (ResultSetScalarType)(int)rawScalarType;

        private static void ScanForErrors(RedisResult[] results)
        {
            foreach (var result in results)
            {
                if (result.Type == ResultType.Error)
                {
                    throw new RedisServerException(result.ToString());
                }
            }
        }

        private Statistics ParseStatistics(RedisResult result)
        {
            RedisResult[] statistics;

            if (result.Type == ResultType.MultiBulk)
            {
                statistics = (RedisResult[])result;
            }
            else
            {
                statistics = new[] { result };
            }

            return new Statistics(
                ((RedisResult[])statistics).Select(x =>
                    {
                        var s = ((string)x).Split(':');

                        return new
                        {
                            Label = s[0].Trim(),
                            Value = s[1].Trim()
                        };
                    }).ToDictionary(k => k.Label, v => v.Value));
        }
    }
}