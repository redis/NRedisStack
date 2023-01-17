using StackExchange.Redis;
namespace NRedisStack.Graph
{
    /// <summary>
    /// Query response header interface. Represents the response schema (column names and types).
    /// </summary>
    public sealed class Header
    {
        /// <summary>
        /// The expected column types.
        /// </summary>
        public enum ResultSetColumnTypes
        {
            UNKNOWN,
            SCALAR,
            NODE,
            RELATION
        }

        /// <summary>
        /// Collection of the schema types present in the header.
        /// </summary>
        [Obsolete("SchemaType is no longer supported after RedisGraph 2.1 and will always return COLUMN_SCALAR")]
        public List<ResultSetColumnTypes> SchemaTypes { get; }

        /// <summary>
        /// Collection of the schema names present in the header.
        /// </summary>
        /// <value></value>
        public List<string> SchemaNames { get; }

        internal Header(RedisResult result)
        {
            SchemaTypes = new List<ResultSetColumnTypes>();
            SchemaNames = new List<string>();

            foreach(RedisResult[] tuple in (RedisResult[])result)
            {
                SchemaTypes.Add((ResultSetColumnTypes)(int)tuple[0]);
                SchemaNames.Add((string)tuple[1]);
            }
        }

        public override bool Equals(object? obj)
        {
            if (obj == null) return this == null;

            if (this == obj)
            {
                return true;
            }

            var header = obj as Header;

            if (header is null)
            {
                return false;
            }

            return Object.Equals(SchemaTypes, header.SchemaTypes)
                && Object.Equals(SchemaNames, header.SchemaNames);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(SchemaTypes, SchemaNames);
        }

        public override string ToString() =>
            $"Header{{schemaTypes=[{string.Join(", ", SchemaTypes)}], schemaNames=[{string.Join(", ", SchemaNames)}]}}";
    }
}