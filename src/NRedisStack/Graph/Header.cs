using System;
using System.Collections.Generic;
using System.Linq;
using StackExchange.Redis;
// TODO: check if this is still needed
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
            /// <summary>
            /// Who can say?
            /// </summary>
            UNKNOWN,

            /// <summary>
            /// A single value.
            /// </summary>
            SCALAR,

            /// <summary>
            /// Refers to an actual node.
            /// </summary>
            NODE,

            /// <summary>
            /// Refers to a relation.
            /// </summary>
            RELATION
        }

        /// <summary>
        /// Collection of the schema types present in the header.
        /// </summary>
        /// <value></value>
        [Obsolete("SchemaType is no longer supported after RedisGraph 2.1 and will always return COLUMN_SCALAR")] // TODO: CHeck This
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

        public override bool Equals(object obj)
        {
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

        public override string ToString() =>
            $"Header{{schemaTypes=[{string.Join(", ", SchemaTypes)}], schemaNames=[{string.Join(", ", SchemaNames)}]}}";
    }
}