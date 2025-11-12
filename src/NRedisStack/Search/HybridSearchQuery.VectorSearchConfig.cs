using System.Buffers;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace NRedisStack.Search;

public sealed partial class HybridSearchQuery
{
    public readonly struct VectorSearchConfig(string fieldName, VectorData vectorData, VectorSearchMethod? method = null, string? filter = null, string? scoreAlias = null)
    {
        internal bool HasValue => _vectorData.HasValue & _fieldName is not null;

        private readonly string _fieldName = fieldName;
        private readonly VectorData _vectorData = vectorData;
        private readonly VectorSearchMethod? _method = method;
        private readonly string? _filter = filter;
        private readonly string? _scoreAlias = scoreAlias;

        /// <summary>
        /// The field name for vector search.
        /// </summary>
        public string FieldName => _fieldName;

        /// <summary>
        /// Vector search method configuration.
        /// </summary>
        public VectorSearchMethod? Method => _method;

        /// <summary>
        /// Filter expression for vector search.
        /// </summary>
        public string? Filter => _filter;

        /// <summary>
        /// Include the score in the query results.
        /// </summary>
        public string? ScoreAlias => _scoreAlias;

        /// <summary>
        /// The vector data to search for.
        /// </summary>
        public VectorData VectorData => _vectorData;

        /// <summary>
        /// Specify the vector search method.
        /// </summary>
        public VectorSearchConfig WithVectorData(VectorData vectorData)
        {
            var copy = this;
            Unsafe.AsRef(in copy._vectorData) = vectorData;
            return copy;
        }

        /// <summary>
        /// Specify the vector search method.
        /// </summary>
        public VectorSearchConfig WithMethod(VectorSearchMethod? method)
        {
            var copy = this;
            Unsafe.AsRef(in copy._method) = method;
            return copy;
        }

        /// <summary>
        /// Specify the field name for vector search.
        /// </summary>
        public VectorSearchConfig WithFieldName(string fieldName)
        {
            var copy = this;
            Unsafe.AsRef(in copy._fieldName) = fieldName;
            return copy;
        }

        /// <summary>
        /// Specify the filter expression.
        /// </summary>
        public VectorSearchConfig WithFilter(string? filter)
        {
            var copy = this;
            Unsafe.AsRef(in copy._filter) = filter;
            return copy;
        }

        /// <summary>
        /// Specify the score alias.
        /// </summary>
        public VectorSearchConfig WithScoreAlias(string? scoreAlias)
        {
            var copy = this;
            Unsafe.AsRef(in copy._scoreAlias) = scoreAlias;
            return copy;
        }

        internal int GetOwnArgsCount()
        {
            int count = 0;
            if (HasValue)
            {
                count += 2 + _vectorData.GetOwnArgsCount();
                if (_method != null) count += _method.GetOwnArgsCount();
                if (_filter != null) count += 2;

                if (_scoreAlias != null) count += 2;
            }
            return count;
        }

        internal void AddOwnArgs(List<object> args)
        {
            if (HasValue)
            {
                args.Add("VSIM");
                args.Add(_fieldName);
                _vectorData.AddOwnArgs(args);

                _method?.AddOwnArgs(args);
                if (_filter != null)
                {
                    args.Add("FILTER");
                    args.Add(_filter);
                }

                if (_scoreAlias != null)
                {
                    args.Add("YIELD_SCORE_AS");
                    args.Add(_scoreAlias);
                }
            }
        }
    }
}