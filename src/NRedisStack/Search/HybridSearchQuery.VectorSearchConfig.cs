namespace NRedisStack.Search;

public sealed partial class HybridSearchQuery
{
    public sealed class VectorSearchConfig
    {
        private string? _filter;
        private VectorFilterPolicy? _filterPolicy;
        private int? _filterBatchSize;

        /// <summary>
        /// Pre-filter for VECTOR results
        /// </summary>
        public VectorSearchConfig Filter(string filter, VectorFilterPolicy? policy = null, int? batchSize = null)
        {
            _filter = filter;
            _filterPolicy = policy;
            _filterBatchSize = batchSize;
            return this;
        }

        /// <summary>
        /// The filter policy to apply
        /// </summary>
        public enum VectorFilterPolicy
        {
            AdHoc,
            Batches,
            Acorn,
        }

        private string? _scoreAlias;

        /// <summary>
        /// Include the score in the query results.
        /// </summary>
        public VectorSearchConfig ScoreAlias(string scoreAlias)
        {
            _scoreAlias = scoreAlias;
            return this;
        }


        private VectorSearchMethod? _method;

        /// <summary>
        /// The method to use for vector search.
        /// </summary>
        public VectorSearchConfig Method(VectorSearchMethod method)
        {
            _method = method;
            return this;
        }

        internal int GetOwnArgsCount()
        {
            int count = 0;
            if (_method != null) count += _method.GetOwnArgsCount();
            if (_filter != null)
            {
                count += 2;
                if (_filterPolicy != null)
                {
                    count += 2;
                    if (_filterBatchSize != null) count += 2;
                }
            }

            if (_scoreAlias != null) count += 2;
            return count;
        }

        internal void AddOwnArgs(List<object> args)
        {
            _method?.AddOwnArgs(args);
            if (_filter != null)
            {
                args.Add("FILTER");
                args.Add(_filter);
                if (_filterPolicy != null)
                {
                    args.Add("POLICY");
                    args.Add(_filterPolicy switch
                    {
                        VectorFilterPolicy.AdHoc => "ADHOC",
                        VectorFilterPolicy.Batches => "BATCHES",
                        VectorFilterPolicy.Acorn => "ACORN",
                        _ => _filterPolicy.ToString()!,
                    });
                    if (_filterBatchSize != null)
                    {
                        args.Add("BATCH_SIZE");
                        args.Add(_filterBatchSize);
                    }
                }
            }

            if (_scoreAlias != null)
            {
                args.Add("YIELD_SCORE_AS");
                args.Add(_scoreAlias);
            }
        }
    }
}