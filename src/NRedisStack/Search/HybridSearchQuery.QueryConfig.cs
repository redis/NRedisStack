namespace NRedisStack.Search;

public sealed partial class HybridSearchQuery
{
    public sealed class QueryConfig
    {
        private Scorer? _scorer;

        /// <summary>
        ///  Scoring algorithm for the query.
        /// </summary>
        public QueryConfig Scorer(Scorer scorer)
        {
            _scorer = scorer;
            return this;
        }

        private string? _scoreAlias;

        /// <summary>
        /// Include the score in the query results.
        /// </summary>
        public QueryConfig ScoreAlias(string scoreAlias)
        {
            _scoreAlias = scoreAlias;
            return this;
        }

        internal int GetOwnArgsCount()
        {
            int count = 0;
            if (_scorer != null) count += 1 + _scorer.GetOwnArgsCount();
            if (_scoreAlias != null) count += 2;
            return count;
        }

        internal void AddOwnArgs(List<object> args)
        {
            if (_scorer != null)
            {
                args.Add("SCORER");
                _scorer.AddOwnArgs(args);
            }

            if (_scoreAlias != null)
            {
                args.Add("YIELD_SCORE_AS");
                args.Add(_scoreAlias);
            }
        }
    }
}