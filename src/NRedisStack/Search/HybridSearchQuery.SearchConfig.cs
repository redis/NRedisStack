using System.Runtime.CompilerServices;

namespace NRedisStack.Search;

public sealed partial class HybridSearchQuery
{
    public readonly struct SearchConfig(string query, Scorer? scorer = null, string? scoreAlias = null)
    {
        private readonly string _query = query;
        private readonly Scorer? _scorer = scorer;
        private readonly string? _scoreAlias = scoreAlias;

        public static implicit operator SearchConfig(string query) => new(query);

        internal bool HasValue => _query is not null;

        /// <summary>
        /// The query string.
        /// </summary>
        public string Query => _query;

        /// <summary>
        /// Scoring algorithm for the query.
        /// </summary>
        public Scorer? Scorer => _scorer;

        /// <summary>
        /// Include the score in the query results.
        /// </summary>
        public string? ScoreAlias => _scoreAlias;

        /// <summary>
        /// Specify the scorer to use for the query.
        /// </summary>
        public SearchConfig WithScorer(Scorer? scorer)
        {
            var copy = this;
            Unsafe.AsRef(in copy._scorer) = scorer;
            return copy;
        }

        /// <summary>
        /// Specify the scorer to use for the query.
        /// </summary>
        public SearchConfig WithQuery(string query)
        {
            var copy = this;
            Unsafe.AsRef(in copy._query) = query;
            return copy;
        }

        /// <summary>
        /// Specify the scorer to use for the query.
        /// </summary>
        public SearchConfig WithScoreAlias(string? alias)
        {
            var copy = this;
            Unsafe.AsRef(in copy._scoreAlias) = alias;
            return copy;
        }

        internal int GetOwnArgsCount()
        {
            int count = 0;
            if (HasValue)
            {
                count += 2;
                if (Scorer != null) count += 1 + Scorer.GetOwnArgsCount();
                if (ScoreAlias != null) count += 2;
            }

            return count;
        }

        internal void AddOwnArgs(List<object> args)
        {
            if (HasValue)
            {
                args.Add("SEARCH");
                args.Add(Query);
                if (Scorer != null)
                {
                    args.Add("SCORER");
                    Scorer.AddOwnArgs(args);
                }

                if (ScoreAlias != null)
                {
                    args.Add("YIELD_SCORE_AS");
                    args.Add(ScoreAlias);
                }
            }
        }
    }
}