using NRedisStack.Search.Literals;
namespace NRedisStack.Search
{
    public class FTSpellCheckParams
    {
        List<object> args = new List<object>();
        private List<KeyValuePair<string, string>> terms = new List<KeyValuePair<string, string>>();
        private int? distance = null;
        private int? dialect = null;

        public FTSpellCheckParams() { }

        /// <summary>
        ///  Specifies an inclusion (INCLUDE) of a custom dictionary.
        /// </summary>
        public FTSpellCheckParams IncludeTerm(string dict)
        {
            return AddTerm(dict, SearchArgs.INCLUDE);
        }

        /// <summary>
        ///  Specifies an inclusion (EXCLUDE) of a custom dictionary.
        /// </summary>
        public FTSpellCheckParams ExcludeTerm(string dict)
        {
            return AddTerm(dict, SearchArgs.EXCLUDE);
        }

        /// <summary>
        ///  Specifies an inclusion (INCLUDE) or exclusion (EXCLUDE) of a custom dictionary.
        /// </summary>
        private FTSpellCheckParams AddTerm(string dict, string type)
        {
            terms.Add(new KeyValuePair<string, string>(dict, type));
            return this;
        }

        /// <summary>
        ///  Maximum Levenshtein distance for spelling suggestions (default: 1, max: 4).
        /// </summary>
        public FTSpellCheckParams Distance(int distance)
        {
            this.distance = distance;
            return this;
        }

        /// <summary>
        ///  Selects the dialect version under which to execute the query.
        /// </summary>
        public FTSpellCheckParams Dialect(int dialect)
        {
            this.dialect = dialect;
            return this;
        }

        public List<object> GetArgs()
        {
            return args;
        }

        public void SerializeRedisArgs()
        {
            Distance();
            Terms();
            Dialect();
        }

        private void Dialect()
        {
            if (dialect != null)
            {
                args.Add(SearchArgs.DIALECT);
                args.Add(dialect);
            }
        }

        private void Terms()
        {
            foreach (var term in terms)
            {
                args.Add(SearchArgs.TERMS);
                args.Add(term.Value);
                args.Add(term.Key);
            }
        }

        private void Distance()
        {
            if (distance != null)
            {
                args.Add(SearchArgs.DISTANCE);
                args.Add(distance);
            }
        }
    }
}
