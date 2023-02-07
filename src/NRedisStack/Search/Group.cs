namespace NRedisStack.Search.Aggregation
{
    public class Group
    {

        private readonly IList<Reducer> _reducers = new List<Reducer>();
        private readonly IList<string> _fields;
        private Limit _limit = Aggregation.Limit.NO_LIMIT;

        public Group(params string[] fields) => _fields = fields;
        public Group(IList<string> fields) => _fields = fields;

        internal Group Limit(Limit limit)
        {
            _limit = limit;
            return this;
        }

        internal Group Reduce(Reducer r)
        {
            _reducers.Add(r);
            return this;
        }

        internal void SerializeRedisArgs(List<object> args)
        {
            args.Add(_fields.Count);
            foreach (var field in _fields)
                args.Add(field);
            foreach (var r in _reducers)
            {
                args.Add("REDUCE");
                args.Add(r.Name);
                r.SerializeRedisArgs(args);
                var alias = r.Alias;
                if (!string.IsNullOrEmpty(alias))
                {
                    args.Add("AS");
                    args.Add(alias);
                }
            }
            _limit.SerializeRedisArgs(args);
        }
    }
}