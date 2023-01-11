namespace NRedisStack.Search.Aggregation
{
    public abstract class Reducer
    {
        // internal Reducer(string field) => _field = field;

        /// <summary>
        /// The name of the reducer
        /// </summary>
        public abstract string Name { get; }

        public string? Alias { get; set; }
        private readonly string _field;


        protected Reducer(string field)
        {
            _field = field;
            Alias = null;
        }

        //protected Reducer() : this(field: null) { }

        protected virtual int GetOwnArgsCount() => _field == null ? 0 : 1;
        protected virtual void AddOwnArgs(List<object> args)
        {
            if (_field != null) args.Add(_field);
        }

        public Reducer As(string alias)
        {
            Alias = alias;
            return this;
        }

        internal void SerializeRedisArgs(List<object> args)
        {
            int count = GetOwnArgsCount();
            args.Add(count);
            int before = args.Count;
            AddOwnArgs(args);
            int after = args.Count;
            if (count != (after - before))
                throw new InvalidOperationException($"Reducer '{ToString()}' incorrectly reported the arg-count as {count}, but added {after - before}");
        }
    }
}