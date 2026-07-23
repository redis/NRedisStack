namespace NRedisStack.Search.Aggregation;

public static class Reducers
{
    public static Reducer Count() => new CountReducer(); // don't memoize; see https://github.com/redis/NRedisStack/issues/453
    private sealed class CountReducer : Reducer
    {
        internal CountReducer() : base(null) { }
        public override string Name => "COUNT";
    }

    private sealed class SingleFieldReducer : Reducer
    {
        public override string Name { get; }

        internal SingleFieldReducer(string name, string field) : base(field)
        {
            Name = name;
        }
    }

    public static Reducer CountDistinct(string field) => new SingleFieldReducer("COUNT_DISTINCT", field);

    public static Reducer CountDistinctish(string field) => new SingleFieldReducer("COUNT_DISTINCTISH", field);

    public static Reducer Sum(string field) => new SingleFieldReducer("SUM", field);

    public static Reducer Min(string field) => new SingleFieldReducer("MIN", field);

    public static Reducer Max(string field) => new SingleFieldReducer("MAX", field);

    public static Reducer Avg(string field) => new SingleFieldReducer("AVG", field);

    public static Reducer StdDev(string field) => new SingleFieldReducer("STDDEV", field);

    public static Reducer Quantile(string field, double percentile) => new QuantileReducer(field, percentile);

    private sealed class QuantileReducer : Reducer
    {
        private readonly double _percentile;
        public QuantileReducer(string field, double percentile) : base(field)
        {
            _percentile = percentile;
        }
        protected override int GetOwnArgsCount() => base.GetOwnArgsCount() + 1;
        protected override void AddOwnArgs(List<object> args)
        {
            base.AddOwnArgs(args);
            args.Add(_percentile);
        }
        public override string Name => "QUANTILE";
    }
    public static Reducer FirstValue(string field, SortedField? sortBy) => new FirstValueReducer(field, sortBy);
    private sealed class FirstValueReducer : Reducer
    {
        private readonly SortedField? _sortBy;
        public FirstValueReducer(string field, SortedField? sortBy) : base(field)
        {
            _sortBy = sortBy;
        }
        public override string Name => "FIRST_VALUE";

        // TODO: Check if needed
        // protected override int GetOwnArgsCount() => base.GetOwnArgsCount() + (_sortBy.HasValue ? 3 : 0);
        protected override void AddOwnArgs(List<object> args)
        {
            base.AddOwnArgs(args);
            if (_sortBy != null)
            {
                var sortBy = _sortBy;
                args.Add("BY");
                args.Add(sortBy.FieldName);
                args.Add(sortBy.Order.ToString());
            }
        }
    }
    public static Reducer FirstValue(string field) => FirstValue(field, null);

    public static Reducer ToList(string field) => new SingleFieldReducer("TOLIST", field);

    /// <summary>
    /// REDUCE COLLECT — gather per-document projections within a GROUPBY group and return them as an array of per-entry maps
    /// under the reducer alias, optionally sorted and bounded.
    /// <para>
    /// Configure the projected fields via <see cref="CollectReducer.Fields"/> or <see cref="CollectReducer.FieldsAll"/>, then
    /// optionally chain <see cref="CollectReducer.SortBy"/> and <see cref="CollectReducer.Limit(int, int)"/> before calling
    /// <see cref="Reducer.As(string)"/>.
    /// </para>
    /// <para>
    /// <b>Experimental.</b> Both the underlying Redis Search feature and this API may change. Before issuing COLLECT queries the
    /// server must be configured with <c>CONFIG SET search-enable-unstable-features yes</c>.
    /// </para>
    /// </summary>
    /// <seealso cref="CollectReducer"/>
    public static CollectReducer Collect() => new CollectReducer();

    public static Reducer RandomSample(string field, int size) => new RandomSampleReducer(field, size);

    private sealed class RandomSampleReducer : Reducer
    {
        private readonly int _size;
        public RandomSampleReducer(string field, int size) : base(field)
        {
            _size = size;
        }
        public override string Name => "RANDOM_SAMPLE";
        protected override int GetOwnArgsCount() => base.GetOwnArgsCount() + 1;
        protected override void AddOwnArgs(List<object> args)
        {
            base.AddOwnArgs(args);
            args.Add(_size);
        }
    }
}