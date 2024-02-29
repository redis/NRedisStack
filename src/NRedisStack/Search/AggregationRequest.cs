using NRedisStack.Search.Literals;

namespace NRedisStack.Search.Aggregation
{
    public class AggregationRequest
    {
        private List<object> args = new List<object>(); // Check if Readonly
        private bool isWithCursor = false;

        public int? dialect { get; private set; } = null;

        public AggregationRequest(string query, int? defaultDialect = null)
        {
            this.dialect = defaultDialect;
            args.Add(query);
        }

        public AggregationRequest() : this("*") { }

        public AggregationRequest Verbatim()
        {
            args.Add(SearchArgs.VERBATIM);
            return this;
        }

        public AggregationRequest Load(params FieldName[] fields)
        {
            if (fields.Length > 0)
            {
                args.Add(SearchArgs.LOAD);
                int loadCountIndex = args.Count;
                //args.Add(null);
                int loadCount = 0;
                foreach (FieldName fn in fields)
                {
                    loadCount += fn.AddCommandArguments(args);
                }

                args.Insert(loadCountIndex, loadCount);
                // args[loadCountIndex] = loadCount.ToString();
            }
            return this;
        }

        public AggregationRequest LoadAll()
        {
            args.Add(SearchArgs.LOAD);
            args.Add("*");
            return this;
        }

        public AggregationRequest Timeout(long timeout)
        {
            args.Add(SearchArgs.TIMEOUT);
            args.Add(timeout);
            return this;
        }



        public AggregationRequest GroupBy(string field, params Reducer[] reducers)
        {
            return GroupBy(new string[] { field }, reducers);
        }

        public AggregationRequest GroupBy(IList<string> fields, IList<Reducer> reducers)
        {
            Group g = new Group(fields);
            foreach (Reducer r in reducers)
            {
                g.Reduce(r);
            }
            GroupBy(g);
            return this;
        }

        public AggregationRequest GroupBy(Group group)
        {
            args.Add(SearchArgs.GROUPBY);
            group.SerializeRedisArgs(args);
            return this;
        }

        public AggregationRequest SortBy(string property) => SortBy(SortedField.Asc(property));

        public AggregationRequest SortBy(params SortedField[] fields) => SortBy(-1, fields);


        public AggregationRequest SortBy(int max, params SortedField[] fields) // TODO: check if it should be params
        {
            if (fields.Length > 0)
            {
                args.Add(SearchArgs.SORTBY);
                args.Add(fields.Length * 2);
                foreach (SortedField field in fields)
                {
                    args.Add(field.FieldName);
                    args.Add(field.Order.ToString());
                }

                if (max > 0)
                {
                    args.Add(SearchArgs.MAX);
                    args.Add(max);
                }
            }
            return this;
        }

        public AggregationRequest Apply(string projection, string alias)
        {
            args.Add(SearchArgs.APPLY);
            args.Add(projection);
            args.Add(SearchArgs.AS);
            args.Add(alias);
            return this;
        }

        public AggregationRequest Limit(int count) => Limit(0, count);

        public AggregationRequest Limit(int offset, int count)
        {
            new Limit(offset, count).SerializeRedisArgs(args);
            return this;
        }

        public AggregationRequest Filter(string filter)
        {
            args.Add(SearchArgs.FILTER);
            args.Add(filter!);
            return this;
        }

        public AggregationRequest Cursor(int? count = null, long? maxIdle = null)
        {
            isWithCursor = true;
            args.Add(SearchArgs.WITHCURSOR);

            if (count != null)
            {
                args.Add(SearchArgs.COUNT);
                args.Add(count);
            }

            if (maxIdle != null && maxIdle < long.MaxValue && maxIdle >= 0)
            {
                args.Add(SearchArgs.MAXIDLE);
                args.Add(maxIdle);
            }
            return this;
        }

        public AggregationRequest Params(Dictionary<string, object> nameValue)
        {
            if (nameValue.Count > 0)
            {
                args.Add(SearchArgs.PARAMS);
                args.Add(nameValue.Count * 2);
                foreach (var entry in nameValue)
                {
                    args.Add(entry.Key);
                    args.Add(entry.Value);
                }
            }
            return this;
        }

        public AggregationRequest Dialect(int dialect)
        {
            this.dialect = dialect;
            return this;
        }

        private void Dialect()
        {
            if (dialect != null)
            {
                args.Add(SearchArgs.DIALECT);
                args.Add(dialect);
            }
        }

        public List<object> GetArgs()
        {
            return args;
        }

        public void SerializeRedisArgs()
        {
            // Verbatim();
            // Load();
            // Timeout();
            // Apply();
            // GroupBy();
            // SortBy();
            // Limit();
            // Filter();
            // Cursor();
            // Params();
            Dialect();
        }

        // public string getArgsstring()
        // {
        //     StringBuilder sj = new StringBuilder(" ");
        //     foreach (var s in GetArgs())
        //     {
        //         sj.Add(s.ToString());
        //     }
        //     return sj.tostring();
        // }

        public bool IsWithCursor()
        {
            return isWithCursor;
        }
    }
}
