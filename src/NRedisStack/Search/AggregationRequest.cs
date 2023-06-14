using NRedisStack.Search.Literals;

namespace NRedisStack.Search.Aggregation
{
    public class AggregationRequest
    {
        private List<object> args = new List<object>(); // Check if Readonly
        private bool isWithCursor = false;

        // Parameters:
<<<<<<< HEAD
=======
        private int? dialect = 2; // Set default value to DIACLECT 2
>>>>>>> ac447f1 (add SerializeRedisArgs for FT.AGGREGATE)

        private bool? verbatim = null;

        // Load
        private List<FieldName> fieldNames = new List<FieldName>(); // TODO: Check if the new list suposed to be here
        private bool? loadAll = null;

        private long? timeout = null;

        // GroupBy:
        private List<Group> groups = new List<Group>();

        // SotrBy:
        private List<SortedField> sortedFields = new List<SortedField>();
        private int? max = null;

        // Apply:
        private List<Tuple<string, string>> apply = new List<Tuple<string, string>>();

        // Limit:
        private int? offset = null;
        private int? num = null;

        private string? filter = null;

        // WithCursor:
        private int? count = null;
        private long? maxIdle = null;

        // Params:
        private Dictionary<string, object> nameValue = new Dictionary<string, object>();
        public int? dialect {get; private set;} = null;

        public AggregationRequest(string query, int? defaultDialect = null)
        {
            this.dialect = defaultDialect;
            args.Add(query);
        }

        public AggregationRequest() : this("*") { }

        public AggregationRequest Verbatim(bool verbatim = true)
        {
            this.verbatim = true;
            return this;
        }

        private void Verbatim()
        {
            if(verbatim == true)
                args.Add("VERBATIM");
        }

        public AggregationRequest Verbatim(bool verbatim = true)
        {
            this.verbatim = true;
            return this;
        }

        private void Verbatim()
        {
            if(verbatim == true)
                args.Add("VERBATIM");
        }

        public AggregationRequest Load(params FieldName[] fields)
        {
            this.fieldNames.AddRange(fields);
            return this;
        }

        public AggregationRequest LoadAll()
        {
            loadAll = true;
            return this;
        }

        private void Load()
        {
            if (loadAll == true)
            {
                args.Add("LOAD");
                args.Add("*");
                return;
            }
            else if (fieldNames.Count > 0)
            {
<<<<<<< HEAD
                args.Add("LOAD");
                int loadCountIndex = args.Count;
                //args.Add(null);
=======
                int loadCountIndex = args.Count;
                args.Add(null);
>>>>>>> ac447f1 (add SerializeRedisArgs for FT.AGGREGATE)
                int loadCount = 0;
                foreach (FieldName fn in fieldNames)
                {
                    loadCount += fn.AddCommandArguments(args);
                }
<<<<<<< HEAD

                args.Insert(loadCountIndex, loadCount);
                // args[loadCountIndex] = loadCount.ToString();
=======
                args.Insert(loadCountIndex, loadCount.ToString());
>>>>>>> ac447f1 (add SerializeRedisArgs for FT.AGGREGATE)
            }
        }

        public AggregationRequest Timeout(long timeout)
        {
            this.timeout = timeout;
            return this;
        }

        private void Timeout()
        {
            if (timeout != null)
            {
                args.Add("TIMEOUT");
                args.Add(timeout);
            }
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

        public AggregationRequest GroupBy(string field, params Reducer[] reducers)
        {
            return GroupBy(new string[] { field }, reducers);
        }

        public AggregationRequest GroupBy(Group group)
        {
            this.groups.Add(group);
            return this;
        }

        private void GroupBy()
        {
            if (groups.Count > 0)
            {
                args.Add("GROUPBY");
                foreach (Group group in groups)
                {
                    group.SerializeRedisArgs(args);
                }
            }
        }

        public AggregationRequest SortBy(string property) => SortBy(SortedField.Asc(property));

        public AggregationRequest SortBy(params SortedField[] fields)
        {
            this.sortedFields.AddRange(fields);
            return this;
        }

        private void SortBy()
        {
            if (sortedFields.Count > 0)
            {
                args.Add("SORTBY");
                args.Add(sortedFields.Count * 2);
                foreach (SortedField field in sortedFields)
                {
                    args.Add(field.FieldName);
<<<<<<< HEAD
                    args.Add(field.Order.ToString());
=======
                    args.Add(field.Order);
>>>>>>> ac447f1 (add SerializeRedisArgs for FT.AGGREGATE)
                }

                if (max > 0)
                {
                    args.Add("MAX");
                    args.Add(max);
                }
            }
        }

        public AggregationRequest SortBy(int max, params SortedField[] fields)
        {
            this.max = max;
            SortBy(fields);
            return this;
        }

        public AggregationRequest Apply(string projection, string alias)
        {
            apply.Add(new Tuple<string, string>(projection, alias));
            return this;
        }

        private void Apply()
        {
            if (apply.Count > 0)
            {
                foreach (Tuple<string, string> tuple in apply)
                {
                    args.Add("APPLY");
                    args.Add(tuple.Item1);
                    args.Add("AS");
                    args.Add(tuple.Item2);
                }
            }
        }

        public AggregationRequest Limit(int count) => Limit(0, count);

        public AggregationRequest Limit(int offset, int count)
        {
            this.offset = offset;
            this.num = count;

            return this;
        }

        private void Limit()
        {
            if (offset != null && num != null)
            {
                new Limit(offset.Value, num.Value).SerializeRedisArgs(args);
            }
        }

        public AggregationRequest Filter(string filter)
        {
            this.filter = filter;
            return this;
        }

        private void Filter()
        {
            if (filter != null)
            {
                args.Add(SearchArgs.FILTER);
                args.Add(filter!);
            }

        }

        public AggregationRequest Cursor(int? count = null, long? maxIdle = null)
        {
            isWithCursor = true;
            if (count != null)
                this.count = count;
            if (maxIdle != null)
                this.maxIdle = maxIdle;
            return this;
        }

        private void Cursor()
        {
            if (isWithCursor)
            {
                args.Add("WITHCURSOR");

                if (count != null)
                {
                    args.Add("COUNT");
                    args.Add(count);
                }

                if (maxIdle != null && maxIdle < long.MaxValue && maxIdle >= 0)
                {
                    args.Add("MAXIDLE");
                    args.Add(maxIdle);
                }
            }
        }

        public AggregationRequest Params(Dictionary<string, object> nameValue)
        {
            foreach (var entry in nameValue)
            {
                this.nameValue.Add(entry.Key, entry.Value);
            }
            return this;
        }

        private void Params()
        {
            if (nameValue.Count > 0)
            {
                args.Add("PARAMS");
                args.Add(nameValue.Count * 2);
                foreach (var entry in nameValue)
                {
                    args.Add(entry.Key);
                    args.Add(entry.Value);
                }
            }
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
                args.Add("DIALECT");
                args.Add(dialect);
            }
        }

        public List<object> GetArgs()
        {
            return args;
        }

        public void SerializeRedisArgs()
        {
            Verbatim();
            Load();
            Timeout();
<<<<<<< HEAD
            Apply();
            GroupBy();
            SortBy();
=======
            GroupBy();
            SortBy();
            Apply();
>>>>>>> ac447f1 (add SerializeRedisArgs for FT.AGGREGATE)
            Limit();
            Filter();
            Cursor();
            Params();
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
