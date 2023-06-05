using NRedisStack.Search.Literals;

namespace NRedisStack.Search.Aggregation
{
    public class AggregationRequest
    {
        private List<object> args = new List<object>(); // Check if Readonly
        private bool isWithCursor = false;

        // Parameters:
        private int dialect = 2;

        private bool? verbatim = null;
        private List<FieldName> fieldNames = new List<FieldName>(); // TODO: Check if the new list suposed to be here
        private long? timeout = null;
        private bool? loadAll = null;
        private List<Group> groups = new List<Group>();
        // SotrBy:
        private List<SortedField> sortedFields = new List<SortedField>();
        private int? max = null;

        // Apply:
        private List<Tuple<string, string>> apply = new List<Tuple<string, string>>();
        private Limit? limit = null;
        private string? filter = null;
        private int? count = null;
        private long? maxIdle = null;
        private Dictionary<string, object> nameValue = new Dictionary<string, object>();

        public AggregationRequest(string query)
        {
            args.Add(query);
        }

        public AggregationRequest() : this("*") { }

        // public AggregationRequest load(params string[] fields)
        // {
        //     return load(FieldName.Convert(fields));
        // }

        public AggregationRequest Load(params FieldName[] fields)
        {
            this.fieldNames.AddRange(fields);
            return this;
        }

        private void Load()
        {
            args.Add("LOAD");
            if (loadAll == true)
            {
                args.Add("*");
                return;
            }
            int loadCountIndex = args.Count();
            args.Add(null);
            int loadCount = 0;
            foreach (FieldName fn in fieldNames)
            {
                loadCount += fn.AddCommandArguments(args);
            }
            args.Insert(loadCountIndex, loadCount.ToString());
        }

        public AggregationRequest LoadAll()
        {
            loadAll = true;
            return this;
        }

        // private void LoadAll()
        // {
        //     args.Add("LOAD");
        //     args.Add("*");
        // }

        public AggregationRequest Limit(int offset, int count)
        {
            Limit limit = new Limit(offset, count);
            limit.SerializeRedisArgs(args);
            return this;
        }

        public AggregationRequest Limit(int count) => Limit(0, count);

        public AggregationRequest SortBy(string property) => SortBy(SortedField.Asc(property));

        public AggregationRequest SortBy(params SortedField[] fields)
        {
            this.sortedFields.AddRange(fields);
            return this;
        }

        private void SortBy()
        {
            args.Add("SORTBY");
            args.Add(sortedFields.Count * 2);
            foreach (SortedField field in sortedFields)
            {
                args.Add(field.FieldName);
                args.Add(field.Order);
            }

            if (max > 0)
            {
                args.Add("MAX");
                args.Add(max);
            }
        }

        public AggregationRequest SortBy(int max, params SortedField[] fields)
        {
            this.max = max;
            SortBy(fields);
            return this;
        }

        // public AggregationRequest SortByAsc(string field)
        // {
        //     return SortBy(SortedField.Asc(field));
        // }

        // public AggregationRequest SortByDesc(string field)
        // {
        //     return SortBy(SortedField.Desc(field));
        // }

        public AggregationRequest Apply(string projection, string alias)
        {
            apply.Add(new Tuple<string, string>(projection, alias));
            return this;
        }

        private void Apply()
        {
            foreach (Tuple<string, string> tuple in apply)
            {
                args.Add("APPLY");
                args.Add(tuple.Item1);
                args.Add("AS");
                args.Add(tuple.Item2);
            }
        }

        public AggregationRequest GroupBy(IList<string> fields, IList<Reducer> reducers)
        {
            // string[] fieldsArr = new string[fields.size()];
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
            args.Add("GROUPBY");
            foreach (Group group in groups)
            {
                group.SerializeRedisArgs(args);
            }
        }

        public AggregationRequest Filter(string filter)
        {
            this.filter = filter;
            return this;
        }

        private void Filter()
        {
            args.Add(SearchArgs.FILTER);
            args.Add(filter!);
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
            if(isWithCursor)
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

        public AggregationRequest Verbatim(bool verbatim = true)
        {
            this.verbatim = true;
            return this;
        }

        private void Verbatim()
        {
            args.Add("VERBATIM");
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

        public AggregationRequest Params(Dictionary<string, object> nameValue)
        {
            if (this.nameValue.Count >= 1)
            {
                foreach (var entry in nameValue)
                {
                    this.nameValue.Add(entry.Key, entry.Value);
                }
                return this;
            }

            this.nameValue = nameValue;
            return this;
        }

        private void Params()
        {
            if (nameValue.Count >= 1)
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
            args.Add("DIALECT");
            args.Add(dialect);
        }

        public List<object> GetArgs()
        {
            return args;
        }

        // TODO: SerializeRedisArgs
        // public void SerializeRedisArgs(List<object> redisArgs)
        // {
        //     foreach (var s in GetArgs())
        //     {
        //         redisArgs.Add(s);
        //     }
        // }

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
