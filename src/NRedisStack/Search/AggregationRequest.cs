using System;
using System.Collections.Generic;
using System.Text;
using NRedisStack.Literals;
// using NRediSearch.Aggregation.Reducers;
using StackExchange.Redis;

namespace NRedisStack.Search.Aggregation
{
    public class AggregationRequest
    {
        private List<object> args = new List<object>(); // Check if Readonly
        private bool isWithCursor = false;

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
            args.Add("LOAD");
            int loadCountIndex = args.Count();
            args.Add(null);
            int loadCount = 0;
            foreach (FieldName fn in fields)
            {
                loadCount += fn.AddCommandArguments(args);
            }
            args.Insert(loadCountIndex, loadCount.ToString());
            return this;
        }

        public AggregationRequest LoadAll()
        {
            args.Add("LOAD");
            args.Add("*");
            return this;
        }

        public AggregationRequest Limit(int offset, int count)
        {
            Limit limit = new Limit(offset, count);
            limit.SerializeRedisArgs(args);
            return this;
        }

        public AggregationRequest Limit(int count)
        {
            return Limit(0, count);
        }

        public AggregationRequest SortBy(params SortedField[] Fields)
        {
            args.Add("SORTBY");
            args.Add(Fields.Length * 2);
            foreach (SortedField field in Fields)
            {
                args.Add(field.FieldName);
                args.Add(field.Order);
            }

            return this;
        }

        public AggregationRequest SortBy(int max, params SortedField[] Fields)
        {
            SortBy(Fields);
            if (max > 0)
            {
                args.Add("MAX");
                args.Add(max);
            }
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
            args.Add("APPLY");
            args.Add(projection);
            args.Add("AS");
            args.Add(alias);
            return this;
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
            args.Add("GROUPBY");
            group.SerializeRedisArgs(args);
            return this;
        }

        public AggregationRequest Filter(string expression)
        {
            args.Add(SearchArgs.FILTER);
            args.Add(expression);
            return this;
        }

        public AggregationRequest Cursor(int count, long maxIdle)
        {
            isWithCursor = true;
            if (count > 0)
            {
                args.Add("WITHCURSOR");
                args.Add("COUNT");
                args.Add(count);
                if (maxIdle < long.MaxValue && maxIdle >= 0)
                {
                    args.Add("MAXIDLE");
                    args.Add(maxIdle);
                }
            }
            return this;
        }

        public AggregationRequest Verbatim()
        {
            args.Add("VERBATIM");
            return this;
        }

        public AggregationRequest Timeout(long timeout)
        {
            if (timeout >= 0)
            {
                args.Add("TIMEOUT");
                args.Add(timeout);
            }
            return this;
        }

        public AggregationRequest Params(Dictionary<string, object> nameValue)
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

            return this;
        }

        public AggregationRequest Dialect(int dialect)
        {
            args.Add("DIALECT");
            args.Add(dialect);
            return this;
        }

        public List<object> GetArgs()
        {
            return args;
        }

        public void SerializeRedisArgs(List<object> redisArgs)
        {
            foreach (var s in GetArgs())
            {
                redisArgs.Add(s);
            }
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
