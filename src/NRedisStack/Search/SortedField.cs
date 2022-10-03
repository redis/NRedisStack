using System.Collections.Generic;

namespace NRedisStack.Search.Aggregation
{
    public class SortedField
    {

        public enum SortOrder
        {
            ASC, DESC
        }

        public string FieldName { get; }
        public SortOrder Order { get; }

        public SortedField(String fieldName, SortOrder order)
        {
            this.FieldName = fieldName;
            this.Order = order;
        }

        public static SortedField Asc(String field)
        {
            return new SortedField(field, SortOrder.ASC);
        }

        public static SortedField Desc(String field)
        {
            return new SortedField(field, SortOrder.DESC);
        }
    }
}