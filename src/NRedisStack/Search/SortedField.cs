namespace NRedisStack.Search.Aggregation;

public class SortedField(string fieldName, SortedField.SortOrder order = SortedField.SortOrder.ASC)
{
    public enum SortOrder
    {
        ASC, DESC
    }

    public string FieldName { get; } = fieldName;
    public SortOrder Order { get; } = order;

    public static SortedField Asc(string field) => new(field, SortOrder.ASC);

    public static SortedField Desc(string field) => new(field, SortOrder.DESC);
}