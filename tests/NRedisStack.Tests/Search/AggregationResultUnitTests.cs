using NRedisStack.Search.Aggregation;
using StackExchange.Redis;
using Xunit;

namespace NRedisStack.Tests.Search;

public class AggregationResultUnitTests
{
    [Fact]
    public void CanIterateRow()
    {
        Row row = new(new()
        {
            // the fields must all be RedisValue internally, else ignored
            { "name", (RedisValue)"John" },
            { "age", (RedisValue)30 },
            { "ignored", new RedisValue[] { "a", "b", "c" } },
            { "city", (RedisValue)"New York" },
        });

        // test via Row
        Assert.Equal(3, row.FieldCount());
        Assert.Equal("John", row.GetString("name"));
        Assert.Equal(30, row.GetLong("age"));
        Assert.Equal("New York", row.GetString("city"));

        // test via foreach
        int count = 0;
        foreach (var field in row)
        {
            switch (field.Key)
            {
                case "name":
                    Assert.Equal("John", (string?)field.Value);
                    break;
                case "age":
                    Assert.Equal(30, (int)field.Value);
                    break;
                case "city":
                    Assert.Equal("New York", (string?)field.Value);
                    break;
                default:
                    Assert.Fail($"Unexpected field: {field.Key}");
                    break;
            }
            count++;
        }
        Assert.Equal(3, count);

        // test via Select
        var isolated = row.Select(x => (x.Key, x.Value)).ToArray();
        Assert.Equal(3, isolated.Length);
        Assert.Contains(("name", (RedisValue)"John"), isolated);
        Assert.Contains(("age", (RedisValue)30), isolated);
        Assert.Contains(("city", (RedisValue)"New York"), isolated);
    }
}