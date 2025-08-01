using NRedisStack.DataTypes;
using Xunit;

namespace NRedisTimeSeries.Test;

public class TestTimeSeriesTuple
{
    [Fact]
    public void TestTupleConstructor()
    {
        TimeSeriesTuple tuple = new(1, 1.1);
        Assert.Equal(1, tuple.Time);
        Assert.Equal(1.1, tuple.Val);
    }

    [Fact]
    public void TestTupleEqual()
    {
        TimeSeriesTuple tuple1 = new(1, 1.1);
        TimeSeriesTuple tuple1_1 = new(1, 1.1);
        TimeSeriesTuple tuple1_2 = new(2, 2.2);
        Assert.Equal(tuple1, tuple1_1);
        Assert.NotEqual(tuple1, tuple1_2);
    }

    [Fact]
    public void TestTupleHashCode()
    {
        TimeSeriesTuple tuple1 = new(1, 1.1);
        TimeSeriesTuple tuple1_1 = new(1, 1.1);
        TimeSeriesTuple tuple1_2 = new(2, 2.2);
        Assert.Equal(tuple1.GetHashCode(), tuple1_1.GetHashCode());
        Assert.NotEqual(tuple1.GetHashCode(), tuple1_2.GetHashCode());
    }

    [Fact]
    public void TestTupleToString()
    {
        TimeSeriesTuple tuple = new(1, 1.1);
        Assert.Equal("Time: 1, Val:1.1", (string)tuple);
    }
}