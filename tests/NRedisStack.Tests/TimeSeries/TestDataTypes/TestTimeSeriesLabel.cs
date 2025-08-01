using NRedisStack.DataTypes;
using Xunit;

namespace NRedisTimeSeries.Test.TestDataTypes;

public class TestLabel
{
    [Fact]
    public void TestLabelConstructor()
    {
        TimeSeriesLabel label = new("a", "b");
        Assert.Equal("a", label.Key);
        Assert.Equal("b", label.Value);
    }


    [Fact]
    public void TestLbaelEquals()
    {
        TimeSeriesLabel label_ab = new("a", "b");
        TimeSeriesLabel label1 = new("a", "b");
        TimeSeriesLabel label2 = new("a", "c");
        TimeSeriesLabel label3 = new("c", "b");

        Assert.Equal(label_ab, label1);
        Assert.NotEqual(label_ab, label2);
        Assert.NotEqual(label_ab, label3);
    }

    [Fact]
    public void TestLabelHashCode()
    {
        TimeSeriesLabel label_ab = new("a", "b");
        TimeSeriesLabel label1 = new("a", "b");
        TimeSeriesLabel label2 = new("a", "c");
        TimeSeriesLabel label3 = new("c", "b");

        Assert.Equal(label_ab.GetHashCode(), label1.GetHashCode());
        Assert.NotEqual(label_ab.GetHashCode(), label2.GetHashCode());
        Assert.NotEqual(label_ab.GetHashCode(), label3.GetHashCode());
    }

    [Fact]
    public void TestLabelToString()
    {
        TimeSeriesLabel label = new("a", "b");
        Assert.Equal("Key: a, Val:b", (string)label);
    }
}