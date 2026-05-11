using NRedisStack.DataTypes;
using Xunit;
namespace NRedisStack.Tests.TimeSeries.TestDataTypes;

public class TestTimeStamp
{

    [Fact]
    public void TestTimeStampImplicitCast()
    {
        TimeStamp ts = 1;
        Assert.Equal<long>(1, ts);

        ts = "+";
        Assert.Equal("+", ts);

        ts = "*";
        Assert.Equal("*", ts);

        ts = "-";
        Assert.Equal("-", ts);

        var ex = Assert.Throws<NotSupportedException>(() => ts = "hi");
        Assert.Equal("The string hi cannot be used", ex.Message);

        DateTime now = DateTime.UtcNow;
        ts = now;
        Assert.Equal(new DateTimeOffset(now).ToUnixTimeMilliseconds(), ts.Value);

    }
}