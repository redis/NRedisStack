using Xunit;
using StackExchange.Redis;

public enum Comparison
{
    LessThan,
    GreaterThanOrEqual,
}

[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public class SkipIfRedisVersionAttribute : FactAttribute
{
    private readonly string _targetVersion;
    private readonly Comparison _comparison;
    private readonly string DefaultRedisConnectionString = Environment.GetEnvironmentVariable("REDIS") ?? "localhost:6379";

    public SkipIfRedisVersionAttribute(string targetVersion) // defaults to LessThan
    {
        _comparison = Comparison.LessThan;
        _targetVersion = targetVersion;
    }

    public SkipIfRedisVersionAttribute(Comparison comparison, string targetVersion)
    {
        _comparison = comparison;
        _targetVersion = targetVersion;
    }

    public override string Skip
    {
        get
        {
            using (var connection = ConnectionMultiplexer.Connect(DefaultRedisConnectionString))
            {
                var serverVersion = connection.GetServer(connection.GetEndPoints()[0]).Version;
                var targetVersion = new Version(_targetVersion);
                int comparisonResult = serverVersion.CompareTo(targetVersion);

                switch (_comparison)
                {
                    case Comparison.LessThan:
                        if (comparisonResult < 0)
                            return $"Test skipped because Redis server version ({serverVersion}) is less than {_targetVersion}.";
                        break;
                    case Comparison.GreaterThanOrEqual:
                        if (comparisonResult >= 0)
                            return $"Test skipped because Redis server version ({serverVersion}) is greater than or equal to {_targetVersion}.";
                        break;
                }

                return null;
            }
        }
    }
}