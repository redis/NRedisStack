using Xunit;

namespace NRedisStack.Tests;
public enum Comparison
{
    LessThan,
    GreaterThanOrEqual,
}

public enum Is
{
    Standalone,
    OSSCluster
}

[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public class SkipIfRedisAttribute : FactAttribute
{
    private readonly string _targetVersion;
    private readonly Comparison _comparison;
    private readonly Is? _is = null;
    private readonly string DefaultRedisConnectionString = Environment.GetEnvironmentVariable("REDIS") ?? "localhost:6379";

    public SkipIfRedisAttribute(
            Is _is,
            Comparison comparison = Comparison.LessThan,
            string targetVersion = "0.0.0")
    {
        this._is = _is;
        _comparison = comparison;
        _targetVersion = targetVersion;
    }

    public SkipIfRedisAttribute(string targetVersion) // defaults to LessThan
    {
        _comparison = Comparison.LessThan;
        _targetVersion = targetVersion;
    }

    public SkipIfRedisAttribute(Comparison comparison, string targetVersion)
    {
        _comparison = comparison;
        _targetVersion = targetVersion;
    }

    public override string? Skip
    {
        get
        {
            string skipReason = "";
            bool skipped = false;
            using (RedisFixture redisFixture = new RedisFixture())
            {

                // Cluster check
                if (_is != null)
                {
                    switch (_is)
                    {
                        case Is.OSSCluster:
                            if (redisFixture.isCluster)
                            {
                                skipReason = skipReason + " Redis server is a cluster.";
                                skipped = true;
                            }
                            break;

                        case Is.Standalone:
                            if (!redisFixture.isCluster)
                            {
                                skipReason = skipReason + " Redis server is not a cluster.";
                                skipped = true;
                            }
                            break;
                    }
                }
                // Version check (if Is.Standalone/Is.Cluster is set then )

                var serverVersion = redisFixture.Redis.GetServer(redisFixture.Redis.GetEndPoints()[0]).Version;
                var targetVersion = new Version(_targetVersion);
                int comparisonResult = serverVersion.CompareTo(targetVersion);

                switch (_comparison)
                {
                    case Comparison.LessThan:
                        if (comparisonResult < 0)
                        {
                            skipReason = skipReason + $" Redis server version ({serverVersion}) is less than {_targetVersion}.";
                            skipped = true;
                        }
                        break;
                    case Comparison.GreaterThanOrEqual:
                        if (comparisonResult >= 0)
                        {
                            skipReason = skipReason + $" Redis server version ({serverVersion}) is greater than or equal to {_targetVersion}.";
                            skipped = true;
                        }
                        break;
                }
            }

            if (skipped)
                return "Test skipped, because:" + skipReason;
            return null;
        }
    }
}