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
    OSSCluster,
    Enterprise
}

[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public class SkipIfRedisAttribute : FactAttribute
{
    private class RedisFixtureData
    {
        public RedisFixtureData()
        {
            using (var redisFixture = new RedisFixture())
            {
                isEnterprise = redisFixture.isEnterprise;
                isOSSCluster = redisFixture.isOSSCluster;
                serverVersion = redisFixture.Redis.GetServer(redisFixture.Redis.GetEndPoints()[0]).Version;
            }
        }

        public readonly bool isEnterprise;
        public readonly bool isOSSCluster;
        public readonly Version serverVersion;
    }
    private readonly string _targetVersion;
    private readonly Comparison _comparison;
    private readonly List<Is> _environments = new List<Is>();

    private static RedisFixtureData _redisFixtureData;
    private static Version serverVersion = null;

    public SkipIfRedisAttribute(
            Is environment,
            Comparison comparison = Comparison.LessThan,
            string targetVersion = "0.0.0")
    {
        _environments.Add(environment);
        _comparison = comparison;
        _targetVersion = targetVersion;
    }

    // skip more than one environment:
    public SkipIfRedisAttribute(
            Is environment1,
            Is environment2,
            Comparison comparison = Comparison.LessThan,
            string targetVersion = "0.0.0")
    {
        _environments.Add(environment1);
        _environments.Add(environment2);
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
            if (_redisFixtureData == null)
            {
                _redisFixtureData = new RedisFixtureData();
            }

            foreach (var environment in _environments)
            {
                switch (environment)
                {
                    case Is.OSSCluster:
                        if (_redisFixtureData.isOSSCluster)
                        {
                            skipReason = skipReason + " Redis server is OSS cluster.";
                            skipped = true;
                        }
                        break;

                    case Is.Standalone:
                        if (!_redisFixtureData.isOSSCluster)
                        {
                            skipReason = skipReason + " Redis server is not OSS cluster.";
                            skipped = true;
                        }
                        break;

                    case Is.Enterprise:
                        if (_redisFixtureData.isEnterprise)
                        {
                            skipReason = skipReason + " Redis Enterprise environment.";
                            skipped = true;
                        }
                        break;
                }
            }
            // Version check (if Is.Standalone/Is.OSSCluster is set then )

            serverVersion = _redisFixtureData.serverVersion;
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

            if (skipped)
                return "Test skipped, because:" + skipReason;
            return null;
        }
    }
}