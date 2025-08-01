using Xunit;
using Xunit.Sdk;

namespace NRedisStack.Tests;

public enum Comparison
{
    LessThan,
    GreaterThanOrEqual,
}

public enum Is
{
    Enterprise
}

internal readonly struct SkipIfRedisCore
{
    private readonly string _targetVersion;
    private readonly Comparison _comparison;
    private readonly List<Is> _environments = [];

    public SkipIfRedisCore(
        Is environment,
        Comparison comparison = Comparison.LessThan,
        string targetVersion = "0.0.0")
    {
        _environments.Add(environment);
        _comparison = comparison;
        _targetVersion = targetVersion;
    }

    public SkipIfRedisCore(string targetVersion) // defaults to LessThan
    {
        _comparison = Comparison.LessThan;
        _targetVersion = targetVersion;
    }

    public SkipIfRedisCore(Comparison comparison, string targetVersion)
    {
        _comparison = comparison;
        _targetVersion = targetVersion;
    }

    public string? Skip
    {
        get
        {
            string skipReason = "";
            bool skipped = false;

            foreach (var environment in _environments)
            {
                switch (environment)
                {
                    case Is.Enterprise:
                        if (EndpointsFixture.IsEnterprise)
                        {
                            skipReason = skipReason + " Redis Enterprise environment.";
                            skipped = true;
                        }

                        break;
                }
            }

            var targetVersion = new Version(_targetVersion);
            int comparisonResult = EndpointsFixture.RedisVersion.CompareTo(targetVersion);

            switch (_comparison)
            {
                case Comparison.LessThan:
                    if (comparisonResult < 0)
                    {
                        skipReason = skipReason +
                                     $" Redis server version ({EndpointsFixture.RedisVersion}) is less than {_targetVersion}.";
                        skipped = true;
                    }

                    break;
                case Comparison.GreaterThanOrEqual:
                    if (comparisonResult >= 0)
                    {
                        skipReason = skipReason +
                                     $" Redis server version ({EndpointsFixture.RedisVersion}) is greater than or equal to {_targetVersion}.";
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

[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
[XunitTestCaseDiscoverer("NRedisStack.Tests.SkippableTheoryDiscoverer", "NRedisStack.Tests")]
public class SkipIfRedisTheoryAttribute : SkippableTheoryAttribute
{
    private readonly SkipIfRedisCore core;

    public SkipIfRedisTheoryAttribute(
        Is environment,
        Comparison comparison = Comparison.LessThan,
        string targetVersion = "0.0.0")
        => core = new(environment, comparison, targetVersion);

    public SkipIfRedisTheoryAttribute(string targetVersion) // defaults to LessThan
        => core = new(targetVersion);

    public SkipIfRedisTheoryAttribute(Comparison comparison, string targetVersion)
        => core = new(comparison, targetVersion);

    public override string? Skip => core.Skip;
}

[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
[XunitTestCaseDiscoverer("NRedisStack.Tests.SkippableTheoryDiscoverer", "NRedisStack.Tests")]
public class SkipIfRedisFactAttribute : SkippableFactAttribute
{
    private readonly SkipIfRedisCore core;

    public SkipIfRedisFactAttribute(
        Is environment,
        Comparison comparison = Comparison.LessThan,
        string targetVersion = "0.0.0")
        => core = new(environment, comparison, targetVersion);

    public SkipIfRedisFactAttribute(string targetVersion) // defaults to LessThan
        => core = new(targetVersion);

    public SkipIfRedisFactAttribute(Comparison comparison, string targetVersion)
        => core = new(comparison, targetVersion);

    public override string? Skip => core.Skip;
}