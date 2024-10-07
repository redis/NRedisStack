using Xunit;

namespace NRedisStack.Tests;
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public class TargetEnvironmentAttribute : SkipIfRedisAttribute
{
    private string targetEnv;
    public TargetEnvironmentAttribute(string targetEnv) : base(Comparison.LessThan, "0.0.0")
    {
        this.targetEnv = targetEnv;
    }

    public TargetEnvironmentAttribute(string targetEnv, Is environment, Comparison comparison = Comparison.LessThan,
            string targetVersion = "0.0.0") : base(environment, comparison, targetVersion)
    {
        this.targetEnv = targetEnv;
    }

    public TargetEnvironmentAttribute(string targetEnv, Is environment1, Is environment2, Comparison comparison = Comparison.LessThan,
        string targetVersion = "0.0.0") : base(environment1, environment2, comparison, targetVersion)
    {
        this.targetEnv = targetEnv;
    }

    public override string? Skip
    {
        get
        {
            if (!new RedisFixture().IsTargetConnectionExist(targetEnv))
            {
                return "Test skipped, because: target environment not found.";
            }
            return base.Skip;
        }
    }
}