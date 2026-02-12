using System.Runtime.CompilerServices;
using Xunit;
using Xunit.Sdk;
using Xunit.v3;

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

/// <summary>
/// <para>Override for <see cref="Xunit.TheoryAttribute"/> that truncates our DisplayName down.</para>
/// <para>
/// Marks a test method as being a data theory. Data theories are tests which are
/// fed various bits of data from a data source, mapping to parameters on the test
/// method. If the data source contains multiple rows, then the test method is executed
/// multiple times (once with each data row). Data is provided by attributes which
/// derive from Xunit.Sdk.DataAttribute (notably, Xunit.InlineDataAttribute and Xunit.MemberDataAttribute).
/// </para>
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
[XunitTestCaseDiscoverer(typeof(TheoryDiscoverer))]
public class TheoryAttribute(
    [CallerFilePath] string? sourceFilePath = null,
    [CallerLineNumber] int sourceLineNumber = -1) : Xunit.TheoryAttribute(sourceFilePath, sourceLineNumber)
{
    public class TheoryDiscoverer : Xunit.v3.TheoryDiscoverer
    {
        protected override ValueTask<IReadOnlyCollection<IXunitTestCase>> CreateTestCasesForDataRow(ITestFrameworkDiscoveryOptions discoveryOptions, IXunitTestMethod testMethod, ITheoryAttribute theoryAttribute, ITheoryDataRow dataRow, object?[] testMethodArguments)
            => base.CreateTestCasesForDataRow(discoveryOptions, testMethod, theoryAttribute, dataRow, testMethodArguments).ExpandAsync();

        protected override ValueTask<IReadOnlyCollection<IXunitTestCase>> CreateTestCasesForTheory(ITestFrameworkDiscoveryOptions discoveryOptions, IXunitTestMethod testMethod, ITheoryAttribute theoryAttribute)
            => base.CreateTestCasesForTheory(discoveryOptions, testMethod, theoryAttribute).ExpandAsync();
    }
}

[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
[XunitTestCaseDiscoverer(typeof(TheoryDiscoverer))]
public class SkipIfRedisTheoryAttribute : TheoryAttribute
{
    public SkipIfRedisTheoryAttribute(
        Is environment,
        Comparison comparison = Comparison.LessThan,
        string targetVersion = "0.0.0",
        [CallerFilePath] string? sourceFilePath = null,
        [CallerLineNumber] int sourceLineNumber = -1) : base(sourceFilePath, sourceLineNumber)
    {
        SkipIfRedisCore core = new(environment, comparison, targetVersion);
        Skip = core.Skip;
    }

    public SkipIfRedisTheoryAttribute(
        string targetVersion,
        [CallerFilePath] string? sourceFilePath = null,
        [CallerLineNumber] int sourceLineNumber = -1) : base(sourceFilePath, sourceLineNumber) // defaults to LessThan
    {
        SkipIfRedisCore core = new(targetVersion);
        Skip = core.Skip;
    }

    public SkipIfRedisTheoryAttribute(
        Comparison comparison, string targetVersion,
        [CallerFilePath] string? sourceFilePath = null,
        [CallerLineNumber] int sourceLineNumber = -1) : base(sourceFilePath, sourceLineNumber)
    {
        SkipIfRedisCore core = new(comparison, targetVersion);
        Skip = core.Skip;
    }
}

/// <summary>
/// <para>Override for <see cref="Xunit.FactAttribute"/> that truncates our DisplayName down.</para>
/// <para>
/// Attribute that is applied to a method to indicate that it is a fact that should
/// be run by the test runner. It can also be extended to support a customized definition
/// of a test method.
/// </para>
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
[XunitTestCaseDiscoverer(typeof(FactDiscoverer))]
public class FactAttribute([CallerFilePath] string? sourceFilePath = null, [CallerLineNumber] int sourceLineNumber = -1)
    : Xunit.FactAttribute(sourceFilePath, sourceLineNumber)
{
    public class FactDiscoverer : Xunit.v3.FactDiscoverer
    {
        public override ValueTask<IReadOnlyCollection<IXunitTestCase>> Discover(ITestFrameworkDiscoveryOptions discoveryOptions, IXunitTestMethod testMethod, IFactAttribute factAttribute)
            => base.Discover(discoveryOptions, testMethod, factAttribute).ExpandAsync();
    }
}

[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
[XunitTestCaseDiscoverer(typeof(FactDiscoverer))]
public class SkipIfRedisFactAttribute : FactAttribute
{
    public SkipIfRedisFactAttribute(
        Is environment,
        Comparison comparison = Comparison.LessThan,
        string targetVersion = "0.0.0",
        [CallerFilePath] string? sourceFilePath = null,
        [CallerLineNumber] int sourceLineNumber = -1) : base(sourceFilePath, sourceLineNumber)
    {
        SkipIfRedisCore core = new(environment, comparison, targetVersion);
        Skip = core.Skip;
    }

    public SkipIfRedisFactAttribute( // defaults to LessThan
        string targetVersion,
        [CallerFilePath] string? sourceFilePath = null,
        [CallerLineNumber] int sourceLineNumber = -1) : base(sourceFilePath, sourceLineNumber)
    {
        SkipIfRedisCore core = new(targetVersion);
        Skip = core.Skip;
    }

    public SkipIfRedisFactAttribute(
        Comparison comparison,
        string targetVersion,
        [CallerFilePath] string? sourceFilePath = null,
        [CallerLineNumber] int sourceLineNumber = -1) : base(sourceFilePath, sourceLineNumber)
    {
        SkipIfRedisCore core = new(comparison, targetVersion);
        Skip = core.Skip;
    }
}

internal static class XUnitExtensions
{
    public static RunProtocol GetRunProtocol(this ITestContext context) =>
        context.Test?.TestCase is IRunProtocolTestCase protocolTestCase
            ? protocolTestCase.Protocol : RunProtocol.Resp2;

    public static bool IsResp3(this RunProtocol protocol) => protocol is RunProtocol.Resp3 or RunProtocol.Resp3HighIntegrity;
    public static bool IsHighIntegrity(this RunProtocol protocol) => protocol is RunProtocol.Resp2HighIntegrity or RunProtocol.Resp3HighIntegrity;

    public static async ValueTask<IReadOnlyCollection<IXunitTestCase>> ExpandAsync(this ValueTask<IReadOnlyCollection<IXunitTestCase>> discovery)
    {
        static IXunitTestCase CreateTestCase(XunitTestCase tc, RunProtocol protocol) => tc switch
        {
            XunitDelayEnumeratedTheoryTestCase delayed => new ProtocolDelayEnumeratedTestCase(delayed, protocol),
            _ => new ProtocolTestCase(tc, protocol),
        };
        var testCases = await discovery;
        List<IXunitTestCase> result = [];
        foreach (var testCase in testCases.OfType<XunitTestCase>())
        {
            var testMethod = testCase.TestMethod;
            if ((testMethod.Method.GetCustomAttributes(typeof(RunPerProtocolAttribute), true).FirstOrDefault()
                 ?? testMethod.TestClass.Class.GetCustomAttributes(typeof(RunPerProtocolAttribute), true).FirstOrDefault()) is RunPerProtocolAttribute rpp)
            {
                var protocols = rpp.Protocols;
                if ((protocols & RunProtocol.Resp2) != 0) result.Add(CreateTestCase(testCase, RunProtocol.Resp2));
                if ((protocols & RunProtocol.Resp3) != 0) result.Add(CreateTestCase(testCase, RunProtocol.Resp3));
                if ((protocols & RunProtocol.Resp2HighIntegrity) != 0) result.Add(CreateTestCase(testCase, RunProtocol.Resp2HighIntegrity));
                if ((protocols & RunProtocol.Resp3HighIntegrity) != 0) result.Add(CreateTestCase(testCase, RunProtocol.Resp3HighIntegrity));
            }
            else
            {
                // Default to RESP2 everywhere else
                result.Add(CreateTestCase(testCase, RunProtocol.Resp2));
            }
        }
        return result;
    }
}