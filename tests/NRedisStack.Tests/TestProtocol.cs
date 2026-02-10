using Xunit.Internal;
using Xunit.Sdk;
using Xunit.v3;

namespace NRedisStack.Tests;

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Method, AllowMultiple = false)]
public class RunPerProtocolAttribute(RunProtocol protocols = RunProtocol.Resp2 | RunProtocol.Resp3) : Attribute
{
    public RunProtocol Protocols => protocols;
}

[Flags]
public enum RunProtocol
{
    None = 0,
    Resp2 = 1 << 0,
    Resp3 = 1 << 1,
    // High integrity is not a separate protocol, but a flag that can be permuted with Resp2 and Resp3
    // This is niche, and is usually omitted.
    Resp2HighIntegrity = 1 << 2,
    Resp3HighIntegrity = 1 << 3,
}
public interface IRunProtocolTestCase
{
    RunProtocol Protocol { get; }
}

public class ProtocolTestCase : XunitTestCase, IRunProtocolTestCase
{
    public RunProtocol Protocol { get; private set; }

    [Obsolete("Called by the de-serializer; should only be called by deriving classes for de-serialization purposes")]
    public ProtocolTestCase() { }

    public ProtocolTestCase(XunitTestCase testCase, RunProtocol protocol) : base(
        testMethod: testCase.TestMethod,
        testCaseDisplayName: $"{testCase.TestCaseDisplayName.Replace("NRedisStack.Tests.", "")} ({protocol.ToString()})",
        uniqueID: testCase.UniqueID + protocol.ToString(),
        @explicit: testCase.Explicit,
        skipExceptions: testCase.SkipExceptions,
        skipReason: testCase.SkipReason,
        skipType: testCase.SkipType,
        skipUnless: testCase.SkipUnless,
        skipWhen: testCase.SkipWhen,
        traits: testCase.TestMethod.Traits.ToReadWrite(StringComparer.OrdinalIgnoreCase),
        testMethodArguments: testCase.TestMethodArguments,
        sourceFilePath: testCase.SourceFilePath,
        sourceLineNumber: testCase.SourceLineNumber,
        timeout: testCase.Timeout)
        => Protocol = protocol;

    protected override void Serialize(IXunitSerializationInfo data)
    {
        base.Serialize(data);
        data.AddValue("resp", (int)Protocol);
    }

    protected override void Deserialize(IXunitSerializationInfo data)
    {
        base.Deserialize(data);
        Protocol = (RunProtocol)data.GetValue<int>("resp");
    }
}

public class ProtocolDelayEnumeratedTestCase : XunitDelayEnumeratedTheoryTestCase, IRunProtocolTestCase
{
    public RunProtocol Protocol { get; private set; }

    [Obsolete("Called by the de-serializer; should only be called by deriving classes for de-serialization purposes")]
    public ProtocolDelayEnumeratedTestCase() { }

    public ProtocolDelayEnumeratedTestCase(XunitDelayEnumeratedTheoryTestCase testCase, RunProtocol protocol) : base(
        testMethod: testCase.TestMethod,
        testCaseDisplayName: $"{testCase.TestCaseDisplayName.Replace("NRedisStack.Tests.", "")} ({protocol.ToString()})",
        uniqueID: testCase.UniqueID + protocol.ToString(),
        @explicit: testCase.Explicit,
        skipTestWithoutData: testCase.SkipTestWithoutData,
        skipExceptions: testCase.SkipExceptions,
        skipReason: testCase.SkipReason,
        skipType: testCase.SkipType,
        skipUnless: testCase.SkipUnless,
        skipWhen: testCase.SkipWhen,
        traits: testCase.TestMethod.Traits.ToReadWrite(StringComparer.OrdinalIgnoreCase),
        sourceFilePath: testCase.SourceFilePath,
        sourceLineNumber: testCase.SourceLineNumber,
        timeout: testCase.Timeout)
        => Protocol = protocol;

    protected override void Serialize(IXunitSerializationInfo data)
    {
        base.Serialize(data);
        data.AddValue("resp", (int)Protocol);
    }

    protected override void Deserialize(IXunitSerializationInfo data)
    {
        base.Deserialize(data);
        Protocol = (RunProtocol)data.GetValue<int>("resp");
    }
}