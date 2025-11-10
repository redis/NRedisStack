using Xunit.Abstractions;

namespace NRedisStack.Tests.Search;

public class HybridSearchIntegrationTests(EndpointsFixture endpointsFixture, ITestOutputHelper log)
    : AbstractNRedisStackTest(endpointsFixture, log), IDisposable
{
    
}