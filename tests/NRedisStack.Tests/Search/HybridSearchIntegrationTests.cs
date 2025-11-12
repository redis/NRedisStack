using System.Runtime.CompilerServices;
using NRedisStack.RedisStackCommands;
using NRedisStack.Search;
using Xunit;
using Xunit.Abstractions;

namespace NRedisStack.Tests.Search;

public class HybridSearchIntegrationTests(EndpointsFixture endpointsFixture, ITestOutputHelper log)
    : AbstractNRedisStackTest(endpointsFixture, log), IDisposable
{
    private readonly struct Api(SearchCommands ft, string index)
    {
        public string Index { get; } = index;
        public SearchCommands FT { get; } = ft;
    }

    private Api CreateIndex(string endpointId, [CallerMemberName] string caller = "", bool populate = true)
    {
        var index = $"ix_{caller}";
        var db = GetCleanDatabase(endpointId);
        // ReSharper disable once RedundantArgumentDefaultValue
        var ft = db.FT(2);
        var vectorAttrs = new Dictionary<string, object>()
        {
            ["TYPE"] = "FLOAT32",
            ["DIM"] = "2",
            ["DISTANCE_METRIC"] = "L2",
        };
        Schema sc = new Schema()
            // ReSharper disable once RedundantArgumentDefaultValue
            .AddTextField("text1", 1.0, missingIndex: true)
            .AddTagField("tag1", missingIndex: true)
            .AddNumericField("numeric1", missingIndex: true)
            .AddGeoField("geo1", missingIndex: true)
            .AddGeoShapeField("geoshape1", Schema.GeoShapeField.CoordinateSystem.FLAT, missingIndex: true)
            .AddVectorField("vector1", Schema.VectorField.VectorAlgo.FLAT, vectorAttrs, missingIndex: true);

        var ftCreateParams = FTCreateParams.CreateParams();
        Assert.True(ft.Create(index, ftCreateParams, sc));

        return new(ft, index);
    }

    [SkipIfRedisTheory(Comparison.LessThan, "8.3.224")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestSetup(string endpointId)
    {
        var api = CreateIndex(endpointId, populate: false);
        Dictionary<string, object> args = new() { ["x"] = "abc" };
        var query = new HybridSearchQuery()
            .Search("*")
            .VectorSearch("@vector1", new byte[] {1,2,3,4})
            .Parameters(args)
            .ReturnFields("@text1");
        var result = api.FT.HybridSearch(api.Index, query);
        Assert.Equal(0, result.TotalResults);
        Assert.NotEqual(TimeSpan.Zero, result.ExecutionTime);
        Assert.Empty(result.Warnings);
        Assert.Empty(result.Results);
    }
}