using System.Buffers;
using System.Data;
using System.Numerics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using NRedisStack.RedisStackCommands;
using NRedisStack.Search;
using NRedisStack.Search.Aggregation;
using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace NRedisStack.Tests.Search;

public class HybridSearchIntegrationTests(EndpointsFixture endpointsFixture, ITestOutputHelper log)
    : AbstractNRedisStackTest(endpointsFixture, log), IDisposable
{
    private readonly struct Api(SearchCommands ft, string index, IDatabase db)
    {
        public string Index { get; } = index;
        public SearchCommands FT { get; } = ft;
        public IDatabase DB { get; } = db;
    }

    private const int V1DIM = 5;

    private async Task<Api> CreateIndexAsync(string endpointId, [CallerMemberName] string caller = "",
        bool populate = true)
    {
        var index = $"ix_{caller}";
        var db = GetCleanDatabase(endpointId);
        // ReSharper disable once RedundantArgumentDefaultValue
        var ft = db.FT(2);

        var vectorAttrs = new Dictionary<string, object>()
        {
            ["TYPE"] = "FLOAT16",
            ["DIM"] = V1DIM,
            ["DISTANCE_METRIC"] = "L2",
        };
        Schema sc = new Schema()
            // ReSharper disable once RedundantArgumentDefaultValue
            .AddTextField("text1", 1.0, missingIndex: true)
            .AddTagField("tag1", missingIndex: true)
            .AddNumericField("numeric1", missingIndex: true)
            .AddVectorField("vector1", Schema.VectorField.VectorAlgo.FLAT, vectorAttrs, missingIndex: true);

        var ftCreateParams = FTCreateParams.CreateParams();
        Assert.True(await ft.CreateAsync(index, ftCreateParams, sc));

        if (populate)
        {
#if NET
            Task last = Task.CompletedTask;
            var rand = new Random(12345);
            string[] tags = ["foo", "bar", "blap"];
            for (int i = 0; i < 16; i++)
            {
                byte[] vec = new byte[V1DIM * sizeof(ushort)];
                var halves = MemoryMarshal.Cast<byte, Half>(vec);
                for (int j = 1; j < V1DIM; j++)
                {
                    halves[j] = (Half)rand.NextDouble();
                }
                HashEntry[] entry = [
                    new("text1", $"Search entry {i}"),
                    new("tag1", tags[rand.Next(tags.Length)]),
                    new("numeric1", rand.Next(0, 32)),
                    new("vector1", vec)];
                last = db.HashSetAsync($"{index}_entry{i}", entry);
            }
            await last;
#else
            throw new PlatformNotSupportedException("FP16");
#endif
        }

        return new(ft, index, db);
    }

    [SkipIfRedisTheory(Comparison.LessThan, "8.3.224")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestSetup(string endpointId)
    {
        var api = await CreateIndexAsync(endpointId, populate: false);
        Dictionary<string, object> args = new() { ["x"] = "abc" };
        var query = new HybridSearchQuery()
            .Search("*")
            .VectorSearch("@vector1", new float[] { 1, 2, 3, 4 })
            .ReturnFields("@text1");
        var result = api.FT.HybridSearch(api.Index, query, args);
        Assert.Equal(0, result.TotalResults);
        Assert.NotEqual(TimeSpan.Zero, result.ExecutionTime);
        Assert.Empty(result.Warnings);
        Assert.Empty(result.Results);
    }

    [SkipIfRedisTheory(Comparison.LessThan, "8.3.224")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestSearch(string endpointId)
    {
        var api = await CreateIndexAsync(endpointId, populate: true);

        var hash = (await api.DB.HashGetAllAsync($"{api.Index}_entry2")).ToDictionary(k => k.Name, v => v.Value);
        var vec = (byte[])hash["vector1"]!;
        var text = (string)hash["text1"]!;
        var query = new HybridSearchQuery()
            .Search(text)
            .VectorSearch("@vector1", VectorData.Raw(vec))
            .ReturnFields("@text1", HybridSearchQuery.Fields.Key, HybridSearchQuery.Fields.Score);

        WriteArgs(api.Index, query);

        var result = api.FT.HybridSearch(api.Index, query);
        Assert.Equal(10, result.TotalResults);
        Assert.NotEqual(TimeSpan.Zero, result.ExecutionTime);
        Assert.Empty(result.Warnings);
        Assert.Same(result.Results, result.Results); // check this is not allocating each time
        Assert.Equal(10, result.Results.Length);
        foreach (var row in result.Results)
        {
            Assert.NotNull(row.Id);
            Assert.NotEqual("", row.Id);
            Assert.False(double.IsNaN(row.Score));
            var text1 = (string)row["text1"]!;
            Assert.False(string.IsNullOrWhiteSpace(text1));
            Log($"{row.Id}, {row.Score}, {text1}");
        }
    }

    public enum Scenario
    {
        Simple,
        NoSort,
        [Obsolete] ExplainScore,
        Apply,
        LinearNoScore,
        [Obsolete]LinearWithScore,
        RrfNoScore,
        [Obsolete]RrfWithScore,
        [Obsolete]FilterByTag,
        FilterByNumber,
        LimitFirstPage,
        LimitSecondPage,
        LimitEmptyPage,
        SortBySingle,
        SortByMultiple,
        Timeout,
        ReduceSingleSimpleWithAlias,
        ReduceSingleSimpleWithoutAlias,
        ReduceSingleComplexWithAlias,
        ReduceMulti,
        GroupByNoReduce,
        SearchWithAlias,
        SearchWithSimpleScorer,
        [Obsolete]SearchWithComplexScorer,
        [Obsolete]VectorWithAlias,
        VectorWithRange,
        [Obsolete]VectorWithRangeAndDistanceAlias,
        [Obsolete]VectorWithRangeAndEpsilon,
        VectorWithTagFilter,
        VectorWithNumericFilter,
        VectorWithNearest,
        VectorWithNearestCount,
        [Obsolete]VectorWithNearestDistAlias,
        [Obsolete]VectorWithNearestMaxCandidates,
    }

    private static class EnumCache<T>
    {
        public static IEnumerable<T> Values { get; } = (
            from field in typeof(T).GetFields(BindingFlags.Public | BindingFlags.Static)
            where !Attribute.IsDefined(field, typeof(ObsoleteAttribute))
            let val = field.GetRawConstantValue()
            where val is not null
            select (T)val).ToArray();
    }

    private static IEnumerable<object[]> CrossJoin<T>(Func<IEnumerable<object[]>> environments)
        where T : unmanaged, Enum
    {
        foreach (var arr in environments())
        {
            foreach (T scenario in EnumCache<T>.Values)
            {
                yield return [..arr, scenario];
            }
        }
    }

    public static IEnumerable<object[]> AllEnvironments_Scenarios() =>
        CrossJoin<Scenario>(EndpointsFixture.Env.AllEnvironments);

    [SkipIfRedisTheory(Comparison.LessThan, "8.3.224")]
    [MemberData(nameof(AllEnvironments_Scenarios))]
    public async Task TestSearchScenarios(string endpointId, Scenario scenario)
    {
        var api = await CreateIndexAsync(endpointId, populate: true);

        var hash = (await api.DB.HashGetAllAsync($"{api.Index}_entry2")).ToDictionary(k => k.Name, v => v.Value);
        var vec = (byte[])hash["vector1"]!;
        var text = (string)hash["text1"]!;
        string[] fields = ["@text1", HybridSearchQuery.Fields.Key, HybridSearchQuery.Fields.Score];
        var query = new HybridSearchQuery()
            .Search(text)
            .VectorSearch("@vector1", VectorData.Raw(vec))
            .ReturnFields(fields);

#pragma warning disable CS0612
        query = scenario switch
        {
            Scenario.Simple => query,
            Scenario.SearchWithAlias => query.Search(new(text, scoreAlias: "score_alias")),
            Scenario.SearchWithSimpleScorer => query.Search(new(text, scorer: Scorer.TfIdf)),
            Scenario.SearchWithComplexScorer => query.Search(new(text, scorer: Scorer.BM25StdTanh(7))),
            Scenario.VectorWithAlias => query.VectorSearch(new("@vector1", VectorData.Raw(vec), scoreAlias: "score_alias" )),
            Scenario.VectorWithRange => query.VectorSearch(new("@vector1", VectorData.Raw(vec), method: VectorSearchMethod.Range(42))),
            Scenario.VectorWithRangeAndDistanceAlias => query.VectorSearch(new("@vector1", VectorData.Raw(vec), method: VectorSearchMethod.Range(42, distanceAlias: "dist_alias"))),
            Scenario.VectorWithRangeAndEpsilon => query.VectorSearch(new("@vector1", VectorData.Raw(vec), method: VectorSearchMethod.Range(42, epsilon: 0.1))),
            Scenario.VectorWithNearest => query.VectorSearch(new("@vector1", VectorData.Raw(vec), method: VectorSearchMethod.NearestNeighbour())),
            Scenario.VectorWithNearestCount => query.VectorSearch(new("@vector1", VectorData.Raw(vec), method: VectorSearchMethod.NearestNeighbour(20))),
            Scenario.VectorWithNearestDistAlias => query.VectorSearch(new("@vector1", VectorData.Raw(vec), method: VectorSearchMethod.NearestNeighbour(distanceAlias: "dist_alias"))),
            Scenario.VectorWithNearestMaxCandidates => query.VectorSearch(new("@vector1", VectorData.Raw(vec), method: VectorSearchMethod.NearestNeighbour(maxTopCandidates: 10))),
            Scenario.VectorWithTagFilter => query.VectorSearch(new("@vector1", VectorData.Raw(vec), filter: "@tag1:{foo}")),
            Scenario.VectorWithNumericFilter => query.VectorSearch(new("@vector1", VectorData.Raw(vec), filter: "@numeric1!=0")),
            Scenario.NoSort => query.NoSort(),
            Scenario.ExplainScore => query.ExplainScore(),
            Scenario.Apply => query.ReturnFields([..fields, "@numeric1"])
                .Apply(new("@numeric1 * 2", "x2"), new("@x2 * 3")), // non-aliased, comes back as the expression
            Scenario.LinearNoScore => query.Combine(HybridSearchQuery.Combiner.Linear(0.4, 0.6)),
            Scenario.LinearWithScore => query.Combine(HybridSearchQuery.Combiner.Linear(), "lin_score"),
            Scenario.RrfNoScore => query.Combine(HybridSearchQuery.Combiner.ReciprocalRankFusion(10, 1.2)),
            Scenario.RrfWithScore => query.Combine(HybridSearchQuery.Combiner.ReciprocalRankFusion(), "rrf_score"),
            Scenario.FilterByTag => query.Filter("@tag1:{foo}"),
            Scenario.FilterByNumber => query.ReturnFields([..fields, "@numeric1"]).Filter("@numeric1!=0"),
            Scenario.LimitFirstPage => query.Limit(0, 2),
            Scenario.LimitSecondPage => query.Limit(2, 2),
            Scenario.LimitEmptyPage => query.Limit(0, 0),
            Scenario.SortBySingle => query.SortBy("@numeric1"),
            Scenario.SortByMultiple => query.SortBy("@text1", "@numeric1", HybridSearchQuery.Fields.Score),
            Scenario.Timeout => query.Timeout(TimeSpan.FromSeconds(1)),
            Scenario.GroupByNoReduce => query.GroupBy("@tag1"),
            Scenario.ReduceSingleSimpleWithAlias => query.GroupBy("@tag1").Reduce(Reducers.Avg("@numeric1").As("avg")),
            Scenario.ReduceSingleSimpleWithoutAlias => query.GroupBy("@tag1").Reduce(Reducers.Sum("@numeric1")),
            Scenario.ReduceSingleComplexWithAlias => query.GroupBy("@tag1").Reduce(Reducers.Quantile("@numeric1", 0.5).As("qt")),
            Scenario.ReduceMulti => query.GroupBy("@tag1").Reduce(Reducers.Count().As("count"), Reducers.Min("@numeric1").As("min"), Reducers.Max("@numeric1").As("max")),
            _ => throw new ArgumentOutOfRangeException(scenario.ToString()),
        };
#pragma warning restore CS0612
        WriteArgs(api.Index, query);

        var result = api.FT.HybridSearch(api.Index, query);
        Assert.True(result.TotalResults > 0);
        Assert.NotEqual(TimeSpan.Zero, result.ExecutionTime);
        Assert.Empty(result.Warnings);
        Assert.Same(result.Results, result.Results); // check this is not allocating each time
        Assert.True(scenario == Scenario.LimitEmptyPage | result.Results.Length > 0);
        foreach (var row in result.Results)
        {
            Log($"{row.Id}, {row.Score}");
            if (!(scenario is Scenario.ReduceSingleSimpleWithAlias or Scenario.ReduceSingleComplexWithAlias
                or Scenario.ReduceMulti or Scenario.ReduceSingleSimpleWithoutAlias or Scenario.GroupByNoReduce))
            {
                Assert.NotNull(row.Id);
                Assert.NotEqual("", row.Id);
                Assert.False(double.IsNaN(row.Score));
            }

            foreach (var prop in row._properties)
            {
                Log($"{prop.Key}={prop.Value}");
            }
        }
    }

    private void WriteArgs(string indexName, HybridSearchQuery query,
        IReadOnlyDictionary<string, object>? parameters = null)
    {
        byte[] scratch = [];

        var sb = new StringBuilder(query.Command).Append(' ');
        var args = query.GetArgs(indexName, parameters);
        foreach (var arg in args)
        {
            sb.Append(' ');
            if (arg is string s)
            {
                sb.Append('"').Append(s.Replace("\"", "\\\"")).Append('"');
            }
            else if (arg is RedisValue v)
            {
                var len = v.GetByteCount();
                if (len > scratch.Length)
                {
                    ArrayPool<byte>.Shared.Return(scratch);
                    scratch = ArrayPool<byte>.Shared.Rent(len);
                }

                v.CopyTo(scratch);
                WriteEscaped(scratch.AsSpan(0, len), sb);
            }
            else
            {
                sb.Append(arg);
            }
        }

        Log(sb.ToString());

        ArrayPool<byte>.Shared.Return(scratch);

        static void WriteEscaped(ReadOnlySpan<byte> span, StringBuilder sb)
        {
            // write resp-cli style
            sb.Append("\"");
            foreach (var b in span)
            {
                if (b < ' ' | b >= 127 | b == '"' | b == '\\')
                {
                    switch (b)
                    {
                        case (byte)'\\': sb.Append("\\\\"); break;
                        case (byte)'"': sb.Append("\\\""); break;
                        case (byte)'\n': sb.Append("\\n"); break;
                        case (byte)'\r': sb.Append("\\r"); break;
                        case (byte)'\t': sb.Append("\\t"); break;
                        case (byte)'\b': sb.Append("\\b"); break;
                        case (byte)'\a': sb.Append("\\a"); break;
                        default: sb.Append("\\x").Append(b.ToString("X2")); break;
                    }
                }
                else
                {
                    sb.Append((char)b);
                }
            }

            sb.Append('"');
        }
    }
}