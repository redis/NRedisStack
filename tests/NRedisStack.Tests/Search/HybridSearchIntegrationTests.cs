using System.Buffers;
using System.Data;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using NRedisStack.RedisStackCommands;
using NRedisStack.Search;
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

    private async Task<Api> CreateIndexAsync(string endpointId, [CallerMemberName] string caller = "", bool populate = true)
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
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
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
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
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

    private void WriteArgs(string indexName, HybridSearchQuery query, IReadOnlyDictionary<string, object>? parameters = null)
    {
        byte[] scratch = [];

        var sb = new StringBuilder(query.Command).Append(' ');
        var args = query.GetArgs(indexName, parameters);
        foreach (var arg in args)
        {
            sb.Append(' ');
            if (arg is string s)
            {
                sb.Append('"').Append(s.Replace("\"","\\\"")).Append('"');
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