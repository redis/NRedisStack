using System.Runtime.InteropServices;
using StackExchange.Redis;
using NRedisStack.Search;
using NRedisStack.RedisStackCommands;
using Xunit;
using NetTopologySuite.Geometries;

namespace NRedisStack.Tests.Search;

// [RunPerProtocol]
public class IndexCreationTests(EndpointsFixture endpointsFixture, ITestOutputHelper log)
    : AbstractNRedisStackTest(endpointsFixture, log), IDisposable
{
    private readonly string index = "MISSING_EMPTY_INDEX";

    // ReSharper disable once InconsistentNaming
    private static readonly string INDEXMISSING = "INDEXMISSING";

    // ReSharper disable once InconsistentNaming
    private static readonly string INDEXEMPTY = "INDEXEMPTY";

    // ReSharper disable once InconsistentNaming
    private static readonly string SORTABLE = "SORTABLE";

    [Fact]
    public void TestMissingEmptyFieldCommandArgs()
    {
        Schema sc = new Schema()
            // ReSharper disable once RedundantArgumentDefaultValue
            .AddTextField("text1", 1.0, missingIndex: true, emptyIndex: true)
            .AddTagField("tag1", missingIndex: true, emptyIndex: true)
            .AddNumericField("numeric1", missingIndex: true)
            .AddGeoField("geo1", missingIndex: true)
            .AddGeoShapeField("geoshape1", Schema.GeoShapeField.CoordinateSystem.FLAT, missingIndex: true)
            .AddVectorField("vector1", Schema.VectorField.VectorAlgo.FLAT, missingIndex: true);

        var ftCreateParams = FTCreateParams.CreateParams();

        var cmd = SearchCommandBuilder.Create(index, ftCreateParams, sc);
        var expectedArgs = new object[]
        {
            "MISSING_EMPTY_INDEX", "SCHEMA",
            "text1", "TEXT", INDEXMISSING, INDEXEMPTY,
            "tag1", "TAG", INDEXMISSING, INDEXEMPTY,
            "numeric1", "NUMERIC", INDEXMISSING,
            "geo1", "GEO", INDEXMISSING,
            "geoshape1", "GEOSHAPE", "FLAT", INDEXMISSING,
            "vector1", "VECTOR", "FLAT", 0, INDEXMISSING
        };
        Assert.Equal(expectedArgs, cmd.Args);
    }

    [SkipIfRedisTheory(Comparison.LessThan, "7.3.240")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestMissingFields(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
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

        var hashWithMissingFields = new HashEntry[] { new("field1", "value1"), new("field2", "value2") };
        db.HashSet("hashWithMissingFields", hashWithMissingFields);

        Polygon polygon = new GeometryFactory().CreatePolygon([new(1, 1), new(10, 10), new(100, 100), new(1, 1)]);

        var hashWithAllFields = new HashEntry[]
        {
            new("text1", "value1"), new("tag1", "value2"), new("numeric1", "3.141"), new("geo1", "-0.441,51.458"),
            new("geoshape1", polygon.ToString()), new("vector1", "aaaaaaaa")
        };
        db.HashSet("hashWithAllFields", hashWithAllFields);

        var result = ft.Search(index, new("ismissing(@text1)"));
        Assert.Equal(1, result.TotalResults);
        Assert.Equal("hashWithMissingFields", result.Documents[0].Id);

        result = ft.Search(index, new("ismissing(@tag1)"));
        Assert.Equal(1, result.TotalResults);
        Assert.Equal("hashWithMissingFields", result.Documents[0].Id);

        result = ft.Search(index, new("ismissing(@numeric1)"));
        Assert.Equal(1, result.TotalResults);
        Assert.Equal("hashWithMissingFields", result.Documents[0].Id);

        result = ft.Search(index, new("ismissing(@geo1)"));
        Assert.Equal(1, result.TotalResults);
        Assert.Equal("hashWithMissingFields", result.Documents[0].Id);

        result = ft.Search(index, new("ismissing(@geoshape1)"));
        Assert.Equal(1, result.TotalResults);
        Assert.Equal("hashWithMissingFields", result.Documents[0].Id);

        result = ft.Search(index, new("ismissing(@vector1)"));
        Assert.Equal(1, result.TotalResults);
        Assert.Equal("hashWithMissingFields", result.Documents[0].Id);
    }

    [SkipIfRedisTheory(Comparison.LessThan, "7.3.240")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestEmptyFields(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        // ReSharper disable once RedundantArgumentDefaultValue
        var ft = db.FT(2);
        Schema sc = new Schema()
            // ReSharper disable once RedundantArgumentDefaultValue
            .AddTextField("text1", 1.0, emptyIndex: true)
            .AddTagField("tag1", emptyIndex: true);

        var ftCreateParams = FTCreateParams.CreateParams();
        Assert.True(ft.Create(index, ftCreateParams, sc));

        var hashWithMissingFields = new HashEntry[] { new("text1", ""), new("tag1", "") };
        db.HashSet("hashWithEmptyFields", hashWithMissingFields);

        var hashWithAllFields = new HashEntry[] { new("text1", "value1"), new("tag1", "value2") };
        db.HashSet("hashWithAllFields", hashWithAllFields);

        var result = ft.Search(index, new("@text1:''"));
        Assert.Equal(1, result.TotalResults);
        Assert.Equal("hashWithEmptyFields", result.Documents[0].Id);

        result = ft.Search(index, new("@tag1:{''}"));
        Assert.Equal(1, result.TotalResults);
        Assert.Equal("hashWithEmptyFields", result.Documents[0].Id);
    }

    [SkipIfRedisTheory(Comparison.LessThan, "7.3.240")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestCreateFloat16VectorField(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        // ReSharper disable once RedundantArgumentDefaultValue
        var ft = db.FT(2);
        var schema = new Schema().AddVectorField("v", Schema.VectorField.VectorAlgo.FLAT, new()
        {
            ["TYPE"] = "FLOAT16",
            ["DIM"] = "5",
            ["DISTANCE_METRIC"] = "L2",
        }).AddVectorField("v2", Schema.VectorField.VectorAlgo.FLAT, new()
        {
            ["TYPE"] = "BFLOAT16",
            ["DIM"] = "4",
            ["DISTANCE_METRIC"] = "L2",
        });
        Assert.True(ft.Create("idx", new(), schema));

        short[] vec1 = [2, 1, 2, 2, 2];
        byte[] vec1ToBytes = new byte[vec1.Length * sizeof(short)];
        Buffer.BlockCopy(vec1, 0, vec1ToBytes, 0, vec1ToBytes.Length);

        short[] vec2 = [1, 2, 2, 2];
        byte[] vec2ToBytes = new byte[vec2.Length * sizeof(short)];
        Buffer.BlockCopy(vec2, 0, vec2ToBytes, 0, vec2ToBytes.Length);

        var entries = new HashEntry[] { new("v", vec1ToBytes), new("v2", vec2ToBytes) };
        db.HashSet("a", entries);
        db.HashSet("b", entries);
        db.HashSet("c", entries);

        var q = new Query("*=>[KNN 2 @v $vec]").ReturnFields("__v_score");
        var res = ft.Search("idx", q.AddParam("vec", vec1ToBytes));
        Assert.Equal(2, res.TotalResults);

        q = new Query("*=>[KNN 2 @v2 $vec]").ReturnFields("__v_score");
        res = ft.Search("idx", q.AddParam("vec", vec2ToBytes));
        Assert.Equal(2, res.TotalResults);
    }

    [SkipIfRedisTheory(Comparison.LessThan, "7.9.0")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestCreateInt8VectorField(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        // ReSharper disable once RedundantArgumentDefaultValue
        var ft = db.FT(2);
        var schema = new Schema().AddVectorField("v", Schema.VectorField.VectorAlgo.FLAT, new()
        {
            ["TYPE"] = "INT8",
            ["DIM"] = "5",
            ["DISTANCE_METRIC"] = "L2",
        }).AddVectorField("v2", Schema.VectorField.VectorAlgo.FLAT, new()
        {
            ["TYPE"] = "UINT8",
            ["DIM"] = "4",
            ["DISTANCE_METRIC"] = "L2",
        });
        Assert.True(ft.Create("idx", new(), schema));

        byte[] vec1 = [2, 1, 2, 2, 2];
        byte[] vec2 = [1, 2, 2, 2];

        var entries = new HashEntry[] { new("v", vec1), new("v2", vec2) };
        db.HashSet("a", entries);
        db.HashSet("b", entries);
        db.HashSet("c", entries);

        var q = new Query("*=>[KNN 2 @v $vec]").ReturnFields("__v_score");
        var res = ft.Search("idx", q.AddParam("vec", vec1));
        Assert.Equal(2, res.TotalResults);

        q = new Query("*=>[KNN 2 @v2 $vec]").ReturnFields("__v_score");
        res = ft.Search("idx", q.AddParam("vec", vec2));
        Assert.Equal(2, res.TotalResults);
    }

    [Fact]
    public void TestMissingSortableFieldCommandArgs()
    {
        string idx = "MISSING_EMPTY_SORTABLE_INDEX";
        Schema sc = new Schema()
            // ReSharper disable once RedundantArgumentDefaultValue
            .AddTextField("text1", 1.0, missingIndex: true, emptyIndex: true, sortable: true)
            .AddTagField("tag1", missingIndex: true, emptyIndex: true, sortable: true)
            .AddNumericField("numeric1", missingIndex: true, sortable: true)
            .AddGeoField("geo1", missingIndex: true, sortable: true);

        var ftCreateParams = FTCreateParams.CreateParams();

        var cmd = SearchCommandBuilder.Create(idx, ftCreateParams, sc);
        var expectedArgs = new object[]
        {
            idx, "SCHEMA",
            "text1", "TEXT", INDEXMISSING, INDEXEMPTY, SORTABLE,
            "tag1", "TAG", INDEXMISSING, INDEXEMPTY, SORTABLE,
            "numeric1", "NUMERIC", INDEXMISSING, SORTABLE,
            "geo1", "GEO", INDEXMISSING, SORTABLE
        };
        Assert.Equal(expectedArgs, cmd.Args);
    }

    [SkipIfRedisTheory(Comparison.LessThan, "7.3.240")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestCombiningMissingEmptySortableFields(string endpointId)
    {
        string idx = "MISSING_EMPTY_SORTABLE_INDEX";
        IDatabase db = GetCleanDatabase(endpointId);
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
            .AddTextField("text1", 1.0, missingIndex: true, emptyIndex: true, sortable: true)
            .AddTagField("tag1", missingIndex: true, emptyIndex: true, sortable: true)
            .AddNumericField("numeric1", missingIndex: true, sortable: true)
            .AddGeoField("geo1", missingIndex: true, sortable: true)
            .AddGeoShapeField("geoshape1", Schema.GeoShapeField.CoordinateSystem.FLAT, missingIndex: true)
            .AddVectorField("vector1", Schema.VectorField.VectorAlgo.FLAT, vectorAttrs, missingIndex: true);

        var ftCreateParams = FTCreateParams.CreateParams();
        Assert.True(ft.Create(idx, ftCreateParams, sc));

        var sampleHash = new HashEntry[] { new("field1", "value1"), new("field2", "value2") };
        db.HashSet("hashWithMissingFields", sampleHash);

        Polygon polygon = new GeometryFactory().CreatePolygon([new(1, 1), new(10, 10), new(100, 100), new(1, 1)]);

        var hashWithAllFields = new HashEntry[]
        {
            new("text1", "value1"), new("tag1", "value2"), new("numeric1", "3.141"), new("geo1", "-0.441,51.458"),
            new("geoshape1", polygon.ToString()), new("vector1", "aaaaaaaa")
        };
        db.HashSet("hashWithAllFields", hashWithAllFields);

        var result = ft.Search(idx, new("ismissing(@text1)"));
        Assert.Equal(1, result.TotalResults);
        Assert.Equal("hashWithMissingFields", result.Documents[0].Id);

        result = ft.Search(idx, new("ismissing(@tag1)"));
        Assert.Equal(1, result.TotalResults);
        Assert.Equal("hashWithMissingFields", result.Documents[0].Id);

        result = ft.Search(idx, new("ismissing(@numeric1)"));
        Assert.Equal(1, result.TotalResults);
        Assert.Equal("hashWithMissingFields", result.Documents[0].Id);

        result = ft.Search(idx, new("ismissing(@geo1)"));
        Assert.Equal(1, result.TotalResults);
        Assert.Equal("hashWithMissingFields", result.Documents[0].Id);

        result = ft.Search(idx, new("ismissing(@geoshape1)"));
        Assert.Equal(1, result.TotalResults);
        Assert.Equal("hashWithMissingFields", result.Documents[0].Id);

        result = ft.Search(idx, new("ismissing(@vector1)"));
        Assert.Equal(1, result.TotalResults);
        Assert.Equal("hashWithMissingFields", result.Documents[0].Id);
    }

    [SkipIfRedisTheory(Comparison.LessThan, "8.1.240")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestCreate_Float16_Int32_VectorField_Svs(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        // ReSharper disable once RedundantArgumentDefaultValue
        var ft = db.FT(2);
        var schema = new Schema().AddSvsVanamaVectorField("v", Schema.VectorField.VectorType.FLOAT16, 5,
                Schema.VectorField.VectorDistanceMetric.EuclideanDistance)
            .AddSvsVanamaVectorField("v2", Schema.VectorField.VectorType.FLOAT32, 4,
                Schema.VectorField.VectorDistanceMetric.EuclideanDistance);

        var cmd = SearchCommandBuilder.Create("idx", FTCreateParams.CreateParams(), schema).ToString();
        Log(cmd);

        Assert.True(ft.Create("idx", new FTCreateParams(), schema));

        byte[] vec1ToBytes = MemoryMarshal.AsBytes(stackalloc short[] { 2, 1, 2, 2, 2 }).ToArray();
        byte[] vec2ToBytes = MemoryMarshal.AsBytes(stackalloc int[] { 1, 2, 2, 2 }).ToArray();

        HashEntry[] entries = [new("v", vec1ToBytes), new("v2", vec2ToBytes)];
        db.HashSet("a", entries);
        db.HashSet("b", entries);
        db.HashSet("c", entries);

        var q = new Query("*=>[KNN 2 @v $vec]").ReturnFields("__v_score");
        var res = ft.Search("idx", q.AddParam("vec", vec1ToBytes));
        Assert.Equal(2, res.TotalResults);

        q = new Query("*=>[KNN 2 @v2 $vec]").ReturnFields("__v_score");
        res = ft.Search("idx", q.AddParam("vec", vec2ToBytes));
        Assert.Equal(2, res.TotalResults);
    }

    [SkipIfRedisTheory(Comparison.LessThan, "8.1.240")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestCreate_Float16_Int32_VectorField_Svs_WithCompression(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        // ReSharper disable once RedundantArgumentDefaultValue
        var ft = db.FT(2);
        var schema = new Schema().AddSvsVanamaVectorField("v", Schema.VectorField.VectorType.FLOAT16, 5,
                Schema.VectorField.VectorDistanceMetric.EuclideanDistance,
                reducedDimensions: 2, compressionAlgorithm: Schema.VectorField.VectorCompressionAlgorithm.LeanVec4x8)
            .AddSvsVanamaVectorField("v2", Schema.VectorField.VectorType.FLOAT32, 4,
                Schema.VectorField.VectorDistanceMetric.EuclideanDistance,
                compressionAlgorithm: Schema.VectorField.VectorCompressionAlgorithm.LVQ4);

        var cmd = SearchCommandBuilder.Create("idx", FTCreateParams.CreateParams(), schema).ToString();
        Log(cmd);

        Assert.True(ft.Create("idx", new FTCreateParams(), schema));

        byte[] vec1ToBytes = MemoryMarshal.AsBytes(stackalloc short[] { 2, 1, 2, 2, 2 }).ToArray();
        byte[] vec2ToBytes = MemoryMarshal.AsBytes(stackalloc int[] { 1, 2, 2, 2 }).ToArray();

        HashEntry[] entries = [new("v", vec1ToBytes), new("v2", vec2ToBytes)];
        db.HashSet("a", entries);
        db.HashSet("b", entries);
        db.HashSet("c", entries);

        var q = new Query("*=>[KNN 2 @v $vec]").ReturnFields("__v_score");
        var res = ft.Search("idx", q.AddParam("vec", vec1ToBytes));
        Assert.Equal(2, res.TotalResults);

        q = new Query("*=>[KNN 2 @v2 $vec]").ReturnFields("__v_score");
        res = ft.Search("idx", q.AddParam("vec", vec2ToBytes));
        Assert.Equal(2, res.TotalResults);
    }

    [Fact]
    public void TestIndexingCreation_Hnsw_Sq8()
    {
        // COMPRESSION and TRAINING_THRESHOLD are emitted only when set, after the graph parameters
        Schema sc = new Schema()
            .AddHnswVectorField("v1", Schema.VectorField.VectorType.FLOAT32, 4,
                Schema.VectorField.VectorDistanceMetric.EuclideanDistance,
                compressionAlgorithm: Schema.VectorField.VectorCompressionAlgorithm.SQ8)
            .AddHnswVectorField("v2", Schema.VectorField.VectorType.FLOAT16, 4,
                Schema.VectorField.VectorDistanceMetric.CosineDistance,
                maxOutgoingConnections: 8,
                compressionAlgorithm: Schema.VectorField.VectorCompressionAlgorithm.SQ8, trainingThreshold: 2048);

        var cmd = SearchCommandBuilder.Create("IDX_NAME", FTCreateParams.CreateParams(), sc).ToString();

        Assert.Equal(
            "FT.CREATE IDX_NAME SCHEMA v1 VECTOR HNSW 8 DIM 4 TYPE FLOAT32 DISTANCE_METRIC L2 COMPRESSION SQ8 " +
            "v2 VECTOR HNSW 12 DIM 4 TYPE FLOAT16 DISTANCE_METRIC COSINE M 8 COMPRESSION SQ8 TRAINING_THRESHOLD 2048",
            cmd);
    }

    // HNSW SQ8 compression (RediSearch MOD-14958): FLOAT32/FLOAT16 with L2/IP/COSINE; the server owns the validation.
    [SkipIfRedisTheory(Comparison.LessThan, "8.12.0")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestCreate_Hnsw_Sq8(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        var schema = new Schema()
            .AddHnswVectorField("v", Schema.VectorField.VectorType.FLOAT32, 4,
                Schema.VectorField.VectorDistanceMetric.EuclideanDistance,
                compressionAlgorithm: Schema.VectorField.VectorCompressionAlgorithm.SQ8)
            .AddHnswVectorField("v2", Schema.VectorField.VectorType.FLOAT16, 4,
                Schema.VectorField.VectorDistanceMetric.CosineDistance,
                compressionAlgorithm: Schema.VectorField.VectorCompressionAlgorithm.SQ8, trainingThreshold: 2048)
            .AddHnswVectorField("plain", Schema.VectorField.VectorType.FLOAT32, 4,
                Schema.VectorField.VectorDistanceMetric.EuclideanDistance);

        Assert.True(ft.Create("idx", new FTCreateParams(), schema));

        // FT.INFO reports the stored configuration; the server default threshold is 10240, and an
        // uncompressed field carries neither key
        var attrs = ft.Info("idx").Attributes.ToDictionary(a => a["identifier"].ToString()!);
        Assert.Equal("SQ8", attrs["v"]["compression"].ToString());
        Assert.Equal(10240, (long)attrs["v"]["training_threshold"]);
        Assert.Equal("SQ8", attrs["v2"]["compression"].ToString());
        Assert.Equal(2048, (long)attrs["v2"]["training_threshold"]);
        Assert.False(attrs["plain"].ContainsKey("compression"));
        Assert.False(attrs["plain"].ContainsKey("training_threshold"));

        // vectors are searchable while still below the training threshold
        byte[] vec = MemoryMarshal.AsBytes(stackalloc float[] { 1, 2, 3, 4 }).ToArray();
        byte[] vec16 = MemoryMarshal.AsBytes(stackalloc short[] { 1, 2, 3, 4 }).ToArray();
        for (int i = 0; i < 3; i++)
        {
            db.HashSet($"doc:{i}", [new("v", vec), new("v2", vec16), new("plain", vec)]);
        }
        var res = ft.Search("idx", new Query("*=>[KNN 2 @v $vec]").ReturnFields("__v_score").AddParam("vec", vec));
        Assert.Equal(2, res.TotalResults);
        res = ft.Search("idx", new Query("*=>[KNN 2 @v2 $vec]").ReturnFields("__v2_score").AddParam("vec", vec16));
        Assert.Equal(2, res.TotalResults);
    }

    [SkipIfRedisTheory(Comparison.LessThan, "8.12.0")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestCreate_Hnsw_Sq8_ServerRejectsInvalidCombinations(string endpointId)
    {
        // the client does not validate type/metric/threshold combinations; these are the server's errors, surfaced
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();

        var float64 = new Schema().AddHnswVectorField("v", Schema.VectorField.VectorType.FLOAT64, 4,
            Schema.VectorField.VectorDistanceMetric.EuclideanDistance,
            compressionAlgorithm: Schema.VectorField.VectorCompressionAlgorithm.SQ8);
        var ex = Assert.Throws<RedisServerException>(() => ft.Create("idx64", new FTCreateParams(), float64));
        Assert.Contains("COMPRESSION is only supported for FLOAT32 and FLOAT16", ex.Message);

        var thresholdOnly = new Schema().AddHnswVectorField("v", Schema.VectorField.VectorType.FLOAT32, 4,
            Schema.VectorField.VectorDistanceMetric.EuclideanDistance, trainingThreshold: 100);
        ex = Assert.Throws<RedisServerException>(() => ft.Create("idxThreshold", new FTCreateParams(), thresholdOnly));
        Assert.Contains("TRAINING_THRESHOLD is irrelevant when compression was not requested", ex.Message);

        var svsOnlyValue = new Schema().AddHnswVectorField("v", Schema.VectorField.VectorType.FLOAT32, 4,
            Schema.VectorField.VectorDistanceMetric.EuclideanDistance,
            compressionAlgorithm: Schema.VectorField.VectorCompressionAlgorithm.LVQ8);
        Assert.Throws<RedisServerException>(() => ft.Create("idxLvq", new FTCreateParams(), svsOnlyValue));
    }

    [Fact]
    public void TestIndexingCreation_Default()
    {
        Schema sc = new Schema()
            .AddFlatVectorField("vector1", Schema.VectorField.VectorType.FLOAT32, 2,
                Schema.VectorField.VectorDistanceMetric.EuclideanDistance, missingIndex: true)
            .AddHnswVectorField("vector2", Schema.VectorField.VectorType.FLOAT64, 3,
                Schema.VectorField.VectorDistanceMetric.CosineDistance, missingIndex: false)
            .AddSvsVanamaVectorField("vector3", Schema.VectorField.VectorType.FLOAT16, 4,
                Schema.VectorField.VectorDistanceMetric.InnerProduct, missingIndex: true);

        var ftCreateParams = FTCreateParams.CreateParams();
        var cmd = SearchCommandBuilder.Create("IDX_NAME", ftCreateParams, sc).ToString();

        Assert.Equal(
            "FT.CREATE IDX_NAME SCHEMA vector1 VECTOR FLAT 6 DIM 2 TYPE FLOAT32 DISTANCE_METRIC L2 INDEXMISSING vector2 VECTOR HNSW 6 DIM 3 TYPE FLOAT64 DISTANCE_METRIC COSINE vector3 VECTOR SVS-VAMANA 6 DIM 4 TYPE FLOAT16 DISTANCE_METRIC IP INDEXMISSING",
            cmd);
    }

    [Fact]
    public void TestIndexingCreation_WithAttribs()
    {
        Schema sc = new Schema()
            .AddFlatVectorField("vector1", Schema.VectorField.VectorType.NotSpecified, 0,
                Schema.VectorField.VectorDistanceMetric.NotSpecified, missingIndex: true,
                attributes: new Dictionary<string, object>()
                {
                    ["TYPE"] = "FUT1", // some values not representable in the old API
                    ["DIM"] = "FUT2",
                    ["DISTANCE_METRIC"] = "FUT3",
                    ["NEW_FIELD"] = "NEW_VALUE",
                });

        var ftCreateParams = FTCreateParams.CreateParams();
        var cmd = SearchCommandBuilder.Create("IDX_NAME", ftCreateParams, sc).ToString();

        Assert.Equal(
            "FT.CREATE IDX_NAME SCHEMA vector1 VECTOR FLAT 8 TYPE FUT1 DIM FUT2 DISTANCE_METRIC FUT3 NEW_FIELD NEW_VALUE INDEXMISSING",
            cmd);
    }

    [Fact]
    public void TestIndexingCreation_Custom_Everything()
    {
        Schema sc = new Schema()
            .AddFlatVectorField("vector1", Schema.VectorField.VectorType.FLOAT32, 2,
                Schema.VectorField.VectorDistanceMetric.EuclideanDistance, missingIndex: true)
            .AddHnswVectorField("vector2", Schema.VectorField.VectorType.FLOAT64, 3,
                Schema.VectorField.VectorDistanceMetric.CosineDistance,
                maxOutgoingConnections: 10, maxConnectedNeighbors: 20, maxTopCandidates: 30, boundaryFactor: 0.7,
                missingIndex: false)
            .AddSvsVanamaVectorField("vector3", Schema.VectorField.VectorType.FLOAT16, 4,
                Schema.VectorField.VectorDistanceMetric.InnerProduct,
                compressionAlgorithm: Schema.VectorField.VectorCompressionAlgorithm.LeanVec4x8,
                constructionWindowSize: 35, graphMaxDegree: 17, searchWindowSize: 30,
                rangeSearchApproximationFactor: 0.5,
                trainingThreshold: 100, reducedDimensions: 50,
                missingIndex: true);

        var ftCreateParams = FTCreateParams.CreateParams();
        var cmd = SearchCommandBuilder.Create("IDX_NAME", ftCreateParams, sc).ToString();

        Assert.Equal(
            "FT.CREATE IDX_NAME SCHEMA vector1 VECTOR FLAT 6 DIM 2 TYPE FLOAT32 DISTANCE_METRIC L2 INDEXMISSING " +
            "vector2 VECTOR HNSW 14 DIM 3 TYPE FLOAT64 DISTANCE_METRIC COSINE M 10 EF_CONSTRUCTION 20 EF_RUNTIME 30 EPSILON 0.7 " +
            "vector3 VECTOR SVS-VAMANA 20 COMPRESSION LeanVec4x8 DIM 4 TYPE FLOAT16 DISTANCE_METRIC IP CONSTRUCTION_WINDOW_SIZE 35 GRAPH_MAX_DEGREE 17 SEARCH_WINDOW_SIZE 30 EPSILON 0.5 TRAINING_THRESHOLD 100 REDUCE 50 INDEXMISSING",
            cmd);
    }
}