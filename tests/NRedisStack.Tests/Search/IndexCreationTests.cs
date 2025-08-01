using System.Runtime.InteropServices;
using StackExchange.Redis;
using NRedisStack.Search;
using NRedisStack.RedisStackCommands;
using Xunit;
using NetTopologySuite.Geometries;
using Xunit.Abstractions;

namespace NRedisStack.Tests.Search;

public class IndexCreationTests : AbstractNRedisStackTest, IDisposable
{
    private readonly string index = "MISSING_EMPTY_INDEX";

    public IndexCreationTests(EndpointsFixture endpointsFixture, ITestOutputHelper log) : base(endpointsFixture, log)
    {
    }

    private readonly ITestOutputHelper log;
    private static readonly string INDEXMISSING = "INDEXMISSING";
    private static readonly string INDEXEMPTY = "INDEXEMPTY";
    private static readonly string SORTABLE = "SORTABLE";

    [Fact]
    public void TestMissingEmptyFieldCommandArgs()
    {
        Schema sc = new Schema()
                .AddTextField("text1", 1.0, missingIndex: true, emptyIndex: true)
                .AddTagField("tag1", missingIndex: true, emptyIndex: true)
                .AddNumericField("numeric1", missingIndex: true)
                .AddGeoField("geo1", missingIndex: true)
                .AddGeoShapeField("geoshape1", Schema.GeoShapeField.CoordinateSystem.FLAT, missingIndex: true)
                .AddVectorField("vector1", Schema.VectorField.VectorAlgo.FLAT, missingIndex: true);

        var ftCreateParams = FTCreateParams.CreateParams();

        var cmd = SearchCommandBuilder.Create(index, ftCreateParams, sc);
        var expectedArgs = new object[] { "MISSING_EMPTY_INDEX", "SCHEMA",
                                            "text1","TEXT",INDEXMISSING,INDEXEMPTY,
                                            "tag1","TAG", INDEXMISSING,INDEXEMPTY,
                                            "numeric1","NUMERIC", INDEXMISSING,
                                            "geo1","GEO", INDEXMISSING,
                                            "geoshape1","GEOSHAPE", "FLAT", INDEXMISSING,
                                            "vector1","VECTOR","FLAT", 0, INDEXMISSING};
        Assert.Equal(expectedArgs, cmd.Args);
    }

    [SkipIfRedis(Comparison.LessThan, "7.3.240")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestMissingFields(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT(2);
        var vectorAttrs = new Dictionary<string, object>()
        {
            ["TYPE"] = "FLOAT32",
            ["DIM"] = "2",
            ["DISTANCE_METRIC"] = "L2",
        };
        Schema sc = new Schema()
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

        Polygon polygon = new GeometryFactory().CreatePolygon(new Coordinate[] { new Coordinate(1, 1), new Coordinate(10, 10), new Coordinate(100, 100), new Coordinate(1, 1), });

        var hashWithAllFields = new HashEntry[] { new("text1", "value1"), new("tag1", "value2"), new("numeric1", "3.141"), new("geo1", "-0.441,51.458"), new("geoshape1", polygon.ToString()), new("vector1", "aaaaaaaa") };
        db.HashSet("hashWithAllFields", hashWithAllFields);

        var result = ft.Search(index, new Query("ismissing(@text1)"));
        Assert.Equal(1, result.TotalResults);
        Assert.Equal("hashWithMissingFields", result.Documents[0].Id);

        result = ft.Search(index, new Query("ismissing(@tag1)"));
        Assert.Equal(1, result.TotalResults);
        Assert.Equal("hashWithMissingFields", result.Documents[0].Id);

        result = ft.Search(index, new Query("ismissing(@numeric1)"));
        Assert.Equal(1, result.TotalResults);
        Assert.Equal("hashWithMissingFields", result.Documents[0].Id);

        result = ft.Search(index, new Query("ismissing(@geo1)"));
        Assert.Equal(1, result.TotalResults);
        Assert.Equal("hashWithMissingFields", result.Documents[0].Id);

        result = ft.Search(index, new Query("ismissing(@geoshape1)"));
        Assert.Equal(1, result.TotalResults);
        Assert.Equal("hashWithMissingFields", result.Documents[0].Id);

        result = ft.Search(index, new Query("ismissing(@vector1)"));
        Assert.Equal(1, result.TotalResults);
        Assert.Equal("hashWithMissingFields", result.Documents[0].Id);
    }

    [SkipIfRedis(Comparison.LessThan, "7.3.240")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestEmptyFields(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT(2);
        Schema sc = new Schema()
           .AddTextField("text1", 1.0, emptyIndex: true)
           .AddTagField("tag1", emptyIndex: true);

        var ftCreateParams = FTCreateParams.CreateParams();
        Assert.True(ft.Create(index, ftCreateParams, sc));

        var hashWithMissingFields = new HashEntry[] { new("text1", ""), new("tag1", "") };
        db.HashSet("hashWithEmptyFields", hashWithMissingFields);

        var hashWithAllFields = new HashEntry[] { new("text1", "value1"), new("tag1", "value2") };
        db.HashSet("hashWithAllFields", hashWithAllFields);

        var result = ft.Search(index, new Query("@text1:''"));
        Assert.Equal(1, result.TotalResults);
        Assert.Equal("hashWithEmptyFields", result.Documents[0].Id);

        result = ft.Search(index, new Query("@tag1:{''}"));
        Assert.Equal(1, result.TotalResults);
        Assert.Equal("hashWithEmptyFields", result.Documents[0].Id);

    }

    [SkipIfRedis(Comparison.LessThan, "7.3.240")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestCreateFloat16VectorField(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT(2);
        var schema = new Schema().AddVectorField("v", Schema.VectorField.VectorAlgo.FLAT, new Dictionary<string, object>()
        {
            ["TYPE"] = "FLOAT16",
            ["DIM"] = "5",
            ["DISTANCE_METRIC"] = "L2",
        }).AddVectorField("v2", Schema.VectorField.VectorAlgo.FLAT, new Dictionary<string, object>()
        {
            ["TYPE"] = "BFLOAT16",
            ["DIM"] = "4",
            ["DISTANCE_METRIC"] = "L2",
        });

        Assert.True(ft.Create("idx", new FTCreateParams(), schema));

        short[] vec1 = new short[] { 2, 1, 2, 2, 2 };
        byte[] vec1ToBytes = new byte[vec1.Length * sizeof(short)];
        Buffer.BlockCopy(vec1, 0, vec1ToBytes, 0, vec1ToBytes.Length);

        short[] vec2 = new short[] { 1, 2, 2, 2 };
        byte[] vec2ToBytes = new byte[vec2.Length * sizeof(short)];
        Buffer.BlockCopy(vec2, 0, vec2ToBytes, 0, vec2ToBytes.Length);

        var entries = new HashEntry[] { new HashEntry("v", vec1ToBytes), new HashEntry("v2", vec2ToBytes) };
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

    [SkipIfRedis(Comparison.LessThan, "8.1.240")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestCreate_Float16_Int32_VectorField_Svs(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
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

        var entries = new HashEntry[] { new HashEntry("v", vec1ToBytes), new HashEntry("v2", vec2ToBytes) };
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

    [SkipIfRedis(Comparison.LessThan, "8.1.240")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestCreate_Float16_Int32_VectorField_Svs_WithCompression(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
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

        var entries = new HashEntry[] { new HashEntry("v", vec1ToBytes), new HashEntry("v2", vec2ToBytes) };
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

    [SkipIfRedis(Comparison.LessThan, "7.9.0")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestCreateInt8VectorField(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT(2);
        var schema = new Schema().AddVectorField("v", Schema.VectorField.VectorAlgo.FLAT, new Dictionary<string, object>()
        {
            ["TYPE"] = "INT8",
            ["DIM"] = "5",
            ["DISTANCE_METRIC"] = "L2",
        }).AddVectorField("v2", Schema.VectorField.VectorAlgo.FLAT, new Dictionary<string, object>()
        {
            ["TYPE"] = "UINT8",
            ["DIM"] = "4",
            ["DISTANCE_METRIC"] = "L2",
        });
        Assert.True(ft.Create("idx", new FTCreateParams(), schema));

        byte[] vec1 = new byte[] { 2, 1, 2, 2, 2 };
        byte[] vec2 = new byte[] { 1, 2, 2, 2 };

        var entries = new HashEntry[] { new HashEntry("v", vec1), new HashEntry("v2", vec2) };
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
                .AddTextField("text1", 1.0, missingIndex: true, emptyIndex: true, sortable: true)
                .AddTagField("tag1", missingIndex: true, emptyIndex: true, sortable: true)
                .AddNumericField("numeric1", missingIndex: true, sortable: true)
                .AddGeoField("geo1", missingIndex: true, sortable: true);

        var ftCreateParams = FTCreateParams.CreateParams();

        var cmd = SearchCommandBuilder.Create(idx, ftCreateParams, sc);
        var expectedArgs = new object[] { idx, "SCHEMA",
                                            "text1","TEXT",INDEXMISSING,INDEXEMPTY,SORTABLE,
                                            "tag1","TAG", INDEXMISSING,INDEXEMPTY,SORTABLE,
                                            "numeric1","NUMERIC", INDEXMISSING,SORTABLE,
                                            "geo1","GEO", INDEXMISSING, SORTABLE};
        Assert.Equal(expectedArgs, cmd.Args);
    }

    [SkipIfRedis(Comparison.LessThan, "7.3.240")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestCombiningMissingEmptySortableFields(string endpointId)
    {
        string idx = "MISSING_EMPTY_SORTABLE_INDEX";
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT(2);
        var vectorAttrs = new Dictionary<string, object>()
        {
            ["TYPE"] = "FLOAT32",
            ["DIM"] = "2",
            ["DISTANCE_METRIC"] = "L2",
        };
        Schema sc = new Schema()
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

        Polygon polygon = new GeometryFactory().CreatePolygon(new Coordinate[] { new Coordinate(1, 1), new Coordinate(10, 10), new Coordinate(100, 100), new Coordinate(1, 1), });

        var hashWithAllFields = new HashEntry[] { new("text1", "value1"), new("tag1", "value2"), new("numeric1", "3.141"), new("geo1", "-0.441,51.458"), new("geoshape1", polygon.ToString()), new("vector1", "aaaaaaaa") };
        db.HashSet("hashWithAllFields", hashWithAllFields);

        var result = ft.Search(idx, new Query("ismissing(@text1)"));
        Assert.Equal(1, result.TotalResults);
        Assert.Equal("hashWithMissingFields", result.Documents[0].Id);

        result = ft.Search(idx, new Query("ismissing(@tag1)"));
        Assert.Equal(1, result.TotalResults);
        Assert.Equal("hashWithMissingFields", result.Documents[0].Id);

        result = ft.Search(idx, new Query("ismissing(@numeric1)"));
        Assert.Equal(1, result.TotalResults);
        Assert.Equal("hashWithMissingFields", result.Documents[0].Id);

        result = ft.Search(idx, new Query("ismissing(@geo1)"));
        Assert.Equal(1, result.TotalResults);
        Assert.Equal("hashWithMissingFields", result.Documents[0].Id);

        result = ft.Search(idx, new Query("ismissing(@geoshape1)"));
        Assert.Equal(1, result.TotalResults);
        Assert.Equal("hashWithMissingFields", result.Documents[0].Id);

        result = ft.Search(idx, new Query("ismissing(@vector1)"));
        Assert.Equal(1, result.TotalResults);
        Assert.Equal("hashWithMissingFields", result.Documents[0].Id);
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

        Assert.Equal("FT.CREATE IDX_NAME SCHEMA vector1 VECTOR FLAT 6 DIM 2 TYPE FLOAT32 DISTANCE_METRIC L2 INDEXMISSING vector2 VECTOR HNSW 6 DIM 3 TYPE FLOAT64 DISTANCE_METRIC COSINE vector3 VECTOR SVS-VAMANA 6 DIM 4 TYPE FLOAT16 DISTANCE_METRIC IP INDEXMISSING", cmd);
    }

    [Fact]
    public void TestIndexingCreation_WithAttribs()
    {
        Schema sc = new Schema()
            .AddFlatVectorField("vector1", Schema.VectorField.VectorType.NotSpecified, 0,
                Schema.VectorField.VectorDistanceMetric.NotSpecified, missingIndex: true, attributes: new Dictionary<string, object>()
                {
                    ["TYPE"] = "FUT1", // some values not representable in the old API
                    ["DIM"] = "FUT2",
                    ["DISTANCE_METRIC"] = "FUT3",
                    ["NEW_FIELD"] = "NEW_VALUE",
                });

        var ftCreateParams = FTCreateParams.CreateParams();
        var cmd = SearchCommandBuilder.Create("IDX_NAME", ftCreateParams, sc).ToString();

        Assert.Equal("FT.CREATE IDX_NAME SCHEMA vector1 VECTOR FLAT 8 TYPE FUT1 DIM FUT2 DISTANCE_METRIC FUT3 NEW_FIELD NEW_VALUE INDEXMISSING", cmd);
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
                constructionWindowSize: 35, graphMaxDegree: 17, searchWindowSize: 30, rangeSearchApproximationFactor: 0.5,
                trainingThreshold: 100, reducedDimensions: 50,
                missingIndex: true);

        var ftCreateParams = FTCreateParams.CreateParams();
        var cmd = SearchCommandBuilder.Create("IDX_NAME", ftCreateParams, sc).ToString();

        Assert.Equal("FT.CREATE IDX_NAME SCHEMA vector1 VECTOR FLAT 6 DIM 2 TYPE FLOAT32 DISTANCE_METRIC L2 INDEXMISSING " +
                    "vector2 VECTOR HNSW 14 DIM 3 TYPE FLOAT64 DISTANCE_METRIC COSINE M 10 EF_CONSTRUCTION 20 EF_RUNTIME 30 EPSILON 0.7 " +
                    "vector3 VECTOR SVS-VAMANA 20 COMPRESSION LeanVec4x8 DIM 4 TYPE FLOAT16 DISTANCE_METRIC IP CONSTRUCTION_WINDOW_SIZE 35 GRAPH_MAX_DEGREE 17 SEARCH_WINDOW_SIZE 30 EPSILON 0.5 TRAINING_THRESHOLD 100 REDUCE 50 INDEXMISSING", cmd);
    }
}