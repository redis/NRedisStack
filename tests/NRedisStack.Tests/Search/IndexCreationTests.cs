using StackExchange.Redis;
using NRedisStack.Search;
using NRedisStack.RedisStackCommands;
using Xunit;
using NetTopologySuite.Geometries;

namespace NRedisStack.Tests.Search;

public class IndexCreationTests : AbstractNRedisStackTest, IDisposable
{
    private readonly string index = "MISSING_EMPTY_INDEX";

    public IndexCreationTests(EndpointsFixture endpointsFixture) : base(endpointsFixture)
    {
    }

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
                                            "vector1","VECTOR","FLAT", INDEXMISSING};
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
           .AddGeoField("geo1", missingIndex: true,  sortable: true)
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


}