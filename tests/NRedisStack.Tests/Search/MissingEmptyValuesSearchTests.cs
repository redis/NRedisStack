using StackExchange.Redis;
using NRedisStack.Search;
using NRedisStack.RedisStackCommands;
using Xunit;
using NRedisStack.Search.Literals;
using NetTopologySuite.Geometries;

namespace NRedisStack.Tests.Search;

public class MissingEmptyValuesSearchTests : AbstractNRedisStackTest, IDisposable
{
    private readonly string index = "MISSING_EMPTY_INDEX";
    public MissingEmptyValuesSearchTests(RedisFixture redisFixture) : base(redisFixture) { }

    [SkipIfRedis(Comparison.LessThan, "7.3.240")]
    public void TestMissingEmptyFieldCommandArgs()
    {
        Schema sc = new Schema()
                .AddTextField("text1", 1.0, missingIndex: true)
                .AddTagField("tag1", missingIndex: true)
                .AddNumericField("numeric1", missingIndex: true)
                .AddGeoField("geo1", missingIndex: true)
                .AddGeoShapeField("geoshape1", Schema.GeoShapeField.CoordinateSystem.FLAT, missingIndex: true)
                .AddVectorField("vector1", Schema.VectorField.VectorAlgo.FLAT, missingIndex: true);

        var ftCreateParams = FTCreateParams.CreateParams();

        var cmd = SearchCommandBuilder.Create(index, ftCreateParams, sc);
        var expectedArgs = new object[] { "MISSING_EMPTY_INDEX", "SCHEMA",
                                            "text1","TEXT",FieldOptions.INDEXMISSING,
                                            "tag1","TAG", FieldOptions.INDEXMISSING,
                                            "numeric1","NUMERIC", FieldOptions.INDEXMISSING,
                                            "geo1","GEO", FieldOptions.INDEXMISSING,
                                            "geoshape1","GEOSHAPE", "FLAT", FieldOptions.INDEXMISSING,
                                            "vector1","VECTOR","FLAT", FieldOptions.INDEXMISSING};
        Assert.Equal(expectedArgs, cmd.Args);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.3.240")]
    public void TestMissingFields()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
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

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.3.240")]
    public void TestEmptyFields()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
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
}
