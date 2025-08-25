// EXAMPLE: geoindex

// STEP_START import
using NRedisStack.RedisStackCommands;
using NRedisStack.Search;
using NRedisStack.Search.Literals.Enums;
using StackExchange.Redis;
// STEP_END

// REMOVE_START
using NRedisStack.Tests;

namespace Doc;
[Collection("DocsTests")]
// REMOVE_END

// HIDE_START
public class GeoIndexExample
// REMOVE_START
: AbstractNRedisStackTest, IDisposable
// REMOVE_END
{
    // REMOVE_START
    public GeoIndexExample(EndpointsFixture fixture) : base(fixture) { }

    [SkippableFact]
    // REMOVE_END
    public void run()
    {
        //REMOVE_START
        // This is needed because we're constructing ConfigurationOptions in the test before calling GetConnection
        SkipIfTargetConnectionDoesNotExist(EndpointsFixture.Env.Standalone);
        var _ = GetCleanDatabase(EndpointsFixture.Env.Standalone);
        //REMOVE_END
        var muxer = ConnectionMultiplexer.Connect("localhost:6379");
        var db = muxer.GetDatabase();
        //REMOVE_START
        // Clear any keys here before using them in tests.
        db.KeyDelete(new RedisKey[] { "product:46885", "product:46886", "shape:1", "shape:2", "shape:3", "shape:4" });
        try { db.FT().DropIndex("productidx"); } catch { }
        try { db.FT().DropIndex("geomidx"); } catch { }
        //REMOVE_END
        // HIDE_END

        // STEP_START create_geo_idx
        Schema geoSchema = new Schema()
            .AddGeoField(new FieldName("$.location", "location"));

        bool geoCreateResult = db.FT().Create(
            "productidx",
            new FTCreateParams()
                .On(IndexDataType.JSON)
                .Prefix("product:"),
            geoSchema
        );
        Console.WriteLine(geoCreateResult); // >>> True
        // STEP_END
        // REMOVE_START
        Assert.True(geoCreateResult);
        // REMOVE_END

        // STEP_START add_geo_json
        var product46885 = new
        {
            description = "Navy Blue Slippers",
            price = 45.99,
            city = "Denver",
            location = "-104.991531, 39.742043"
        };

        bool gjAddResult1 = db.JSON().Set("product:46885", "$", product46885);
        Console.WriteLine(gjAddResult1); // >>> True

        var product46886 = new
        {
            description = "Bright Green Socks",
            price = 25.50,
            city = "Fort Collins",
            location = "-105.0618814,40.5150098"
        };

        bool gjAddResult2 = db.JSON().Set("product:46886", "$", product46886);
        Console.WriteLine(gjAddResult2); // >>> True
        // STEP_END
        // REMOVE_START
        Assert.True(gjAddResult1);
        Assert.True(gjAddResult2);
        // REMOVE_END

        // STEP_START geo_query
        SearchResult geoQueryResult = db.FT().Search(
            "productidx",
            new Query("@location:[-104.800644 38.846127 100 mi]")
        );
        Console.WriteLine(geoQueryResult.Documents.Count); // >>> 1

        Console.WriteLine(
            string.Join(", ", geoQueryResult.Documents.Select(x => x["json"]))
        );
        // >>> {"description":"Navy Blue Slippers","price":45.99,"city":"Denver"...
        // STEP_END
        // REMOVE_START
        Assert.Single(geoQueryResult.Documents);
        Assert.Equal(
            "{\"description\":\"Navy Blue Slippers\",\"price\":45.99,\"city\":\"Denver\",\"location\":\"-104.991531, 39.742043\"}",
            string.Join(", ", geoQueryResult.Documents.Select(x => x["json"]))
        );
        // REMOVE_END

        // STEP_START create_gshape_idx
        Schema geomSchema = new Schema()
            .AddGeoShapeField(
                new FieldName("$.geom", "geom"),
                Schema.GeoShapeField.CoordinateSystem.FLAT
            )
            .AddTextField(new FieldName("$.name", "name"));

        bool geomCreateResult = db.FT().Create(
            "geomidx",
            new FTCreateParams()
                .On(IndexDataType.JSON)
                .Prefix("shape:"),
                geomSchema
        );
        Console.WriteLine(geomCreateResult); // >>> True
        // STEP_END
        // REMOVE_START
        Assert.True(geomCreateResult);
        // REMOVE_END

        // STEP_START add_gshape_json
        var shape1 = new
        {
            name = "Green Square",
            geom = "POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))"
        };

        bool gmJsonRes1 = db.JSON().Set("shape:1", "$", shape1);
        Console.WriteLine(gmJsonRes1); // >>> True

        var shape2 = new
        {
            name = "Red Rectangle",
            geom = "POLYGON ((2 2.5, 2 3.5, 3.5 3.5, 3.5 2.5, 2 2.5))"
        };

        bool gmJsonRes2 = db.JSON().Set("shape:2", "$", shape2);
        Console.WriteLine(gmJsonRes1); // >>> True

        var shape3 = new
        {
            name = "Blue Triangle",
            geom = "POLYGON ((3.5 1, 3.75 2, 4 1, 3.5 1))"
        };

        bool gmJsonRes3 = db.JSON().Set("shape:3", "$", shape3);
        Console.WriteLine(gmJsonRes3); // >>> True

        var shape4 = new
        {
            name = "Purple Point",
            geom = "POINT (2 2)"
        };

        bool gmJsonRes4 = db.JSON().Set("shape:4", "$", shape4);
        Console.WriteLine(gmJsonRes3); // >>> True
        // STEP_END
        // REMOVE_START
        Assert.True(gmJsonRes1);
        Assert.True(gmJsonRes2);
        Assert.True(gmJsonRes3);
        Assert.True(gmJsonRes4);
        // REMOVE_END

        // STEP_START gshape_query
        SearchResult geomQueryResult = db.FT().Search(
            "geomidx",
            new Query("(-@name:(Green Square) @geom:[WITHIN $qshape])")
                .AddParam("qshape", "POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))")
                .Limit(0, 1)
                .Dialect(4)
        );

        Console.WriteLine(geomQueryResult.Documents.Count); // >>> 1
        var res = string.Join(", ", geomQueryResult.Documents.Select(x => x["json"]));

        Console.WriteLine(
            string.Join(", ", geomQueryResult.Documents.Select(x => x["json"]))
        );
        // >>> [{"name":"Purple Point","geom":"POINT (2 2)"}]
        // STEP_END
        // REMOVE_START
        Assert.Single(geomQueryResult.Documents);
        Assert.Equal(
            "[{\"name\":\"Purple Point\",\"geom\":\"POINT (2 2)\"}]",
            string.Join(", ", geomQueryResult.Documents.Select(x => x["json"]))
        );
        // REMOVE_END
        // HIDE_START
    }
}
// HIDE_END
