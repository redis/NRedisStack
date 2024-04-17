// EXAMPLE: geo_tutorial
// HIDE_START

using NRedisStack.Tests;
using StackExchange.Redis;

// HIDE_END

// REMOVE_START
namespace Doc;
[Collection("DocsTests")]
// REMOVE_END

// HIDE_START
public class Geo_tutorial
{

    [SkipIfRedis(Is.OSSCluster)]
    public void run()
    {
        var muxer = ConnectionMultiplexer.Connect("localhost:6379");
        var db = muxer.GetDatabase();
        //REMOVE_START
        // Clear any keys here before using them in tests.
        db.KeyDelete("bikes:rentable");
        //REMOVE_END
        // HIDE_END


        // STEP_START geoadd
        bool res1 = db.GeoAdd("bikes:rentable", -122.27652, 37.805186, "station:1");
        Console.WriteLine(res1);    // >>> True

        bool res2 = db.GeoAdd("bikes:rentable", -122.2674626, 37.8062344, "station:2");
        Console.WriteLine(res2);    // >>> True

        bool res3 = db.GeoAdd("bikes:rentable", -122.2469854, 37.8104049, "station:3");
        Console.WriteLine(res3);    // >>> True
        // STEP_END

        // Tests for 'geoadd' step.
        // REMOVE_START
        Assert.True(res1);
        Assert.True(res2);
        Assert.True(res3);
        // REMOVE_END


        // STEP_START geosearch
        GeoRadiusResult[] res4 = db.GeoSearch("bikes:rentable",
            -122.27652,
            37.805186,
            new GeoSearchCircle(5, GeoUnit.Kilometers)
        );

        foreach (GeoRadiusResult member in res4)
        {
            Console.WriteLine($"Member: '{member.Member}', distance: {member.Distance}, position: {member.Position}");
        }
        // >>> Member: 'station:1', distance: 0.0001, position: -122.27652043104172 37.80518485897756
        // >>> Member: 'station:2', distance: 0.8047, position: -122.26745992898941 37.80623423353753
        // >>> Member: 'station:3', distance: 2.6596, position: -122.24698394536972 37.81040384984464
        // STEP_END

        // Tests for 'geosearch' step.
        // REMOVE_START
        Assert.Equal(3, res4.Length);
        Assert.Equal(
            "Member: 'station:1', distance: 0.0001, position: -122.27652043104172 37.80518485897756",
            $"Member: '{res4[0].Member}', distance: {res4[0].Distance}, position: {res4[0].Position}"
        );
        Assert.Equal(
            "Member: 'station:2', distance: 0.8047, position: -122.26745992898941 37.80623423353753",
            $"Member: '{res4[1].Member}', distance: {res4[1].Distance}, position: {res4[1].Position}"
        );
        Assert.Equal(
            "Member: 'station:3', distance: 2.6596, position: -122.24698394536972 37.81040384984464",
            $"Member: '{res4[2].Member}', distance: {res4[2].Distance}, position: {res4[2].Position}"
        );
        // REMOVE_END


        // HIDE_START
    }
}
// HIDE_END

