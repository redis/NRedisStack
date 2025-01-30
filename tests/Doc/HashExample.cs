// EXAMPLE: hash_tutorial
// HIDE_START

using NRedisStack.Tests;
using StackExchange.Redis;

//REMOVE_START
namespace Doc;
[Collection("DocsTests")]
//REMOVE_END
public class HashExample : AbstractNRedisStackTest, IDisposable
{
    public HashExample(EndpointsFixture fixture) : base(fixture) { }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void run(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        db.KeyDelete("bike:1");
        //HIDE_END
        //STEP_START set_get_all
        db.HashSet("bike:1", new HashEntry[]
        {
            new HashEntry("model", "Deimos"),
            new HashEntry("brand", "Ergonom"),
            new HashEntry("type", "Enduro bikes"),
            new HashEntry("price", 4972)
        });

        Console.WriteLine("Hash Created");
        // Hash Created

        var model = db.HashGet("bike:1", "model");
        Console.WriteLine($"Model: {model}");
        // Model: Deimos

        var price = db.HashGet("bike:1", "price");
        Console.WriteLine($"Price: {price}");
        // Price: 4972

        var bike = db.HashGetAll("bike:1");
        Console.WriteLine("bike:1");
        Console.WriteLine(string.Join("\n", bike.Select(b => $"{b.Name}: {b.Value}")));
        // Bike:1:
        // model: Deimos
        // brand: Ergonom
        // type: Enduro bikes
        // price: 4972
        //STEP_END

        //REMOVE_START
        Assert.Equal(4, bike.Length);
        Assert.Equal("Deimos", model);
        Assert.Equal(4972, price);
        //REMOVE_END

        //STEP_START hmget
        var values = db.HashGet("bike:1", new RedisValue[] { "model", "price" });
        Console.WriteLine(string.Join(" ", values));
        // Deimos 4972
        //REMOVE_START
        Assert.Equal("Deimos", values[0]);
        Assert.Equal(4972, values[1]);
        //REMOVE_END
        //STEP_END

        //STEP_START hincrby
        var newPrice = db.HashIncrement("bike:1", "price", 100);
        Console.WriteLine($"New price: {newPrice}");
        //REMOVE_START
        Assert.Equal(5072, newPrice);
        //REMOVE_END
        // New price: 5072

        newPrice = db.HashIncrement("bike:1", "price", -100);
        Console.WriteLine($"New price: {newPrice}");
        //REMOVE_START
        Assert.Equal(4972, newPrice);
        //REMOVE_END
        // New price: 4972
        //STEP_END

        //STEP_START incrby_get_mget
        var rides = db.HashIncrement("bike:1", "rides");
        Console.WriteLine($"Rides: {rides}");
        //REMOVE_START
        Assert.Equal(1, rides);
        //REMOVE_END
        // Rides: 1

        rides = db.HashIncrement("bike:1", "rides");
        Console.WriteLine($"Rides: {rides}");
        //REMOVE_START
        Assert.Equal(2, rides);
        //REMOVE_END
        // Rides: 2

        rides = db.HashIncrement("bike:1", "rides");
        Console.WriteLine($"Rides: {rides}");
        //REMOVE_START
        Assert.Equal(3, rides);
        //REMOVE_END
        // Rides: 3

        var crashes = db.HashIncrement("bike:1", "crashes");
        Console.WriteLine($"Crashes: {crashes}");
        //REMOVE_START
        Assert.Equal(1, crashes);
        //REMOVE_END
        // Crashes: 1

        var owners = db.HashIncrement("bike:1", "owners");
        Console.WriteLine($"Owners: {owners}");
        //REMOVE_START
        Assert.Equal(1, owners);
        //REMOVE_END
        // Owners: 1

        var stats = db.HashGet("bike:1", new RedisValue[] { "crashes", "owners" });
        Console.WriteLine($"Bike stats: crashes={stats[0]}, owners={stats[1]}");
        //REMOVE_START
        Assert.Equal(1, stats[0]);
        Assert.Equal(1, stats[1]);
        //REMOVE_END
        // Bike stats: crashes=1, owners=1
        //STEP_END
        //HIDE_START
    }
}
//HIDE_END