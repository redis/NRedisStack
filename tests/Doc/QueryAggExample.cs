// EXAMPLE: query_agg
// HIDE_START

using NRedisStack.RedisStackCommands;
using NRedisStack.Search;
using NRedisStack.Search.Aggregation;
using NRedisStack.Search.Literals.Enums;
using NRedisStack.Tests;
using StackExchange.Redis;

// HIDE_END

// REMOVE_START
namespace Doc;
[Collection("DocsTests")]
// REMOVE_END

// HIDE_START
public class QueryAggExample
// REMOVE_START
: AbstractNRedisStackTest, IDisposable
// REMOVE_END
{
    // REMOVE_START

    public QueryAggExample(EndpointsFixture fixture) : base(fixture) { }

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
        try { db.FT().DropIndex("idx:bicycle"); } catch { }
        //REMOVE_END

        Schema bikeSchema = new Schema()
            .AddTagField(new FieldName("$.condition", "condition"))
            .AddNumericField(new FieldName("$.price", "price"));

        FTCreateParams bikeParams = new FTCreateParams()
            .AddPrefix("bicycle:")
            .On(IndexDataType.JSON);

        db.FT().Create("idx:bicycle", bikeParams, bikeSchema);

        var bicycles = new object[] {
            new
            {
                pickup_zone = "POLYGON((-74.0610 40.7578, -73.9510 40.7578, -73.9510 40.6678, -74.0610 40.6678, -74.0610 40.7578))",
                store_location = "-74.0060,40.7128",
                brand = "Velorim",
                model = "Jigger",
                price = 270,
                description = "Small and powerful, the Jigger is the best ride for the smallest of tikes! This is the tiniest kids’ pedal bike on the market available without a coaster brake, the Jigger is the vehicle of choice for the rare tenacious little rider raring to go.",
                condition = "new"
            },
            new
            {
                pickup_zone = "POLYGON((-118.2887 34.0972, -118.1987 34.0972, -118.1987 33.9872, -118.2887 33.9872, -118.2887 34.0972))",
                store_location = "-118.2437,34.0522",
                brand = "Bicyk",
                model = "Hillcraft",
                price = 1200,
                description = "Kids want to ride with as little weight as possible. Especially on an incline! They may be at the age when a 27.5\" wheel bike is just too clumsy coming off a 24\" bike. The Hillcraft 26 is just the solution they need!",
                condition = "used"
            },
            new
            {
                pickup_zone = "POLYGON((-87.6848 41.9331, -87.5748 41.9331, -87.5748 41.8231, -87.6848 41.8231, -87.6848 41.9331))",
                store_location = "-87.6298,41.8781",
                brand = "Nord",
                model = "Chook air 5",
                price = 815,
                description = "The Chook Air 5  gives kids aged six years and older a durable and uberlight mountain bike for their first experience on tracks and easy cruising through forests and fields. The lower  top tube makes it easy to mount and dismount in any situation, giving your kids greater safety on the trails.",
                condition = "used"
            },
            new
            {
                pickup_zone = "POLYGON((-80.2433 25.8067, -80.1333 25.8067, -80.1333 25.6967, -80.2433 25.6967, -80.2433 25.8067))",
                store_location = "-80.1918,25.7617",
                brand = "Eva",
                model = "Eva 291",
                price = 3400,
                description = "The sister company to Nord, Eva launched in 2005 as the first and only women-dedicated bicycle brand. Designed by women for women, allEva bikes are optimized for the feminine physique using analytics from a body metrics database. If you like 29ers, try the Eva 291. It’s a brand new bike for 2022.. This full-suspension, cross-country ride has been designed for velocity. The 291 has 100mm of front and rear travel, a superlight aluminum frame and fast-rolling 29-inch wheels. Yippee!",
                condition = "used"
            },
            new
            {
                pickup_zone = "POLYGON((-122.4644 37.8199, -122.3544 37.8199, -122.3544 37.7099, -122.4644 37.7099, -122.4644 37.8199))",
                store_location = "-122.4194,37.7749",
                brand = "Noka Bikes",
                model = "Kahuna",
                price = 3200,
                description = "Whether you want to try your hand at XC racing or are looking for a lively trail bike that's just as inspiring on the climbs as it is over rougher ground, the Wilder is one heck of a bike built specifically for short women. Both the frames and components have been tweaked to include a women’s saddle, different bars and unique colourway.",
                condition = "used"
            },
            new
            {
                pickup_zone = "POLYGON((-0.1778 51.5524, 0.0822 51.5524, 0.0822 51.4024, -0.1778 51.4024, -0.1778 51.5524))",
                store_location = "-0.1278,51.5074",
                brand = "Breakout",
                model = "XBN 2.1 Alloy",
                price = 810,
                description = "The XBN 2.1 Alloy is our entry-level road bike – but that’s not to say that it’s a basic machine. With an internal weld aluminium frame, a full carbon fork, and the slick-shifting Claris gears from Shimano’s, this is a bike which doesn’t break the bank and delivers craved performance.",
                condition = "new"
            },
            new
            {
                pickup_zone = "POLYGON((2.1767 48.9016, 2.5267 48.9016, 2.5267 48.5516, 2.1767 48.5516, 2.1767 48.9016))",
                store_location = "2.3522,48.8566",
                brand = "ScramBikes",
                model = "WattBike",
                price = 2300,
                description = "The WattBike is the best e-bike for people who still feel young at heart. It has a Bafang 1000W mid-drive system and a 48V 17.5AH Samsung Lithium-Ion battery, allowing you to ride for more than 60 miles on one charge. It’s great for tackling hilly terrain or if you just fancy a more leisurely ride. With three working modes, you can choose between E-bike, assisted bicycle, and normal bike modes.",
                condition = "new"
            },
            new
            {
                pickup_zone = "POLYGON((13.3260 52.5700, 13.6550 52.5700, 13.6550 52.2700, 13.3260 52.2700, 13.3260 52.5700))",
                store_location = "13.4050,52.5200",
                brand = "Peaknetic",
                model = "Secto",
                price = 430,
                description = "If you struggle with stiff fingers or a kinked neck or back after a few minutes on the road, this lightweight, aluminum bike alleviates those issues and allows you to enjoy the ride. From the ergonomic grips to the lumbar-supporting seat position, the Roll Low-Entry offers incredible comfort. The rear-inclined seat tube facilitates stability by allowing you to put a foot on the ground to balance at a stop, and the low step-over frame makes it accessible for all ability and mobility levels. The saddle is very soft, with a wide back to support your hip joints and a cutout in the center to redistribute that pressure. Rim brakes deliver satisfactory braking control, and the wide tires provide a smooth, stable ride on paved roads and gravel. Rack and fender mounts facilitate setting up the Roll Low-Entry as your preferred commuter, and the BMX-like handlebar offers space for mounting a flashlight, bell, or phone holder.",
                condition = "new"
            },
            new
            {
                pickup_zone = "POLYGON((1.9450 41.4301, 2.4018 41.4301, 2.4018 41.1987, 1.9450 41.1987, 1.9450 41.4301))",
                store_location = "2.1734, 41.3851",
                brand = "nHill",
                model = "Summit",
                price = 1200,
                description = "This budget mountain bike from nHill performs well both on bike paths and on the trail. The fork with 100mm of travel absorbs rough terrain. Fat Kenda Booster tires give you grip in corners and on wet trails. The Shimano Tourney drivetrain offered enough gears for finding a comfortable pace to ride uphill, and the Tektro hydraulic disc brakes break smoothly. Whether you want an affordable bike that you can take to work, but also take trail in mountains on the weekends or you’re just after a stable, comfortable ride for the bike path, the Summit gives a good value for money.",
                condition = "new"
            },
            new
            {
                pickup_zone = "POLYGON((12.4464 42.1028, 12.5464 42.1028, 12.5464 41.7028, 12.4464 41.7028, 12.4464 42.1028))",
                store_location = "12.4964,41.9028",
                model = "ThrillCycle",
                brand = "BikeShind",
                price = 815,
                description = "An artsy,  retro-inspired bicycle that’s as functional as it is pretty: The ThrillCycle steel frame offers a smooth ride. A 9-speed drivetrain has enough gears for coasting in the city, but we wouldn’t suggest taking it to the mountains. Fenders protect you from mud, and a rear basket lets you transport groceries, flowers and books. The ThrillCycle comes with a limited lifetime warranty, so this little guy will last you long past graduation.",
                condition = "refurbished"
            }
        };

        for (var i = 0; i < bicycles.Length; i++)
        {
            db.JSON().Set($"bicycle:{i}", "$", bicycles[i]);
        }
        // HIDE_END


        // STEP_START agg1
        AggregationResult res1 = db.FT().Aggregate(
            "idx:bicycle",
            new AggregationRequest("@condition:{new}")
                .Load(new FieldName("__key"), new FieldName("price"))
                .Apply("@price - (@price * 0.1)", "discounted")
        );
        Console.WriteLine(res1.TotalResults);   // >>> 5

        for (int i = 0; i < res1.TotalResults; i++)
        {
            Row res1Row = res1.GetRow(i);

            Console.WriteLine(
                $"Key: {res1Row["__key"]}, Price: {res1Row["price"]}, Discounted: {res1Row["discounted"]}"
            );
        }
        // >>> Key: bicycle:0, Price: 270, Discounted: 243
        // >>> Key: bicycle:5, Price: 810, Discounted: 729
        // >>> Key: bicycle:6, Price: 2300, Discounted: 2070
        // >>> Key: bicycle:7, Price: 430, Discounted: 387
        // >>> Key: bicycle:8, Price: 1200, Discounted: 1080
        // STEP_END

        // Tests for 'agg1' step.
        // REMOVE_START
        Assert.Equal(5, res1.TotalResults);

        for (int i = 0; i < 5; i++)
        {
            Row test1Row = res1.GetRow(i);

            switch (test1Row["__key"])
            {
                case "bicycle:0":
                    Assert.Equal(
                        "Key: bicycle:0, Price: 270, Discounted: 243",
                        $"Key: {test1Row["__key"]}, Price: {test1Row["price"]}, Discounted: {test1Row["discounted"]}"
                    );
                    break;

                case "bicycle:5":
                    Assert.Equal(
                        "Key: bicycle:5, Price: 810, Discounted: 729",
                        $"Key: {test1Row["__key"]}, Price: {test1Row["price"]}, Discounted: {test1Row["discounted"]}"
                    );
                    break;

                case "bicycle:6":
                    Assert.Equal(
                        "Key: bicycle:6, Price: 2300, Discounted: 2070",
                        $"Key: {test1Row["__key"]}, Price: {test1Row["price"]}, Discounted: {test1Row["discounted"]}"
                    );
                    break;

                case "bicycle:7":
                    Assert.Equal(
                        "Key: bicycle:7, Price: 430, Discounted: 387",
                        $"Key: {test1Row["__key"]}, Price: {test1Row["price"]}, Discounted: {test1Row["discounted"]}"
                    );
                    break;

                case "bicycle:8":
                    Assert.Equal(
                        "Key: bicycle:8, Price: 1200, Discounted: 1080",
                        $"Key: {test1Row["__key"]}, Price: {test1Row["price"]}, Discounted: {test1Row["discounted"]}"
                    );
                    break;
            }
        }

        // REMOVE_END


        // STEP_START agg2
        AggregationResult res2 = db.FT().Aggregate(
            "idx:bicycle",
            new AggregationRequest("*")
                .Load(new FieldName("price"))
                .Apply("@price<1000", "price_category")
                .GroupBy(
                    "@condition",
                    Reducers.Sum("@price_category").As("num_affordable")
                )
        );
        Console.WriteLine(res2.TotalResults);   // >>> 3

        for (int i = 0; i < res2.TotalResults; i++)
        {
            Row res2Row = res2.GetRow(i);

            Console.WriteLine(
                $"Condition: {res2Row["condition"]}, Num affordable: {res2Row["num_affordable"]}"
            );
        }
        // >>> Condition: refurbished, Num affordable: 1
        // >>> Condition: used, Num affordable: 1
        // >>> Condition: new, Num affordable: 3
        // STEP_END

        // Tests for 'agg2' step.
        // REMOVE_START
        Assert.Equal(3, res2.TotalResults);

        for (int i = 0; i < 3; i++)
        {
            Row test2Row = res2.GetRow(i);
            switch (test2Row["condition"])
            {
                case "refurbished":
                    Assert.Equal(
                        "Condition: refurbished, Num affordable: 1",
                        $"Condition: {test2Row["condition"]}, Num affordable: {test2Row["num_affordable"]}"
                    );
                    break;

                case "used":
                    Assert.Equal(
                        "Condition: used, Num affordable: 1",
                        $"Condition: {test2Row["condition"]}, Num affordable: {test2Row["num_affordable"]}"
                    );
                    break;

                case "new":
                    Assert.Equal(
                        "Condition: new, Num affordable: 3",
                        $"Condition: {test2Row["condition"]}, Num affordable: {test2Row["num_affordable"]}"
                    );
                    break;
            }
        }
        // REMOVE_END


        // STEP_START agg3
        AggregationResult res3 = db.FT().Aggregate(
            "idx:bicycle",
            new AggregationRequest("*")
                .Apply("'bicycle'", "type")
                .GroupBy("@type", Reducers.Count().As("num_total"))
        );
        Console.WriteLine(res3.TotalResults);   // >>> 1

        Row res3Row = res3.GetRow(0);
        Console.WriteLine($"Type: {res3Row["type"]}, Num total: {res3Row["num_total"]}");
        // >>> Type: bicycle, Num total: 10
        // STEP_END

        // Tests for 'agg3' step.
        // REMOVE_START
        Assert.Equal(1, res3.TotalResults);

        Assert.Equal(
            "Type: bicycle, Num total: 10",
            $"Type: {res3Row["type"]}, Num total: {res3Row["num_total"]}"
        );
        // REMOVE_END


        // STEP_START agg4

        // Not supported in NRedisStack.

        // STEP_END

        // Tests for 'agg4' step.
        // REMOVE_START

        // REMOVE_END


        // HIDE_START
    }
}
// HIDE_END

