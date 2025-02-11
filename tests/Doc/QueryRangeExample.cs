// EXAMPLE: query_range
// HIDE_START

using NRedisStack.RedisStackCommands;
using NRedisStack.Search;
using NRedisStack.Search.Literals.Enums;
using NRedisStack.Tests;
using StackExchange.Redis;

// HIDE_END

// REMOVE_START
namespace Doc;
[Collection("DocsTests")]
// REMOVE_END

// HIDE_START
public class QueryRangeExample
// REMOVE_START
: AbstractNRedisStackTest, IDisposable
// REMOVE_END
{
    // REMOVE_START
    public QueryRangeExample(EndpointsFixture fixture) : base(fixture) { }

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
        try { db.FT().DropIndex("idx:bicycle", dd: true); } catch { }
        //REMOVE_END

        Schema bikeSchema = new Schema()
            .AddTextField(new FieldName("$.description", "description"))
            .AddNumericField(new FieldName("$.price", "price"))
            .AddTagField(new FieldName("$.condition", "condition"));

        FTCreateParams bikeParams = new FTCreateParams()
            .AddPrefix("bicycle:")
            .On(IndexDataType.JSON);

        db.FT().Create("idx:bicycle", bikeParams, bikeSchema);

        var bicycles = new object[]
        {
            new
            {
                brand = "Velorim",
                model = "Jigger",
                price = 270,
                description = "Small and powerful, the Jigger is the best ride " +
                                "for the smallest of tikes! This is the tiniest " +
                                "kids’ pedal bike on the market available without" +
                                " a coaster brake, the Jigger is the vehicle of " +
                                "choice for the rare tenacious little rider " +
                                "raring to go.",
                condition = "used"
            },
            new
            {
                brand = "Bicyk",
                model = "Hillcraft",
                price = 1200,
                description = "Kids want to ride with as little weight as possible." +
                    " Especially on an incline! They may be at the age " +
                    "when a 27.5 inch wheel bike is just too clumsy coming " +
                    "off a 24 inch bike. The Hillcraft 26 is just the solution" +
                    " they need!",
                condition = "used",
            },
            new
            {
                brand = "Nord",
                model = "Chook air 5",
                price = 815,
                description = "The Chook Air 5  gives kids aged six years and older " +
                    "a durable and uberlight mountain bike for their first" +
                    " experience on tracks and easy cruising through forests" +
                    " and fields. The lower  top tube makes it easy to mount" +
                    " and dismount in any situation, giving your kids greater" +
                    " safety on the trails.",
                condition = "used",
            },
            new
            {
                brand = "Eva",
                model = "Eva 291",
                price = 3400,
                description = "The sister company to Nord, Eva launched in 2005 as the" +
                    " first and only women-dedicated bicycle brand. Designed" +
                    " by women for women, allEva bikes are optimized for the" +
                    " feminine physique using analytics from a body metrics" +
                    " database. If you like 29ers, try the Eva 291. It’s a " +
                    "brand new bike for 2022.. This full-suspension, " +
                    "cross-country ride has been designed for velocity. The" +
                    " 291 has 100mm of front and rear travel, a superlight " +
                    "aluminum frame and fast-rolling 29-inch wheels. Yippee!",
                condition = "used",
            },
            new
            {
                brand = "Noka Bikes",
                model = "Kahuna",
                price = 3200,
                description = "Whether you want to try your hand at XC racing or are " +
                    "looking for a lively trail bike that's just as inspiring" +
                    " on the climbs as it is over rougher ground, the Wilder" +
                    " is one heck of a bike built specifically for short women." +
                    " Both the frames and components have been tweaked to " +
                    "include a women’s saddle, different bars and unique " +
                    "colourway.",
                condition = "used",
            },
            new
            {
                brand = "Breakout",
                model = "XBN 2.1 Alloy",
                price = 810,
                description = "The XBN 2.1 Alloy is our entry-level road bike – but that’s" +
                    " not to say that it’s a basic machine. With an internal " +
                    "weld aluminium frame, a full carbon fork, and the slick-shifting" +
                    " Claris gears from Shimano’s, this is a bike which doesn’t" +
                    " break the bank and delivers craved performance.",
                condition = "new",
            },
            new
            {
                brand = "ScramBikes",
                model = "WattBike",
                price = 2300,
                description = "The WattBike is the best e-bike for people who still feel young" +
                    " at heart. It has a Bafang 1000W mid-drive system and a 48V" +
                    " 17.5AH Samsung Lithium-Ion battery, allowing you to ride for" +
                    " more than 60 miles on one charge. It’s great for tackling hilly" +
                    " terrain or if you just fancy a more leisurely ride. With three" +
                    " working modes, you can choose between E-bike, assisted bicycle," +
                    " and normal bike modes.",
                condition = "new",
            },
            new
            {
                brand = "Peaknetic",
                model = "Secto",
                price = 430,
                description = "If you struggle with stiff fingers or a kinked neck or back after" +
                    " a few minutes on the road, this lightweight, aluminum bike" +
                    " alleviates those issues and allows you to enjoy the ride. From" +
                    " the ergonomic grips to the lumbar-supporting seat position, the" +
                    " Roll Low-Entry offers incredible comfort. The rear-inclined seat" +
                    " tube facilitates stability by allowing you to put a foot on the" +
                    " ground to balance at a stop, and the low step-over frame makes it" +
                    " accessible for all ability and mobility levels. The saddle is" +
                    " very soft, with a wide back to support your hip joints and a" +
                    " cutout in the center to redistribute that pressure. Rim brakes" +
                    " deliver satisfactory braking control, and the wide tires provide" +
                    " a smooth, stable ride on paved roads and gravel. Rack and fender" +
                    " mounts facilitate setting up the Roll Low-Entry as your preferred" +
                    " commuter, and the BMX-like handlebar offers space for mounting a" +
                    " flashlight, bell, or phone holder.",
                condition = "new",
            },
            new
            {
                brand = "nHill",
                model = "Summit",
                price = 1200,
                description = "This budget mountain bike from nHill performs well both on bike" +
                    " paths and on the trail. The fork with 100mm of travel absorbs" +
                    " rough terrain. Fat Kenda Booster tires give you grip in corners" +
                    " and on wet trails. The Shimano Tourney drivetrain offered enough" +
                    " gears for finding a comfortable pace to ride uphill, and the" +
                    " Tektro hydraulic disc brakes break smoothly. Whether you want an" +
                    " affordable bike that you can take to work, but also take trail in" +
                    " mountains on the weekends or you’re just after a stable," +
                    " comfortable ride for the bike path, the Summit gives a good value" +
                    " for money.",
                condition = "new",
            },
            new
            {
                model = "ThrillCycle",
                brand = "BikeShind",
                price = 815,
                description = "An artsy,  retro-inspired bicycle that’s as functional as it is" +
                    " pretty: The ThrillCycle steel frame offers a smooth ride. A" +
                    " 9-speed drivetrain has enough gears for coasting in the city, but" +
                    " we wouldn’t suggest taking it to the mountains. Fenders protect" +
                    " you from mud, and a rear basket lets you transport groceries," +
                    " flowers and books. The ThrillCycle comes with a limited lifetime" +
                    " warranty, so this little guy will last you long past graduation.",
                condition = "refurbished",
            },
        };

        for (var i = 0; i < bicycles.Length; i++)
        {
            db.JSON().Set($"bicycle:{i}", "$", bicycles[i]);
        }
        // HIDE_END


        // STEP_START range1
        SearchResult res1 = db.FT().Search(
            "idx:bicycle",
            new Query("@price:[500 1000]")
        );
        Console.WriteLine(res1.TotalResults);   // >>> 3
        // STEP_END

        // Tests for 'range1' step.
        // REMOVE_START
        Assert.Equal(3, res1.TotalResults);
        // REMOVE_END


        // STEP_START range2
        SearchResult res2 = db.FT().Search(
            "idx:bicycle",
            new Query().AddFilter(
                new Query.NumericFilter("price", 500, 1000)
            )
        );
        Console.WriteLine(res2.TotalResults);   // >>> 3
        // STEP_END

        // Tests for 'range2' step.
        // REMOVE_START
        Assert.Equal(3, res2.TotalResults);
        // REMOVE_END


        // STEP_START range3
        SearchResult res3 = db.FT().Search(
            "idx:bicycle",
            new Query("*").AddFilter(new Query.NumericFilter(
                    "price", 1000, true, Double.PositiveInfinity, false
                )
            )
        );
        Console.WriteLine(res3.TotalResults);   // >>> 5
        // STEP_END

        // Tests for 'range3' step.
        // REMOVE_START
        Assert.Equal(5, res3.TotalResults);
        // REMOVE_END


        // STEP_START range4
        SearchResult res4 = db.FT().Search(
            "idx:bicycle",
            new Query("@price:[-inf 2000]")
                .SetSortBy("price")
                .Limit(0, 5)
        );
        Console.WriteLine(res4.TotalResults);   // >>> 7
        Console.WriteLine($"Prices: {string.Join(", ", res4.Documents.Select(d => d["price"]))}");
        // >>> Prices: 270, 430, 810, 815, 815
        // STEP_END

        // Tests for 'range4' step.
        // REMOVE_START
        Assert.Equal(7, res4.TotalResults);
        Assert.Equal(
            "Prices: 270, 430, 810, 815, 815",
            $"Prices: {string.Join(", ", res4.Documents.Select(d => d["price"]))}"
        );
        // REMOVE_END


        // HIDE_START
    }
}
// HIDE_END

