// EXAMPLE: search_quickstart

using NRedisStack.RedisStackCommands;
using NRedisStack.Search;
using NRedisStack.Search.Aggregation;
using NRedisStack.Search.Literals.Enums;
using NRedisStack.Tests;
using StackExchange.Redis;

// REMOVE_START
namespace Doc;
[Collection("DocsTests")]
// REMOVE_END
public class SearchQuickstartExample
{
    private readonly ITestOutputHelper testOutputHelper;

    public SearchQuickstartExample(ITestOutputHelper testOutputHelper)
    {
        this.testOutputHelper = testOutputHelper;
    }

    [SkipIfRedis(Is.OSSCluster)]
    public void run()
    {
        // STEP_START connect
        var redis = ConnectionMultiplexer.Connect("localhost:6379");
        var db = redis.GetDatabase();
        var ft = db.FT();
        var json = db.JSON();
        // STEP_END

        // REMOVE_START
        try
        {
            ft.DropIndex("idx:bicycle");
        }
        catch
        {
            // ignored
        }
        // REMOVE_END

        // STEP_START data_sample
        var bike1 = new
        {
            Brand = "Velorim",
            Model = "Jigger",
            Price = 270M,
            Description = "Small and powerful, the Jigger is the best ride " +
                            "for the smallest of tikes! This is the tiniest " +
                            "kids’ pedal bike on the market available without" +
                            " a coaster brake, the Jigger is the vehicle of " +
                            "choice for the rare tenacious little rider " +
                            "raring to go.",
            Condition = "used"
        };
        // STEP_END

        var bicycles = new object[]
            {
                bike1,
                new
                {
                    Brand = "Bicyk",
                    Model = "Hillcraft",
                    Price = 1200M,
                    Description = "Kids want to ride with as little weight as possible." +
                        " Especially on an incline! They may be at the age " +
                        "when a 27.5 inch wheel bike is just too clumsy coming " +
                        "off a 24 inch bike. The Hillcraft 26 is just the solution" +
                        " they need!",
                    Condition = "used",
                },
                new
                {
                    Brand = "Nord",
                    Model = "Chook air 5",
                    Price = 815M,
                    Description = "The Chook Air 5  gives kids aged six years and older " +
                        "a durable and uberlight mountain bike for their first" +
                        " experience on tracks and easy cruising through forests" +
                        " and fields. The lower  top tube makes it easy to mount" +
                        " and dismount in any situation, giving your kids greater" +
                        " safety on the trails.",
                    Condition = "used",
                },
                new
                {
                    Brand = "Eva",
                    Model = "Eva 291",
                    Price = 3400M,
                    Description = "The sister company to Nord, Eva launched in 2005 as the" +
                        " first and only women-dedicated bicycle brand. Designed" +
                        " by women for women, allEva bikes are optimized for the" +
                        " feminine physique using analytics from a body metrics" +
                        " database. If you like 29ers, try the Eva 291. It’s a " +
                        "brand new bike for 2022.. This full-suspension, " +
                        "cross-country ride has been designed for velocity. The" +
                        " 291 has 100mm of front and rear travel, a superlight " +
                        "aluminum frame and fast-rolling 29-inch wheels. Yippee!",
                    Condition = "used",
                },
                new
                {
                    Brand = "Noka Bikes",
                    Model = "Kahuna",
                    Price = 3200M,
                    Description = "Whether you want to try your hand at XC racing or are " +
                        "looking for a lively trail bike that's just as inspiring" +
                        " on the climbs as it is over rougher ground, the Wilder" +
                        " is one heck of a bike built specifically for short women." +
                        " Both the frames and components have been tweaked to " +
                        "include a women’s saddle, different bars and unique " +
                        "colourway.",
                    Condition = "used",
                },
                new
                {
                    Brand = "Breakout",
                    Model = "XBN 2.1 Alloy",
                    Price = 810M,
                    Description = "The XBN 2.1 Alloy is our entry-level road bike – but that’s" +
                        " not to say that it’s a basic machine. With an internal " +
                        "weld aluminium frame, a full carbon fork, and the slick-shifting" +
                        " Claris gears from Shimano’s, this is a bike which doesn’t" +
                        " break the bank and delivers craved performance.",
                    Condition = "new",
                },
                new
                {
                    Brand = "ScramBikes",
                    Model = "WattBike",
                    Price = 2300M,
                    Description = "The WattBike is the best e-bike for people who still feel young" +
                        " at heart. It has a Bafang 1000W mid-drive system and a 48V" +
                        " 17.5AH Samsung Lithium-Ion battery, allowing you to ride for" +
                        " more than 60 miles on one charge. It’s great for tackling hilly" +
                        " terrain or if you just fancy a more leisurely ride. With three" +
                        " working modes, you can choose between E-bike, assisted bicycle," +
                        " and normal bike modes.",
                    Condition = "new",
                },
                new
                {
                    Brand = "Peaknetic",
                    Model = "Secto",
                    Price = 430M,
                    Description = "If you struggle with stiff fingers or a kinked neck or back after" +
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
                    Condition = "new",
                },
                new
                {
                    Brand = "nHill",
                    Model = "Summit",
                    Price = 1200M,
                    Description = "This budget mountain bike from nHill performs well both on bike" +
                        " paths and on the trail. The fork with 100mm of travel absorbs" +
                        " rough terrain. Fat Kenda Booster tires give you grip in corners" +
                        " and on wet trails. The Shimano Tourney drivetrain offered enough" +
                        " gears for finding a comfortable pace to ride uphill, and the" +
                        " Tektro hydraulic disc brakes break smoothly. Whether you want an" +
                        " affordable bike that you can take to work, but also take trail in" +
                        " mountains on the weekends or you’re just after a stable," +
                        " comfortable ride for the bike path, the Summit gives a good value" +
                        " for money.",
                    Condition = "new",
                },
                new
                {
                    Model = "ThrillCycle",
                    Brand = "BikeShind",
                    Price = 815M,
                    Description = "An artsy,  retro-inspired bicycle that’s as functional as it is" +
                        " pretty: The ThrillCycle steel frame offers a smooth ride. A" +
                        " 9-speed drivetrain has enough gears for coasting in the city, but" +
                        " we wouldn’t suggest taking it to the mountains. Fenders protect" +
                        " you from mud, and a rear basket lets you transport groceries," +
                        " flowers and books. The ThrillCycle comes with a limited lifetime" +
                        " warranty, so this little guy will last you long past graduation.",
                    Condition = "refurbished",
                },
            };

        // STEP_START create_index
        var schema = new Schema()
            .AddTextField(new FieldName("$.Brand", "Brand"))
            .AddTextField(new FieldName("$.Model", "Model"))
            .AddTextField(new FieldName("$.Description", "Description"))
            .AddNumericField(new FieldName("$.Price", "Price"))
            .AddTagField(new FieldName("$.Condition", "Condition"));

        ft.Create(
            "idx:bicycle",
            new FTCreateParams().On(IndexDataType.JSON).Prefix("bicycle:"),
            schema);
        // STEP_END

        // STEP_START add_documents
        for (int i = 0; i < bicycles.Length; i++)
        {
            json.Set($"bicycle:{i}", "$", bicycles[i]);
        }
        // STEP_END

        // STEP_START wildcard_query
        var query1 = new Query("*");
        var res1 = ft.Search("idx:bicycle", query1).Documents;
        testOutputHelper.WriteLine(string.Join("\n", res1.Count()));
        // Prints: Documents found: 10
        // STEP_END
        // REMOVE_START
        Assert.Equal(10, res1.Count());
        // REMOVE_END

        // STEP_START query_single_term
        var query2 = new Query("@Model:Jigger");
        var res2 = ft.Search("idx:bicycle", query2).Documents;
        testOutputHelper.WriteLine(string.Join("\n", res2.Select(x => x["json"])));
        // Prints: {"Brand":"Moore PLC","Model":"Award Race","Price":3790.76,
        //          "Description":"This olive folding bike features a carbon frame
        //          and 27.5 inch wheels. This folding bike is perfect for compact
        //          storage and transportation.","Condition":"new"}
        // STEP_END
        // REMOVE_START
        Assert.Single(res2);
        Assert.Equal("bicycle:0", res2[0].Id);
        // REMOVE_END

        // STEP_START query_single_term_and_num_range
        var query3 = new Query("basic @Price:[500 1000]");
        var res3 = ft.Search("idx:bicycle", query3).Documents;
        testOutputHelper.WriteLine(string.Join("\n", res3.Select(x => x["json"])));
        // Prints: {"Brand":"Moore PLC","Model":"Award Race","Price":3790.76,
        //          "Description":"This olive folding bike features a carbon frame
        //          and 27.5 inch wheels. This folding bike is perfect for compact
        //          storage and transportation.","Condition":"new"}
        // STEP_END
        // REMOVE_START
        Assert.Single(res3);
        Assert.Equal("bicycle:5", res3[0].Id);
        // REMOVE_END

        // STEP_START query_exact_matching
        var query4 = new Query("@Brand:\"Noka Bikes\"");
        var res4 = ft.Search("idx:bicycle", query4).Documents;
        testOutputHelper.WriteLine(string.Join("\n", res4.Select(x => x["json"])));
        // Prints: {"Brand":"Moore PLC","Model":"Award Race","Price":3790.76,
        //          "Description":"This olive folding bike features a carbon frame
        //          and 27.5 inch wheels. This folding bike is perfect for compact
        //          storage and transportation.","Condition":"new"}
        // STEP_END
        // REMOVE_START
        Assert.Single(res4);
        Assert.Equal("bicycle:4", res4[0].Id);
        // REMOVE_END

        // STEP_START query_single_term_limit_fields
        var query5 = new Query("@Model:Jigger").ReturnFields("Price");
        var res5 = ft.Search("idx:bicycle", query5).Documents;
        testOutputHelper.WriteLine(res5.First()["Price"]);
        // Prints: 270
        // STEP_END
        // REMOVE_START
        Assert.Single(res5);
        Assert.Equal("bicycle:0", res5[0].Id);
        // REMOVE_END

        // STEP_START simple_aggregation
        var request = new AggregationRequest("*").GroupBy(
            "@Condition", Reducers.Count().As("Count"));
        var result = ft.Aggregate("idx:bicycle", request);

        for (var i = 0; i < result.TotalResults; i++)
        {
            var row = result.GetRow(i);
            testOutputHelper.WriteLine($"{row["Condition"]} - {row["Count"]}");
        }

        // Prints:
        // refurbished - 1
        // used - 5
        // new - 4
        // STEP_END
        // REMOVE_START
        Assert.Equal(3, result.TotalResults);
        // REMOVE_END
    }
}