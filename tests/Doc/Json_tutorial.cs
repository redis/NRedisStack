// EXAMPLE: json_tutorial
// HIDE_START

using NRedisStack;
using NRedisStack.RedisStackCommands;
using NRedisStack.Tests;
using StackExchange.Redis;

// HIDE_END

// REMOVE_START
namespace Doc;

[Collection("DocsTests")]
// REMOVE_END

// HIDE_START
public class JsonTutorial
// REMOVE_START
: AbstractNRedisStackTest, IDisposable
// REMOVE_END
{
    // REMOVE_START
    public JsonTutorial(EndpointsFixture fixture) : base(fixture) { }

    [Fact]
    // REMOVE_END
    public void Run()
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
        db.KeyDelete("bike");
        db.KeyDelete("crashes");
        db.KeyDelete("newbike");
        db.KeyDelete("riders");
        db.KeyDelete("bike:1");
        db.KeyDelete("bikes:inventory");
        //REMOVE_END
        // HIDE_END


        // STEP_START set_get
        bool res1 = db.JSON().Set("bike", "$", "\"Hyperion\"");
        Console.WriteLine(res1);    // >>> True

        RedisResult res2 = db.JSON().Get("bike", path: "$");
        Console.WriteLine(res2);    // >>> ["Hyperion"]

        JsonType[] res3 = db.JSON().Type("bike", "$");
        Console.WriteLine(string.Join(", ", res3)); // >>> STRING
        // STEP_END

        // Tests for 'set_get' step.
        // REMOVE_START
        Assert.True(res1);
        Assert.Equal("[\"Hyperion\"]", (string?)res2);
        Assert.Equal("STRING", string.Join(", ", res3));
        // REMOVE_END


        // STEP_START str
        long?[] res4 = db.JSON().StrLen("bike", "$");
        Console.Write(string.Join(", ", res4)); // >>> 8

        long?[] res5 = db.JSON().StrAppend("bike", " (Enduro bikes)");
        Console.WriteLine(string.Join(", ", res5)); // >>> 23

        RedisResult res6 = db.JSON().Get("bike", path: "$");
        Console.WriteLine(res6);    // >>> ["Hyperion (Enduro bikes)"]
        // STEP_END

        // Tests for 'str' step.
        // REMOVE_START
        Assert.Equal("8", string.Join(", ", res4));
        Assert.Equal("23", string.Join(", ", res5));
        Assert.Equal("[\"Hyperion (Enduro bikes)\"]", (string?)res6);
        // REMOVE_END


        // STEP_START num
        bool res7 = db.JSON().Set("crashes", "$", 0);
        Console.WriteLine(res7);    // >>> True

        double?[] res8 = db.JSON().NumIncrby("crashes", "$", 1);
        Console.WriteLine(res8);    // >>> 1

        double?[] res9 = db.JSON().NumIncrby("crashes", "$", 1.5);
        Console.WriteLine(res9);    // >>> 2.5

        double?[] res10 = db.JSON().NumIncrby("crashes", "$", -0.75);
        Console.WriteLine(res9);    // >>> 1.75
        // STEP_END

        // Tests for 'num' step.
        // REMOVE_START
        Assert.True(res7);
        Assert.Equal("1", string.Join(", ", res8));
        Assert.Equal("2.5", string.Join(", ", res9));
        Assert.Equal("1.75", string.Join(", ", res10));
        // REMOVE_END


        // STEP_START arr
        bool res11 = db.JSON().Set("newbike", "$", new object?[] { "Deimos", new { crashes = 0 }, null });
        Console.WriteLine(res11);   // >>> True

        RedisResult res12 = db.JSON().Get("newbike", path: "$");
        Console.WriteLine(res12);   //  >>> [["Deimos",{"crashes":0},null]]

        RedisResult res13 = db.JSON().Get("newbike", path: "$[1].crashes");
        Console.WriteLine(res13);   // >>> [0]

        long res14 = db.JSON().Del("newbike", "$.[-1]");
        Console.WriteLine(res14);   // >>> 1

        RedisResult res15 = db.JSON().Get("newbike", path: "$");
        Console.WriteLine(res15);   // >>> [["Deimos",{"crashes":0}]]
        // STEP_END

        // Tests for 'arr' step.
        // REMOVE_START
        Assert.True(res11);
        Assert.Equal("[[\"Deimos\",{\"crashes\":0},null]]", (string?)res12);
        Assert.Equal("[0]", (string?)res13);
        Assert.Equal(1, res14);
        Assert.Equal("[[\"Deimos\",{\"crashes\":0}]]", (string?)res15);
        // REMOVE_END


        // STEP_START arr2
        bool res16 = db.JSON().Set("riders", "$", new object[] { });
        Console.WriteLine(res16);   // >>> True

        long?[] res17 = db.JSON().ArrAppend("riders", "$", "Norem");
        Console.WriteLine(string.Join(", ", res17));    // >>> 1

        RedisResult res18 = db.JSON().Get("riders", path: "$");
        Console.WriteLine(res18);   // >>> [["Norem"]]

        long?[] res19 = db.JSON().ArrInsert("riders", "$", 1, "Prickett", "Royce", "Castilla");
        Console.WriteLine(string.Join(", ", res19));    // >>> 4

        RedisResult res20 = db.JSON().Get("riders", path: "$");
        Console.WriteLine(res20);   // >>> [["Norem","Prickett","Royce","Castilla"]]

        long?[] res21 = db.JSON().ArrTrim("riders", "$", 1, 1);
        Console.WriteLine(string.Join(", ", res21));    // 1

        RedisResult res22 = db.JSON().Get("riders", path: "$");
        Console.WriteLine(res22);   // >>> [["Prickett"]]

        RedisResult[] res23 = db.JSON().ArrPop("riders", "$");
        Console.WriteLine(string.Join(", ", (object[])res23)); // >>> "Prickett"

        RedisResult[] res24 = db.JSON().ArrPop("riders", "$");
        Console.WriteLine(string.Join(", ", (object[])res24)); // >>> <Empty string>
        // STEP_END

        // Tests for 'arr2' step.
        // REMOVE_START
        Assert.True(res16);
        Assert.Equal("1", string.Join(", ", res17));
        Assert.Equal("[[\"Norem\"]]", (string?)res18);
        Assert.Equal("4", string.Join(", ", res19));
        Assert.Equal("[[\"Norem\",\"Prickett\",\"Royce\",\"Castilla\"]]", (string?)res20);
        Assert.Equal("1", string.Join(", ", res21));
        Assert.Equal("[[\"Prickett\"]]", (string?)res22);
        Assert.Equal("\"Prickett\"", string.Join(", ", (object[])res23));
        Assert.Equal("", string.Join(", ", (object[])res24));
        // REMOVE_END


        // STEP_START obj
        bool res25 = db.JSON().Set("bike:1", "$",
            new { model = "Deimos", brand = "Ergonom", price = 4972 }
        );
        Console.WriteLine(res25);   // >>> True

        long?[] res26 = db.JSON().ObjLen("bike:1", "$");
        Console.WriteLine(string.Join(", ", res26));    // >>> 3

        IEnumerable<HashSet<string>> res27 = db.JSON().ObjKeys("bike:1", "$");
        Console.WriteLine(
            string.Join(", ", res27.Select(b => $"{string.Join(", ", b.Select(c => $"{c}"))}"))
        ); // >>> model, brand, price
        // STEP_END

        // Tests for 'obj' step.
        // REMOVE_START
        Assert.True(res25);
        Assert.Equal("3", string.Join(", ", res26));
        Assert.Equal("model, brand, price", string.Join(", ", res27.Select(b => $"{string.Join(", ", b.Select(c => $"{c}"))}")));
        // REMOVE_END


        // STEP_START set_bikes
        string inventoryJson = @"
{
    ""inventory"": {
        ""mountain_bikes"": [
            {
                ""id"": ""bike:1"",
                ""model"": ""Phoebe"",
                ""description"": ""This is a mid-travel trail slayer that is a fantastic daily driver or one bike quiver. The Shimano Claris 8-speed groupset gives plenty of gear range to tackle hills and there\u2019s room for mudguards and a rack too.  This is the bike for the rider who wants trail manners with low fuss ownership."",
                ""price"": 1920,
                ""specs"": {""material"": ""carbon"", ""weight"": 13.1},
                ""colors"": [""black"", ""silver""]
            },
            {
                ""id"": ""bike:2"",
                ""model"": ""Quaoar"",
                ""description"": ""Redesigned for the 2020 model year, this bike impressed our testers and is the best all-around trail bike we've ever tested. The Shimano gear system effectively does away with an external cassette, so is super low maintenance in terms of wear and tear. All in all it's an impressive package for the price, making it very competitive."",
                ""price"": 2072,
                ""specs"": {""material"": ""aluminium"", ""weight"": 7.9},
                ""colors"": [""black"", ""white""]
            },
            {
                ""id"": ""bike:3"",
                ""model"": ""Weywot"",
                ""description"": ""This bike gives kids aged six years and older a durable and uberlight mountain bike for their first experience on tracks and easy cruising through forests and fields. A set of powerful Shimano hydraulic disc brakes provide ample stopping ability. If you're after a budget option, this is one of the best bikes you could get."",
                ""price"": 3264,
                ""specs"": {""material"": ""alloy"", ""weight"": 13.8}
            }
        ],
        ""commuter_bikes"": [
            {
                ""id"": ""bike:4"",
                ""model"": ""Salacia"",
                ""description"": ""This bike is a great option for anyone who just wants a bike to get about on With a slick-shifting Claris gears from Shimano\u2019s, this is a bike which doesn\u2019t break the bank and delivers craved performance.  It\u2019s for the rider who wants both efficiency and capability."",
                ""price"": 1475,
                ""specs"": {""material"": ""aluminium"", ""weight"": 16.6},
                ""colors"": [""black"", ""silver""]
            },
            {
                ""id"": ""bike:5"",
                ""model"": ""Mimas"",
                ""description"": ""A real joy to ride, this bike got very high scores in last years Bike of the year report. The carefully crafted 50-34 tooth chainset and 11-32 tooth cassette give an easy-on-the-legs bottom gear for climbing, and the high-quality Vittoria Zaffiro tires give balance and grip.It includes a low-step frame , our memory foam seat, bump-resistant shocks and conveniently placed thumb throttle. Put it all together and you get a bike that helps redefine what can be done for this price."",
                ""price"": 3941,
                ""specs"": {""material"": ""alloy"", ""weight"": 11.6}
            }
        ]
    }
}";

        bool res28 = db.JSON().Set("bikes:inventory", "$", inventoryJson);
        Console.WriteLine(res28);   // >>> True
        // STEP_END

        // Tests for 'set_bikes' step.
        // REMOVE_START
        Assert.True(res28);
        // REMOVE_END


        // STEP_START get_bikes
        RedisResult res29 = db.JSON().Get("bikes:inventory", path: "$.inventory.*");
        Console.WriteLine(res29);   // >>> {[[{"id":"bike:1","model":"Phoebe", ...
        // STEP_END

        // Tests for 'get_bikes' step.
        // REMOVE_START
        Assert.Equal(@"[[{""id"":""bike:1"",""model"":""Phoebe"",""description"":""This is a mid-travel trail slayer that is a fantastic daily driver or one bike quiver. The Shimano Claris 8-speed groupset gives plenty of gear range to tackle hills and there’s room for mudguards and a rack too.  This is the bike for the rider who wants trail manners with low fuss ownership."",""price"":1920,""specs"":{""material"":""carbon"",""weight"":13.1},""colors"":[""black"",""silver""]},{""id"":""bike:2"",""model"":""Quaoar"",""description"":""Redesigned for the 2020 model year, this bike impressed our testers and is the best all-around trail bike we've ever tested. The Shimano gear system effectively does away with an external cassette, so is super low maintenance in terms of wear and tear. All in all it's an impressive package for the price, making it very competitive."",""price"":2072,""specs"":{""material"":""aluminium"",""weight"":7.9},""colors"":[""black"",""white""]},{""id"":""bike:3"",""model"":""Weywot"",""description"":""This bike gives kids aged six years and older a durable and uberlight mountain bike for their first experience on tracks and easy cruising through forests and fields. A set of powerful Shimano hydraulic disc brakes provide ample stopping ability. If you're after a budget option, this is one of the best bikes you could get."",""price"":3264,""specs"":{""material"":""alloy"",""weight"":13.8}}],[{""id"":""bike:4"",""model"":""Salacia"",""description"":""This bike is a great option for anyone who just wants a bike to get about on With a slick-shifting Claris gears from Shimano’s, this is a bike which doesn’t break the bank and delivers craved performance.  It’s for the rider who wants both efficiency and capability."",""price"":1475,""specs"":{""material"":""aluminium"",""weight"":16.6},""colors"":[""black"",""silver""]},{""id"":""bike:5"",""model"":""Mimas"",""description"":""A real joy to ride, this bike got very high scores in last years Bike of the year report. The carefully crafted 50-34 tooth chainset and 11-32 tooth cassette give an easy-on-the-legs bottom gear for climbing, and the high-quality Vittoria Zaffiro tires give balance and grip.It includes a low-step frame , our memory foam seat, bump-resistant shocks and conveniently placed thumb throttle. Put it all together and you get a bike that helps redefine what can be done for this price."",""price"":3941,""specs"":{""material"":""alloy"",""weight"":11.6}}]]",
            (string?)res29
        );
        // REMOVE_END


        // STEP_START get_mtnbikes
        RedisResult res30 = db.JSON().Get("bikes:inventory", path: "$.inventory.mountain_bikes[*].model");
        Console.WriteLine(res30);   // >>> ["Phoebe","Quaoar","Weywot"]

        RedisResult res31 = db.JSON().Get("bikes:inventory", path: "$.inventory[\"mountain_bikes\"][*].model");
        Console.WriteLine(res31);   // >>> ["Phoebe","Quaoar","Weywot"]

        RedisResult res32 = db.JSON().Get("bikes:inventory", path: "$..mountain_bikes[*].model");
        Console.WriteLine(res32);   // >>> ["Phoebe","Quaoar","Weywot"]
        // STEP_END

        // Tests for 'get_mtnbikes' step.
        // REMOVE_START
        Assert.Equal("[\"Phoebe\",\"Quaoar\",\"Weywot\"]", (string?)res30);
        Assert.Equal("[\"Phoebe\",\"Quaoar\",\"Weywot\"]", (string?)res31);
        Assert.Equal("[\"Phoebe\",\"Quaoar\",\"Weywot\"]", (string?)res32);
        // REMOVE_END


        // STEP_START get_models
        RedisResult res33 = db.JSON().Get("bikes:inventory", path: "$..model");
        Console.WriteLine(res33);   // >>> ["Phoebe","Quaoar","Weywot","Salacia","Mimas"]
        // STEP_END

        // Tests for 'get_models' step.
        // REMOVE_START
        Assert.Equal("[\"Phoebe\",\"Quaoar\",\"Weywot\",\"Salacia\",\"Mimas\"]", (string?)res33);
        // REMOVE_END


        // STEP_START get2mtnbikes
        RedisResult res34 = db.JSON().Get("bikes:inventory", path: "$..mountain_bikes[0:2].model");
        Console.WriteLine(res34);   // >>> ["Phoebe","Quaoar"]
        // STEP_END

        // Tests for 'get2mtnbikes' step.
        // REMOVE_START
        Assert.Equal("[\"Phoebe\",\"Quaoar\"]", (string?)res34);
        // REMOVE_END


        // STEP_START filter1
        RedisResult res35 = db.JSON().Get(
            "bikes:inventory",
            path: "$..mountain_bikes[?(@.price < 3000 && @.specs.weight < 10)]"
        );
        Console.WriteLine(res35);
        // >>> [{"id":"bike:2","model":"Quaoar","description":"Redesigned for the 2020 model year...
        // STEP_END

        // Tests for 'filter1' step.
        // REMOVE_START
        Assert.Equal(@"[{""id"":""bike:2"",""model"":""Quaoar"",""description"":""Redesigned for the 2020 model year, this bike impressed our testers and is the best all-around trail bike we've ever tested. The Shimano gear system effectively does away with an external cassette, so is super low maintenance in terms of wear and tear. All in all it's an impressive package for the price, making it very competitive."",""price"":2072,""specs"":{""material"":""aluminium"",""weight"":7.9},""colors"":[""black"",""white""]}]",
            (string?)res35
        );
        // REMOVE_END


        // STEP_START filter2
        RedisResult res36 = db.JSON().Get(
            "bikes:inventory",
            path: "$..[?(@.specs.material == 'alloy')].model"
        );
        Console.WriteLine(res36);   // >>> ["Weywot","Mimas"]
        // STEP_END

        // Tests for 'filter2' step.
        // REMOVE_START
        Assert.Equal("[\"Weywot\",\"Mimas\"]", (string?)res36);
        // REMOVE_END


        // STEP_START filter3
        RedisResult res37 = db.JSON().Get(
            "bikes:inventory",
            path: "$..[?(@.specs.material =~ '(?i)al')].model"
        );
        Console.WriteLine(res37);   // >>> ["Quaoar","Weywot","Salacia","Mimas"]
        // STEP_END

        // Tests for 'filter3' step.
        // REMOVE_START
        Assert.Equal("[\"Quaoar\",\"Weywot\",\"Salacia\",\"Mimas\"]", (string?)res37);
        // REMOVE_END


        // STEP_START filter4
        bool res38 = db.JSON().Set(
            "bikes:inventory",
            "$.inventory.mountain_bikes[0].regex_pat",
            "\"(?i)al\""
        );
        Console.WriteLine(res38);   // >>> True

        bool res39 = db.JSON().Set(
            "bikes:inventory",
            "$.inventory.mountain_bikes[1].regex_pat",
            "\"(?i)al\""
        );
        Console.WriteLine(res39);   // >>> True

        bool res40 = db.JSON().Set(
            "bikes:inventory",
            "$.inventory.mountain_bikes[2].regex_pat",
            "\"(?i)al\""
        );
        Console.WriteLine(res40);   // >>> True

        RedisResult res41 = db.JSON().Get(
            "bikes:inventory",
            path: "$.inventory.mountain_bikes[?(@.specs.material =~ @.regex_pat)].model"
        );
        Console.WriteLine(res41);   // >>> ["Quaoar","Weywot"]
        // STEP_END

        // Tests for 'filter4' step.
        // REMOVE_START
        Assert.Equal("[\"Quaoar\",\"Weywot\"]", (string?)res41);
        // REMOVE_END


        // STEP_START update_bikes
        RedisResult res42 = db.JSON().Get("bikes:inventory", path: "$..price");
        Console.WriteLine(res42);   // >>> [1920,2072,3264,1475,3941]

        double?[] res43 = db.JSON().NumIncrby("bikes:inventory", "$..price", -100);
        Console.WriteLine(string.Join(", ", res43));    // >>> 1820, 1972, 3164, 1375, 3841

        double?[] res44 = db.JSON().NumIncrby("bikes:inventory", "$..price", 100);
        Console.WriteLine(string.Join(", ", res44));    // >>> 1920, 2072, 3264, 1475, 3941
        // STEP_END

        // Tests for 'update_bikes' step.
        // REMOVE_START
        Assert.Equal("[1920,2072,3264,1475,3941]", (string?)res42);
        Assert.Equal("1820, 1972, 3164, 1375, 3841", string.Join(", ", res43));
        Assert.Equal("1920, 2072, 3264, 1475, 3941", string.Join(", ", res44));
        // REMOVE_END


        // STEP_START update_filters1
        bool res45 = db.JSON().Set(
            "bikes:inventory",
            "$.inventory.*[?(@.price<2000)].price",
            1500
        );
        Console.WriteLine(res45);   // >>> True

        RedisResult res46 = db.JSON().Get("bikes:inventory", path: "$..price");
        Console.WriteLine(res46);   // >>> [1500,2072,3264,1500,3941]
        // STEP_END

        // Tests for 'update_filters1' step.
        // REMOVE_START
        Assert.Equal("[1500,2072,3264,1500,3941]", (string?)res46);
        // REMOVE_END


        // STEP_START update_filters2
        long?[] res47 = db.JSON().ArrAppend(
            "bikes:inventory", "$.inventory.*[?(@.price<2000)].colors", "pink"
        );
        Console.WriteLine(string.Join(", ", res47));    // >>> 3, 3

        RedisResult res48 = db.JSON().Get("bikes:inventory", path: "$..[*].colors");
        Console.WriteLine(res48);   // >>> [["black","silver","pink"],["black","white"],["black","silver","pink"]]
        // STEP_END

        // Tests for 'update_filters2' step.
        // REMOVE_START
        Assert.Equal("3, 3", string.Join(", ", res47));
        Assert.Equal(
            "[[\"black\",\"silver\",\"pink\"],[\"black\",\"white\"],[\"black\",\"silver\",\"pink\"]]",
            (string?)res48);
        // REMOVE_END


        // HIDE_START
    }
}
// HIDE_END

