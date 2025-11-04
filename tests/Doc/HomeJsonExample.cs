// EXAMPLE: cs_home_json
// BINDER_ID netsync-cs_home_json
// STEP_START import
using NRedisStack.RedisStackCommands;
using NRedisStack.Search;
using NRedisStack.Search.Aggregation;
using NRedisStack.Search.Literals.Enums;
using StackExchange.Redis;
// STEP_END

// REMOVE_START
using NRedisStack.Tests;

namespace Doc;
[Collection("DocsTests")]
// REMOVE_END

public class HomeJsonExample
// REMOVE_START
: AbstractNRedisStackTest, IDisposable
// REMOVE_END
{
    // REMOVE_START
    public HomeJsonExample(EndpointsFixture fixture) : base(fixture) { }

    [SkippableFact]
    // REMOVE_END
    public void Run()
    {
        //REMOVE_START
        // This is needed because we're constructing ConfigurationOptions in the test before calling GetConnection
        SkipIfTargetConnectionDoesNotExist(EndpointsFixture.Env.Standalone);
        var _ = GetCleanDatabase(EndpointsFixture.Env.Standalone);
        //REMOVE_END

        // STEP_START connect
        var muxer = ConnectionMultiplexer.Connect("localhost:6379");
        var db = muxer.GetDatabase();
        // STEP_END

        // STEP_START cleanup_json
        db.KeyDelete(["user:1", "user:2", "user:3"]);
        try { db.FT().DropIndex("idx:users"); } catch { }
        // STEP_END

        // STEP_START create_data
        var user1 = new
        {
            name = "Paul John",
            email = "paul.john@example.com",
            age = 42,
            city = "London"
        };

        var user2 = new
        {
            name = "Eden Zamir",
            email = "eden.zamir@example.com",
            age = 29,
            city = "Tel Aviv"
        };

        var user3 = new
        {
            name = "Paul Zamir",
            email = "paul.zamir@example.com",
            age = 35,
            city = "Tel Aviv"
        };
        // STEP_END

        // STEP_START make_index
        var schema = new Schema()
            .AddTextField(new FieldName("$.name", "name"))
            .AddTagField(new FieldName("$.city", "city"))
            .AddNumericField(new FieldName("$.age", "age"));

        bool indexCreated = db.FT().Create(
            "idx:users",
            new FTCreateParams()
                .On(IndexDataType.JSON)
                .Prefix("user:"),
            schema
        );
        // STEP_END
        // REMOVE_START
        Assert.True(indexCreated);
        // REMOVE_END


        // STEP_START add_data
        bool user1Set = db.JSON().Set("user:1", "$", user1);
        bool user2Set = db.JSON().Set("user:2", "$", user2);
        bool user3Set = db.JSON().Set("user:3", "$", user3);
        // STEP_END
        // REMOVE_START
        Assert.True(user1Set);
        Assert.True(user2Set);
        Assert.True(user3Set);
        // REMOVE_END


        // STEP_START query1
        SearchResult findPaulResult = db.FT().Search(
            "idx:users",
            new("Paul @age:[30 40]")
        );
        Console.WriteLine(string.Join(
            ", ",
            findPaulResult.Documents.Select(x => x["json"])
        ));
        // >>> {"name":"Paul Zamir","email":"paul.zamir@example.com", ...
        // STEP_END
        // REMOVE_START
        Assert.Equal(
            "{\"name\":\"Paul Zamir\",\"email\":\"paul.zamir@example.com\",\"age\":35,\"city\":\"Tel Aviv\"}",
            string.Join(", ", findPaulResult.Documents.Select(x => x["json"]))
        );
        // REMOVE_END


        // STEP_START query2
        var citiesResult = db.FT().Search(
            "idx:users",
            new Query("Paul")
                .ReturnFields(new FieldName("$.city", "city"))
        );
        Console.WriteLine(string.Join(
            ", ",
            citiesResult.Documents.Select(x => x["city"]).OrderBy(x => x)
        ));
        // >>> London, Tel Aviv
        // STEP_END
        // REMOVE_START
        Assert.Equal(
            "London, Tel Aviv",
            string.Join(", ", citiesResult.Documents.Select(x => x["city"]).OrderBy(x => x))
        );
        // REMOVE_END


        // STEP_START query3
        AggregationRequest aggRequest = new AggregationRequest("*")
            .GroupBy("@city", Reducers.Count().As("count"));

        AggregationResult aggResult = db.FT().Aggregate("idx:users", aggRequest);
        IReadOnlyList<Dictionary<string, RedisValue>> resultsList =
                                                        aggResult.GetResults();

        for (var i = 0; i < resultsList.Count; i++)
        {
            Dictionary<string, RedisValue> item = resultsList.ElementAt(i);
            Console.WriteLine($"{item["city"]} - {item["count"]}");
        }
        // >>> London - 1
        // >>> Tel Aviv - 2
        // STEP_END
        // REMOVE_START
        Assert.Equal(2, resultsList.Count);

        var sortedResults = resultsList.OrderBy(x => x["city"]);
        Dictionary<string, RedisValue> testItem = sortedResults.ElementAt(0);
        Assert.Equal("London", testItem["city"]);
        Assert.Equal(1, testItem["count"]);

        testItem = sortedResults.ElementAt(1);
        Assert.Equal("Tel Aviv", testItem["city"]);
        Assert.Equal(2, testItem["count"]);
        // REMOVE_END

        // STEP_START cleanup_hash
        db.KeyDelete(["huser:1", "huser:2", "huser:3"]);
        try { db.FT().DropIndex("hash-idx:users"); } catch { }
        // STEP_END

        // STEP_START make_hash_index
        var hashSchema = new Schema()
            .AddTextField("name")
            .AddTagField("city")
            .AddNumericField("age");

        bool hashIndexCreated = db.FT().Create(
            "hash-idx:users",
            new FTCreateParams()
                .On(IndexDataType.HASH)
                .Prefix("huser:"),
            hashSchema
        );
        // STEP_END
        // REMOVE_START
        Assert.True(hashIndexCreated);
        // REMOVE_END

        // STEP_START add_hash_data
        db.HashSet("huser:1", [
            new("name", "Paul John"),
            new("email", "paul.john@example.com"),
            new("age", 42),
            new("city", "London")
        ]);

        db.HashSet("huser:2", [
            new("name", "Eden Zamir"),
            new("email", "eden.zamir@example.com"),
            new("age", 29),
            new("city", "Tel Aviv")
        ]);

        db.HashSet("huser:3", [
            new("name", "Paul Zamir"),
            new("email", "paul.zamir@example.com"),
            new("age", 35),
            new("city", "Tel Aviv")
        ]);
        // STEP_END

        // STEP_START query1_hash
        SearchResult findPaulHashResult = db.FT().Search(
            "hash-idx:users",
            new("Paul @age:[30 40]")
        );

        foreach (Document doc in findPaulHashResult.Documents)
        {
            Console.WriteLine(
                $"Name: {doc["name"]}, email: {doc["email"]}, " +
                $"age: {doc["age"]}, city:{doc["city"]}"
            );
        }
        // >>> Name: Paul Zamir, email: paul.zamir@example.com, age: 35, ...
        // STEP_END
        // REMOVE_START
        Document d = findPaulHashResult.Documents[0];
        Assert.Equal(
            "Name: Paul Zamir, email: paul.zamir@example.com, age: 35, city:Tel Aviv",
            $"Name: {d["name"]}, email: {d["email"]}, " +
                $"age: {d["age"]}, city:{d["city"]}"
        );
        // REMOVE_END
    }
}
