using Xunit;
using StackExchange.Redis;
using NRedisStack.RedisStackCommands;
using Moq;
using NRedisStack.Search.FT.CREATE;
using NRedisStack.Search;
using NRedisStack.DataTypes;
using NRedisStack.Literals.Enums;

namespace NRedisStack.Tests;

public class ExaplesTests : AbstractNRedisStackTest, IDisposable
{
    Mock<IDatabase> _mock = new Mock<IDatabase>();
    private readonly string key = "EXAMPLES_TESTS";
    public ExaplesTests(RedisFixture redisFixture) : base(redisFixture) { }

    public void Dispose()
    {
        redisFixture.Redis.GetDatabase().KeyDelete(key);
    }

    [Fact]
    public void HSETandSearch()
    {
        // Connect to the Redis server
        var redis = ConnectionMultiplexer.Connect("localhost");

        // Get a reference to the database and for search commands:
        var db = redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();

        // Use HSET to add a field-value pair to a hash
        db.HashSet("profesor:5555", new HashEntry[] { new("first", "Albert"), new("last", "Blue"), new("age", "55") });
        db.HashSet("student:1111", new HashEntry[] { new("first", "Joe"), new("last", "Dod"), new("age", "18") });
        db.HashSet("pupil:2222", new HashEntry[] { new("first", "Jen"), new("last", "Rod"), new("age", "14") });
        db.HashSet("student:3333", new HashEntry[] { new("first", "El"), new("last", "Mark"), new("age", "17") });
        db.HashSet("pupil:4444", new HashEntry[] { new("first", "Pat"), new("last", "Shu"), new("age", "21") });
        db.HashSet("student:5555", new HashEntry[] { new("first", "Joen"), new("last", "Ko"), new("age", "20") });
        db.HashSet("teacher:6666", new HashEntry[] { new("first", "Pat"), new("last", "Rod"), new("age", "20") });

        // Create the schema to index first and last as text fields, and age as a numeric field
        var schema = new Schema().AddTextField("first").AddTextField("last").AddNumericField("age");
        // Filter the index to only include hashes with an age greater than 16, and prefix of student: or pupil:
        var parameters = FTCreateParams.CreateParams().Filter("@age>16").Prefix("student:", "pupil:");
        // Create the index
        ft.Create("example_index", parameters, schema);

        // Search all hashes in the index
        var noFilters = ft.Search("example_index", new Query());
        // noFilters now contains: student:1111, student:5555, pupil:4444, student:3333

        // Search for hashes with a first name starting with Jo
        var startWithJo = ft.Search("example_index", new Query("@first:Jo*"));
        // startWithJo now contains: student:1111 (Joe), student:5555 (Joen)

        // Search for hashes with first name of Pat
        var namedPat = ft.Search("example_index", new Query("@first:Pat"));
        // namedPat now contains pupil:4444 (Pat). teacher:6666 (Pat) is not included because it does not have a prefix of student: or pupil:

        // Search for hashes with last name of Rod
        var lastNameRod = ft.Search("example_index", new Query("@last:Rod"));
        // lastNameRod is empty because there are no hashes with a last name of Rod that match the index definition
    }

    [Fact]
    public async Task AsyncExample()
    {
        // Connect to the Redis server
        var redis = await ConnectionMultiplexer.ConnectAsync("localhost");
        var db = redis.GetDatabase();
        db.Execute("FLUSHALL");
        var json = db.JSON();

        // call async version of JSON.SET/GET
        await json.SetAsync("key", "$", new { name = "John", age = 30, city = "New York" });
        var john = await json.GetAsync("key");
    }

    [Fact]
    public void PipelineExample()
    {
        // Connect to the Redis server and Setup 2 Pipelines

        // Pipeline can get IDatabase for pipeline1
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var pipeline1 = new Pipeline(db);

        // Pipeline can get IConnectionMultiplexer for pipeline2
        var redis = ConnectionMultiplexer.Connect("localhost");
        var pipeline2 = new Pipeline(redis);

        // Add JsonSet to pipeline
        pipeline1.Json.SetAsync("person", "$", new { name = "John", age = 30, city = "New York", nicknames = new[] { "John", "Johny", "Jo" } });

        // Increase age by 2
        pipeline1.Json.NumIncrbyAsync("person", "$.age", 2);

        // Execute the pipeline1
        pipeline1.Execute();

        // Clear the nicknames from the Json
        pipeline2.Json.ClearAsync("person", "$.nicknames");

        // Del the nicknames
        pipeline2.Json.DelAsync("person", "$.nicknames");

        // Get the Json response
        var getResponse = pipeline2.Json.GetAsync("person");

        // Execute the pipeline2
        pipeline2.Execute();

        // Get the result back JSON
        var result = getResponse.Result;

        // Assert the result
        var expected = "{\"name\":\"John\",\"age\":32,\"city\":\"New York\"}";
        Assert.Equal(expected, result.ToString());
    }

    [Fact]
    public void JsonWithSearchPipeline()
    {
        //Setup pipeline connection
        var pipeline = new Pipeline(ConnectionMultiplexer.Connect("localhost"));
        pipeline.Db.ExecuteAsync("FLUSHALL");

        // Add JsonSet to pipeline
        pipeline.Json.SetAsync("person:01", "$", new { name = "John", age = 30, city = "New York" });
        pipeline.Json.SetAsync("person:02", "$", new { name = "Joy", age = 25, city = "Los Angeles" });
        pipeline.Json.SetAsync("person:03", "$", new { name = "Mark", age = 21, city = "Chicago" });
        pipeline.Json.SetAsync("person:04", "$", new { name = "Steve", age = 24, city = "Phoenix" });
        pipeline.Json.SetAsync("person:05", "$", new { name = "Michael", age = 55, city = "San Antonio" });

        // Create the schema to index name as text field, age as a numeric field and city as tag field.
        var schema = new Schema().AddTextField("name").AddNumericField("age", true).AddTagField("city");

        // Filter the index to only include Jsons with prefix of person:
        var parameters = FTCreateParams.CreateParams().On(IndexDataType.JSON).Prefix("person:");

        // Create the index via pipeline
        pipeline.Ft.CreateAsync("person-idx", parameters, schema);

        // Search for all indexed person records
        var getAllPersons = pipeline.Ft.SearchAsync("person-idx", new Query());

        // execute the pipeline
        pipeline.Execute();

        // Get the total count of people records that indexed.
        var getAllPersonsResult = getAllPersons.Result;
        var count = getAllPersonsResult.TotalResults;

        // Gets the first person form the result.
        var firstPerson = getAllPersonsResult.Documents.FirstOrDefault();
        // first person is John here.

        Assert.Equal(5, count);
        Assert.Equal("person:01", firstPerson?.Id);
    }

    [Fact]
    public async Task PipelineWithAsync()
    {
        // Connect to the Redis server
        var redis = ConnectionMultiplexer.Connect("localhost");

        // Get a reference to the database
        var db = redis.GetDatabase();
        db.Execute("FLUSHALL");
        // Setup pipeline connection
        var pipeline = new Pipeline(redis);


        // Create metedata lables for time-series.
        TimeSeriesLabel label1 = new TimeSeriesLabel("temp", "TLV");
        TimeSeriesLabel label2 = new TimeSeriesLabel("temp", "JLM");
        var labels1 = new List<TimeSeriesLabel> { label1 };
        var labels2 = new List<TimeSeriesLabel> { label2 };

        // Create a new time-series.
        pipeline.Ts.CreateAsync("temp:TLV", labels: labels1);
        pipeline.Ts.CreateAsync("temp:JLM", labels: labels2);

        // Adding multiple sequenece of time-series data.
        List<(string, TimeStamp, double)> sequence1 = new List<(string, TimeStamp, double)>()
        {
            ("temp:TLV",1000,30),
            ("temp:TLV", 1010 ,35),
            ("temp:TLV", 1020, 9999),
            ("temp:TLV", 1030, 40)
        };
        List<(string, TimeStamp, double)> sequence2 = new List<(string, TimeStamp, double)>()
        {
            ("temp:JLM",1005,30),
            ("temp:JLM", 1015 ,35),
            ("temp:JLM", 1025, 9999),
            ("temp:JLM", 1035, 40)
        };

        // Adding mutiple samples to mutiple series.
        pipeline.Ts.MAddAsync(sequence1);
        pipeline.Ts.MAddAsync(sequence2);

        // Execute the pipeline
        pipeline.Execute();

        // Get a reference to the database and for time-series commands
        var ts = db.TS();

        // Get only the location label for each last sample, use SELECTED_LABELS.
        var respons = await ts.MGetAsync(new List<string> { "temp=JLM" }, selectedLabels: new List<string> { "location" });

        // Assert the respons
        Assert.Equal(1, respons.Count);
        Assert.Equal("temp:JLM", respons[0].key);
    }

    [Fact]
    public void TransactionExample()
    {
        // implementation for transaction
    }
}