using Xunit;
using StackExchange.Redis;
using NRedisStack.RedisStackCommands;
using Moq;
using NRedisStack.Search.FT.CREATE;
using NRedisStack.Search;

namespace NRedisStack.Tests.Bloom;

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
        var json = db.JSON();

        // call async version of JSON.SET/GET
        await json.SetAsync("key", "$", new { name = "John", age = 30, city = "New York" });
        var john = await json.GetAsync("key");
    }
}