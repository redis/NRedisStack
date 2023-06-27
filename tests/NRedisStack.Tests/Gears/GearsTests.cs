using Xunit;
using StackExchange.Redis;
using Moq;

namespace NRedisStack.Tests.Gears;

public class GearsTests : AbstractNRedisStackTest, IDisposable
{
    Mock<IDatabase> _mock = new Mock<IDatabase>();
    private readonly string key = "BLOOM_TESTS";
    public GearsTests(RedisFixture redisFixture) : base(redisFixture) { }

    public void Dispose()
    {
        redisFixture.Redis.GetDatabase().KeyDelete(key);
    }


    [Fact]
    [Trait("Category", "edge")]
    public void TestTFunctionLoadDelete()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        Assert.True(db.TFunctionLoad("#!js api_version=1.0 name=lib\n redis.registerFunction('foo', ()=>{return 'bar'})"));
        Assert.True(db.TFunctionDelete("lib"));
    }


    [Fact]
    [Trait("Category", "edge")]
    public async Task TestTFunctionLoadDeleteAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        TryDeleteLib(db, "lib", "lib1", "lib2", "lib3");

        Assert.True(await db.TFunctionLoadAsync("#!js api_version=1.0 name=lib\n redis.registerFunction('foo', ()=>{return 'bar'})"));
        Assert.True(await db.TFunctionDeleteAsync("lib"));
    }

    [Fact]
    [Trait("Category", "edge")]
    public void TestTFunctionList()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        TryDeleteLib(db, "lib", "lib1", "lib2", "lib3");

        Assert.True(db.TFunctionLoad("#!js api_version=1.0 name=lib1\n redis.registerFunction('foo', ()=>{return 'bar'})"));
        Assert.True(db.TFunctionLoad("#!js api_version=1.0 name=lib2\n redis.registerFunction('foo', ()=>{return 'bar'})"));
        Assert.True(db.TFunctionLoad("#!js api_version=1.0 name=lib3\n redis.registerFunction('foo', ()=>{return 'bar'})"));

        // test error throwing:
        Assert.Throws<ArgumentOutOfRangeException>(() => db.TFunctionList(verbose: 8));
        var functions = db.TFunctionList(verbose: 1);
        Assert.Equal(3, functions.Length);

        HashSet<string> expectedNames = new HashSet<string> { "lib1", "lib2", "lib3" };
        HashSet<string> actualNames = new HashSet<string>{
            functions[0]["name"].ToString()!,
            functions[1]["name"].ToString()!,
            functions[2]["name"].ToString()!
        };

        Assert.Equal(expectedNames, actualNames);


        Assert.True(db.TFunctionDelete("lib1"));
        Assert.True(db.TFunctionDelete("lib2"));
        Assert.True(db.TFunctionDelete("lib3"));
    }

    [Fact]
    [Trait("Category", "edge")]
    public async Task TestTFunctionListAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        TryDeleteLib(db, "lib", "lib1", "lib2", "lib3");

        Assert.True(await db.TFunctionLoadAsync("#!js api_version=1.0 name=lib1\n redis.registerFunction('foo', ()=>{return 'bar'})"));
        Assert.True(await db.TFunctionLoadAsync("#!js api_version=1.0 name=lib2\n redis.registerFunction('foo', ()=>{return 'bar'})"));
        Assert.True(await db.TFunctionLoadAsync("#!js api_version=1.0 name=lib3\n redis.registerFunction('foo', ()=>{return 'bar'})"));

        var functions = await db.TFunctionListAsync(verbose: 1);
        Assert.Equal(3, functions.Length);

        HashSet<string> expectedNames = new HashSet<string> { "lib1", "lib2", "lib3" };
        HashSet<string> actualNames = new HashSet<string>{
            functions[0]["name"].ToString()!,
            functions[1]["name"].ToString()!,
            functions[2]["name"].ToString()!
        };

        Assert.Equal(expectedNames, actualNames);


        Assert.True(await db.TFunctionDeleteAsync("lib1"));
        Assert.True(await db.TFunctionDeleteAsync("lib2"));
        Assert.True(await db.TFunctionDeleteAsync("lib3"));
    }

    [Fact]
    [Trait("Category", "edge")]
    public void TestTFCall()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        TryDeleteLib(db, "lib", "lib1", "lib2", "lib3");

        Assert.True(db.TFunctionLoad("#!js api_version=1.0 name=lib\n redis.registerFunction('foo', ()=>{return 'bar'})"));
        Assert.Equal("bar", db.TFCall("lib", "foo", async: false).ToString());
        Assert.Equal("bar", db.TFCall("lib", "foo", async: true).ToString());

        Assert.True(db.TFunctionDelete("lib"));
    }

    [Fact]
    [Trait("Category", "edge")]
    public async Task TestTFCallAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        TryDeleteLib(db, "lib", "lib1", "lib2", "lib3");

        Assert.True(await db.TFunctionLoadAsync("#!js api_version=1.0 name=lib\n redis.registerFunction('foo', ()=>{return 'bar'})"));
        Assert.Equal("bar", (await db.TFCallAsync("lib", "foo", async: false)).ToString());
        Assert.Equal("bar", (await db.TFCallAsync("lib", "foo", async: true)).ToString());

        Assert.True(await db.TFunctionDeleteAsync("lib"));
    }

    [Fact]
    [Trait("Category", "edge")]
    public void TestGearsCommandBuilder()
    {
        // TFunctionLoad:
        var buildCommand = GearsCommandBuilder
            .TFunctionLoad("#!js api_version=1.0 name=lib\n redis.registerFunction('foo', ()=>{return 'bar'})",
            true, "config");
        var expected = new List<object>
        {
            "LOAD",
            "REPLACE",
            "CONFIG",
            "config",
            "#!js api_version=1.0 name=lib\n redis.registerFunction('foo', ()=>{return 'bar'})"
        };
        Assert.Equal("TFUNCTION", buildCommand.Command);
        Assert.Equal(expected, buildCommand.Args);

        // TFunctionDelete:
        buildCommand = GearsCommandBuilder.TFunctionDelete("lib");
        expected = new List<object>
        {
            "DELETE",
            "lib"
        };
        Assert.Equal("TFUNCTION", buildCommand.Command);
        Assert.Equal(expected, buildCommand.Args);

        // TFunctionList:
        buildCommand = GearsCommandBuilder.TFunctionList(true, 2, "lib");
        expected = new List<object>
        {
            "LIST",
            "WITHCODE",
            "vv",
            "LIBRARY",
            "lib",
        };
        Assert.Equal("TFUNCTION", buildCommand.Command);
        Assert.Equal(expected, buildCommand.Args);

        // TFCall:
        var buildSync = GearsCommandBuilder.TFCall("libName", "funcName", new string[] { "key1", "key2" }, new string[] { "arg1", "arg2" }, false);
        var buildAsync = GearsCommandBuilder.TFCall("libName", "funcName", new string[] { "key1", "key2" }, new string[] { "arg1", "arg2" }, true);

        expected = new List<object>
        {
            "libName.funcName",
            2,
            "key1",
            "key2",
            "arg1",
            "arg2"
        };

        Assert.Equal("TFCALL", buildSync.Command);
        Assert.Equal(expected, buildSync.Args);

        Assert.Equal("TFCALLASYNC", buildAsync.Command);
        Assert.Equal(expected, buildAsync.Args);
    }

    private static void TryDeleteLib(IDatabase db, params string[] libNames)
    {
        try
        {
            foreach(var libName in libNames)
                db.TFunctionDelete(libName);
        }
        catch (RedisServerException) { }
    }
}
