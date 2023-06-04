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
    public void TestTFunctionLoadDelete()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        Assert.True(db.TFunctionLoad("#!js api_version=1.0 name=lib\n redis.registerFunction('foo', ()=>{return 'bar'})"));
        Assert.True(db.TFunctionDelete("lib"));
    }

    [Fact]
    public async Task TestTFunctionLoadDeleteAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        Assert.True(await db.TFunctionLoadAsync("#!js api_version=1.0 name=lib\n redis.registerFunction('foo', ()=>{return 'bar'})"));
        Assert.True(await db.TFunctionDeleteAsync("lib"));
    }

    [Fact]
    public void TestTFunctionList()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        Assert.True(db.TFunctionLoad("#!js api_version=1.0 name=lib1\n redis.registerFunction('foo', ()=>{return 'bar'})"));
        Assert.True(db.TFunctionLoad("#!js api_version=1.0 name=lib2\n redis.registerFunction('foo', ()=>{return 'bar'})"));
        Assert.True(db.TFunctionLoad("#!js api_version=1.0 name=lib3\n redis.registerFunction('foo', ()=>{return 'bar'})"));

        var functions = db.TFunctionList(verbose : 1);
        Assert.Equal(3, functions.Length);

        Assert.Equal("lib1", functions[0]["name"].ToString());
        Assert.Equal("lib2", functions[1]["name"].ToString());
        Assert.Equal("lib3", functions[2]["name"].ToString());


        Assert.True(db.TFunctionDelete("lib1"));
        Assert.True(db.TFunctionDelete("lib2"));
        Assert.True(db.TFunctionDelete("lib3"));
    }

    [Fact]
    public async Task TestTFunctionListAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        Assert.True(await db.TFunctionLoadAsync("#!js api_version=1.0 name=lib1\n redis.registerFunction('foo', ()=>{return 'bar'})"));
        Assert.True(await db.TFunctionLoadAsync("#!js api_version=1.0 name=lib2\n redis.registerFunction('foo', ()=>{return 'bar'})"));
        Assert.True(await db.TFunctionLoadAsync("#!js api_version=1.0 name=lib3\n redis.registerFunction('foo', ()=>{return 'bar'})"));

        var functions = await db.TFunctionListAsync(verbose : 1);
        Assert.Equal(3, functions.Length);

        Assert.Equal("lib1", functions[0]["name"].ToString());
        Assert.Equal("lib2", functions[1]["name"].ToString());
        Assert.Equal("lib3", functions[2]["name"].ToString());


        Assert.True(await db.TFunctionDeleteAsync("lib1"));
        Assert.True(await db.TFunctionDeleteAsync("lib2"));
        Assert.True(await db.TFunctionDeleteAsync("lib3"));
    }

    [Fact]
    public void TestCommandBuilder()
    {
        var buildCommand = GearsCommandBuilder
            .TFunctionLoad("#!js api_version=1.0 name=lib\n redis.registerFunction('foo', ()=>{return 'bar'})",
            "config", true);
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

        buildCommand = GearsCommandBuilder.TFunctionDelete("lib");
        expected = new List<object>
        {
            "DELETE",
            "lib"
        };
        Assert.Equal("TFUNCTION", buildCommand.Command);
        Assert.Equal(expected, buildCommand.Args);

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
    }
}