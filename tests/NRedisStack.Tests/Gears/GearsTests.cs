using Xunit;
using StackExchange.Redis;

namespace NRedisStack.Tests.Gears;

public class GearsTests : AbstractNRedisStackTest, IDisposable
{
    // private readonly string key = "GEARS_TESTS";
    public GearsTests(RedisFixture redisFixture) : base(redisFixture) { }

    [SkipIfRedis(Is.Enterprise, Comparison.LessThan, "7.1.242")]
    public void TestTFunctionLoadDelete()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        if (!redisFixture.isEnterprise)
            db.ExecuteAllShards("REDISGEARS_2.REFRESHCLUSTER");
        db.FlushAll();
        Assert.True(db.TFunctionLoad(GenerateLibCode("lib")));
        Assert.True(db.TFunctionDelete("lib"));
    }


    [SkipIfRedis(Is.Enterprise, Comparison.LessThan, "7.1.242")]
    public async Task TestTFunctionLoadDeleteAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        if (!redisFixture.isEnterprise)
            db.ExecuteAllShards("REDISGEARS_2.REFRESHCLUSTER");
        db.FlushAll();
        TryDeleteLib(db, "lib", "lib1", "lib2", "lib3");

        Assert.True(await db.TFunctionLoadAsync(GenerateLibCode("lib")));
        Assert.True(await db.TFunctionDeleteAsync("lib"));
    }

    [SkipIfRedis(Is.Enterprise, Comparison.LessThan, "7.1.242")]
    public void TestTFunctionList()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        if (!redisFixture.isEnterprise)
            db.ExecuteAllShards("REDISGEARS_2.REFRESHCLUSTER");
        db.FlushAll();
        TryDeleteLib(db, "lib", "lib1", "lib2", "lib3");

        Assert.True(db.TFunctionLoad(GenerateLibCode("lib1")));
        Assert.True(db.TFunctionLoad(GenerateLibCode("lib2")));
        Assert.True(db.TFunctionLoad(GenerateLibCode("lib3")));

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

    [SkipIfRedis(Is.Enterprise, Comparison.LessThan, "7.1.242")]
    public async Task TestTFunctionListAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        if (!redisFixture.isEnterprise)
            db.ExecuteAllShards("REDISGEARS_2.REFRESHCLUSTER");
        db.FlushAll();
        TryDeleteLib(db, "lib", "lib1", "lib2", "lib3");

        Assert.True(await db.TFunctionLoadAsync(GenerateLibCode("lib1")));
        Assert.True(await db.TFunctionLoadAsync(GenerateLibCode("lib2")));
        Assert.True(await db.TFunctionLoadAsync(GenerateLibCode("lib3")));

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

    [SkipIfRedis(Is.Enterprise, Comparison.LessThan, "7.1.242")]
    public void TestTFCall()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        if (!redisFixture.isEnterprise)
            db.ExecuteAllShards("REDISGEARS_2.REFRESHCLUSTER");
        db.FlushAll();
        TryDeleteLib(db, "lib", "lib1", "lib2", "lib3");

        Assert.True(db.TFunctionLoad(GenerateLibCode("lib")));
        Assert.Equal("bar", db.TFCall_("lib", "foo").ToString());
        Assert.Equal("bar", db.TFCallAsync_("lib", "foo").ToString());

        Assert.True(db.TFunctionDelete("lib"));
    }

    [SkipIfRedis(Is.Enterprise, Comparison.LessThan, "7.1.242")]
    public async Task TestTFCallAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        if (!redisFixture.isEnterprise)
            db.ExecuteAllShards("REDISGEARS_2.REFRESHCLUSTER");
        db.FlushAll();
        TryDeleteLib(db, "lib", "lib1", "lib2", "lib3");

        Assert.True(await db.TFunctionLoadAsync(GenerateLibCode("lib")));
        Assert.Equal("bar", (await db.TFCall_Async("lib", "foo")).ToString());
        Assert.Equal("bar", (await db.TFCallAsync_Async("lib", "foo")).ToString());

        Assert.True(await db.TFunctionDeleteAsync("lib"));
    }

    [SkipIfRedis(Is.Enterprise, Comparison.LessThan, "7.1.242")]
    public void TestGearsCommandBuilder()
    {
        // TFunctionLoad:
        var buildCommand = GearsCommandBuilder
            .TFunctionLoad(GenerateLibCode("lib"),
            true, "config");
        var expected = new List<object>
        {
            "LOAD",
            "REPLACE",
            "CONFIG",
            "config",
            GenerateLibCode("lib")
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
        foreach (var libName in libNames)
        {
            try
            {
                db.ExecuteAllShards(GearsCommandBuilder.TFunctionDelete(libName));
            }
            catch (RedisServerException) { } // ignore
        }
    }

    private static string GenerateLibCode(string libName)
    {
        return $"#!js api_version=1.0 name={libName}\n redis.registerFunction('foo', ()=>{{return 'bar'}})";
    }
}
