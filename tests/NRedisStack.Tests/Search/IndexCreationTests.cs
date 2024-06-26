using StackExchange.Redis;
using NRedisStack.Search;
using NRedisStack.RedisStackCommands;
using Xunit;
using System.Runtime.InteropServices;

namespace NRedisStack.Tests.Search;

public class IndexCreationTests : AbstractNRedisStackTest, IDisposable
{
    public IndexCreationTests(RedisFixture redisFixture) : base(redisFixture) { }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.3.240")]
    public void TestCreateFloat16VectorField()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT(2);

        var schema = new Schema().AddVectorField("v", Schema.VectorField.VectorAlgo.FLAT, new Dictionary<string, object>()
        {
            ["TYPE"] = "FLOAT16",
            ["DIM"] = "5",
            ["DISTANCE_METRIC"] = "L2",
        }).AddVectorField("v2", Schema.VectorField.VectorAlgo.FLAT, new Dictionary<string, object>()
        {
            ["TYPE"] = "BFLOAT16",
            ["DIM"] = "4",
            ["DISTANCE_METRIC"] = "L2",
        });
        Assert.True(ft.Create("idx", new FTCreateParams(), schema));

        short[] vec1 = new short[] { 2, 1, 2, 2, 2 };
        byte[] vec1ToBytes = MemoryMarshal.Cast<short, byte>(vec1).ToArray();

        short[] vec2 = new short[] { 1, 2, 2, 2 };
        byte[] vec2ToBytes = MemoryMarshal.Cast<short, byte>(vec2).ToArray();

        var entries = new HashEntry[] { new HashEntry("v", vec1ToBytes), new HashEntry("v2", vec2ToBytes) };
        db.HashSet("a", entries);
        db.HashSet("b", entries);
        db.HashSet("c", entries);

        var q = new Query("*=>[KNN 2 @v $vec]").ReturnFields("__v_score");
        var res = ft.Search("idx", q.AddParam("vec", vec1ToBytes));
        Assert.Equal(2, res.TotalResults);

        q = new Query("*=>[KNN 2 @v2 $vec]").ReturnFields("__v_score");
        res = ft.Search("idx", q.AddParam("vec", vec2ToBytes));
        Assert.Equal(2, res.TotalResults);
    }
}