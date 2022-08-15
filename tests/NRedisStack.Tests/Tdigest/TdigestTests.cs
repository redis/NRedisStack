using Xunit;
using StackExchange.Redis;
using NRedisStack.Core.RedisStackCommands;
using Moq;

namespace NRedisStack.Tests.Tdigest;

public class TdigestTests : AbstractNRedisStackTest, IDisposable
{
    Mock<IDatabase> _mock = new Mock<IDatabase>();
    private readonly string key = "TDIGEST_TESTS";
    public TdigestTests(RedisFixture redisFixture) : base(redisFixture) { }

    public void Dispose()
    {
        redisFixture.Redis.GetDatabase().KeyDelete(key);
    }

//     private void AssertMergedUnmergedNodes( IDatabase db, string key, int mergedNodes, int unmergedNodes) {
//     var info = db.TDIGEST().Info(key);
//     Assert.Equal((long) mergedNodes, info.MergedNodes);
//     Assert.Equal((long) unmergedNodes, info.UnmergedNodes);
//   }

//   private void assertTotalWeight(IDatabase db, string key, double totalWeight) {
//     var info = db.TDIGEST().Info(key);
//     Assert.Equal(totalWeight, info.MergedWeight Double.parseDouble((string) info.get("Merged weight"))
//         + Double.parseDouble((string) info.get("Unmerged weight")), 0.01);
//   }

//     [Fact]
//     public void AssertMergedUnmergedNodes()
//     {
//         IDatabase db = redisFixture.Redis.GetDatabase();
//         db.Execute("FLUSHALL");

//         var info = db.TDIGEST().Info()

//     }
}