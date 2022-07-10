using Xunit;
using System;
using System.Collections.Generic;
using StackExchange.Redis;
using System.Linq;
using System.IO;
using NRedisStack.Core;
using NRedisStack.Core.RedisStackCommands;
using Moq;

using System.Text.Json;
using System.Text.Json.Serialization;


namespace NRedisStack.Tests.Bloom;

public class BloomTests : AbstractNRedisStackTest, IDisposable
{
    Mock<IDatabase> _mock = new Mock<IDatabase>();
    private readonly string key = "BLOOM_TESTS";
    public BloomTests(RedisFixture redisFixture) : base(redisFixture) { }

    public void Dispose()
    {
        redisFixture.Redis.GetDatabase().KeyDelete(key);
    }

    [Fact]
    public void TestBfAddWhenExist()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();

        Assert.True(db.BfAdd(key, "item1").ToString() == "1"); // first time
        Assert.True(db.BfAdd(key, "item1").ToString() == "0"); // second time
    }

    [Fact]
    public void TestBfAddExists()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();

        db.BfAdd(key, "item1");
        Assert.True(db.BfExists(key, "item1").ToString() == "1");
    }
}