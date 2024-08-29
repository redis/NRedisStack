// EXAMPLE: cmds_generic
// HIDE_START

using System.ComponentModel.DataAnnotations;
using NRedisStack.Tests;
using StackExchange.Redis;

// HIDE_END

// REMOVE_START
namespace Doc;
[Collection("DocsTests")]
// REMOVE_END

// HIDE_START
public class CmdsGenericExample
{

    [SkipIfRedis(Is.OSSCluster)]
    public void run()
    {
        var muxer = ConnectionMultiplexer.Connect("localhost:6379");
        var db = muxer.GetDatabase();
        //REMOVE_START
        // Clear any keys here before using them in tests.

        //REMOVE_END
// HIDE_END


        // STEP_START copy

        // STEP_END

        // Tests for 'copy' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START del
        bool delResult1 = db.StringSet("key1", "Hello");
        Console.WriteLine(delResult1);  // >>> true

        bool delResult2 = db.StringSet("key2", "World");
        Console.WriteLine(delResult2);  // >>> true

        long delResult3 = db.KeyDelete(new RedisKey[] {"key1", "key2", "key3"});
        Console.WriteLine(delResult3);  // >>> 2
        // STEP_END

        // Tests for 'del' step.
        // REMOVE_START
        Assert.True(delResult1);
        Assert.True(delResult2);
        Assert.Equal(2, delResult3);
        // REMOVE_END


        // STEP_START dump

        // STEP_END

        // Tests for 'dump' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START exists

        // STEP_END

        // Tests for 'exists' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START expire
        bool expireResult1 = db.StringSet("mykey", "Hello");
        Console.WriteLine(expireResult1);   // >>> true

        bool expireResult2 = db.KeyExpire("mykey", new TimeSpan(0, 0, 10));
        Console.WriteLine(expireResult2);   // >>> true

        TimeSpan expireResult3 = db.KeyTimeToLive("mykey") ?? TimeSpan.Zero;
        Console.WriteLine(Math.Round(expireResult3.TotalSeconds));   // >>> 10

        bool expireResult4 = db.StringSet("mykey", "Hello World");
        Console.WriteLine(expireResult4);   // >>> true

        TimeSpan expireResult5 = db.KeyTimeToLive("mykey") ?? TimeSpan.Zero;
        Console.WriteLine(Math.Round(expireResult5.TotalSeconds).ToString());   // >>> 0

        bool expireResult6 = db.KeyExpire("mykey", new TimeSpan(0, 0, 10), ExpireWhen.HasExpiry);
        Console.WriteLine(expireResult6);   // >>> false

        TimeSpan expireResult7 = db.KeyTimeToLive("mykey") ??  TimeSpan.Zero;
        Console.WriteLine(Math.Round(expireResult7.TotalSeconds));   // >>> 0

        bool expireResult8 = db.KeyExpire("mykey", new TimeSpan(0, 0, 10), ExpireWhen.HasNoExpiry);
        Console.WriteLine(expireResult8);   // >>> true

        TimeSpan expireResult9 = db.KeyTimeToLive("mykey") ?? TimeSpan.Zero;
        Console.WriteLine(Math.Round(expireResult9.TotalSeconds));   // >>> 10
        // STEP_END

        // Tests for 'expire' step.
        // REMOVE_START
        Assert.True(expireResult1);
        Assert.True(expireResult2);
        Assert.Equal(10, Math.Round(expireResult3.TotalSeconds));
        Assert.True(expireResult4);
        Assert.Equal(0, Math.Round(expireResult5.TotalSeconds));
        Assert.False(expireResult6);
        Assert.Equal(0, Math.Round(expireResult7.TotalSeconds));
        Assert.True(expireResult8);
        Assert.Equal(10, Math.Round(expireResult9.TotalSeconds));
        // REMOVE_END


        // STEP_START expireat

        // STEP_END

        // Tests for 'expireat' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START expiretime

        // STEP_END

        // Tests for 'expiretime' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START keys

        // STEP_END

        // Tests for 'keys' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START migrate

        // STEP_END

        // Tests for 'migrate' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START move

        // STEP_END

        // Tests for 'move' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START object encoding

        // STEP_END

        // Tests for 'object encoding' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START object freq

        // STEP_END

        // Tests for 'object freq' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START object idletime

        // STEP_END

        // Tests for 'object idletime' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START object refcount

        // STEP_END

        // Tests for 'object refcount' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START persist

        // STEP_END

        // Tests for 'persist' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START pexpire

        // STEP_END

        // Tests for 'pexpire' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START pexpireat

        // STEP_END

        // Tests for 'pexpireat' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START pexpiretime

        // STEP_END

        // Tests for 'pexpiretime' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START pttl

        // STEP_END

        // Tests for 'pttl' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START randomkey

        // STEP_END

        // Tests for 'randomkey' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START rename

        // STEP_END

        // Tests for 'rename' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START renamenx

        // STEP_END

        // Tests for 'renamenx' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START restore

        // STEP_END

        // Tests for 'restore' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START scan

        // STEP_END

        // Tests for 'scan' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START sort

        // STEP_END

        // Tests for 'sort' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START sort_ro

        // STEP_END

        // Tests for 'sort_ro' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START touch

        // STEP_END

        // Tests for 'touch' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START ttl

        // STEP_END

        // Tests for 'ttl' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START type

        // STEP_END

        // Tests for 'type' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START unlink

        // STEP_END

        // Tests for 'unlink' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START wait

        // STEP_END

        // Tests for 'wait' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START waitaof

        // STEP_END

        // Tests for 'waitaof' step.
        // REMOVE_START

        // REMOVE_END


// HIDE_START
    }
}
// HIDE_END

