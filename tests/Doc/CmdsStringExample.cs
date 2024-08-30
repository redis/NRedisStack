// EXAMPLE: cmds_string
// HIDE_START

using NRedisStack.Tests;
using StackExchange.Redis;

// HIDE_END

// REMOVE_START
namespace Doc;
[Collection("DocsTests")]
// REMOVE_END

// HIDE_START
public class CmdsStringExample
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


        // STEP_START append1

        // STEP_END

        // Tests for 'append1' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START append2

        // STEP_END

        // Tests for 'append2' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START decr

        // STEP_END

        // Tests for 'decr' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START decrby

        // STEP_END

        // Tests for 'decrby' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START get

        // STEP_END

        // Tests for 'get' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START getdel

        // STEP_END

        // Tests for 'getdel' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START getex

        // STEP_END

        // Tests for 'getex' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START getrange

        // STEP_END

        // Tests for 'getrange' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START getset

        // STEP_END

        // Tests for 'getset' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START incr
        bool incrResult1 = db.StringSet("mykey", "10");
        Console.WriteLine(incrResult1); // >>> true

        long incrResult2 = db.StringIncrement("mykey");
        Console.WriteLine(incrResult2); // >>> 11

        RedisValue incrResult3 = db.StringGet("mykey");
        Console.WriteLine(incrResult3); // >>> 11
        // STEP_END

        // Tests for 'incr' step.
        // REMOVE_START
        Assert.True(incrResult1);
        Assert.Equal(11, incrResult2);
        Assert.Equal("11", incrResult3);
        // REMOVE_END


        // STEP_START incrby

        // STEP_END

        // Tests for 'incrby' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START incrbyfloat

        // STEP_END

        // Tests for 'incrbyfloat' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START lcs1

        // STEP_END

        // Tests for 'lcs1' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START lcs2

        // STEP_END

        // Tests for 'lcs2' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START lcs3

        // STEP_END

        // Tests for 'lcs3' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START lcs4

        // STEP_END

        // Tests for 'lcs4' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START lcs5

        // STEP_END

        // Tests for 'lcs5' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START mget

        // STEP_END

        // Tests for 'mget' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START mset

        // STEP_END

        // Tests for 'mset' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START msetnx

        // STEP_END

        // Tests for 'msetnx' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START psetex

        // STEP_END

        // Tests for 'psetex' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START set

        // STEP_END

        // Tests for 'set' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START setex

        // STEP_END

        // Tests for 'setex' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START setnx

        // STEP_END

        // Tests for 'setnx' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START setrange1

        // STEP_END

        // Tests for 'setrange1' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START setrange2

        // STEP_END

        // Tests for 'setrange2' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START strlen

        // STEP_END

        // Tests for 'strlen' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START substr

        // STEP_END

        // Tests for 'substr' step.
        // REMOVE_START

        // REMOVE_END


        // HIDE_START
    }
}
// HIDE_END

