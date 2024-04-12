// EXAMPLE: stream_tutorial
// HIDE_START

using System.Runtime.CompilerServices;
using NRedisStack.Tests;
using StackExchange.Redis;

// HIDE_END

// REMOVE_START
namespace Doc;
[Collection("DocsTests")]
// REMOVE_END

// HIDE_START
public class Stream_tutorial
{

    [SkipIfRedis(Is.OSSCluster)]
    public void run()
    {
        var muxer = ConnectionMultiplexer.Connect("localhost:6379");
        var db = muxer.GetDatabase();
        //REMOVE_START
        // Clear any keys here before using them in tests.
        db.KeyDelete("race:france");
        db.KeyDelete("race:italy");
        db.KeyDelete("race:usa");
        //REMOVE_END
        // HIDE_END

        // STEP_START xadd
        RedisValue res1 = db.StreamAdd(
            "race:france",
            new NameValueEntry[] {
                new NameValueEntry("rider", "Castilla"),
                new NameValueEntry("speed", 30.2),
                new NameValueEntry("position", 1),
                new NameValueEntry("location_id", 1)
            }
        );
        Console.WriteLine(res1);    // >>> 1712668482289-0

        RedisValue res2 = db.StreamAdd(
            "race:france",
            new NameValueEntry[] {
                new NameValueEntry("rider", "Norem"),
                new NameValueEntry("speed", 28.8),
                new NameValueEntry("position", 3),
                new NameValueEntry("location_id", 1)
            }
        );
        Console.WriteLine(res2);    // >>> 1712668766534-1

        RedisValue res3 = db.StreamAdd(
            "race:france",
            new NameValueEntry[]{
                new NameValueEntry("rider", "Prickett"),
                new NameValueEntry("speed", 29.7),
                new NameValueEntry("position", 2),
                new NameValueEntry("location_id", 1)
            }
        );
        Console.WriteLine(res3);    // >>> 1712669055705-0

        // STEP_END

        // Tests for 'xadd' step.
        // REMOVE_START
        Assert.Equal(3, db.StreamLength("race:france"));
        // REMOVE_END


        // STEP_START xrange
        string StreamEntryToString(StreamEntry entry)
        {
            string[] values = new string[entry.Values.Length];

            for (int i = 0; i < entry.Values.Length; i++)
            {
                values[i] = $"{entry.Values[i].Name}: {entry.Values[i].Value}";
            }

            return $"{entry.Id}: [{string.Join(", ", values)}]";
        }

        StreamEntry[] res4 = db.StreamRange("race:france", "1712668482289-0", "+", 2);

        foreach (StreamEntry entry in res4)
        {
            Console.WriteLine(StreamEntryToString(entry));
        }

        // >>> 1712668482289-0: [rider: Castilla, speed: 30.199999999999999, position: 1, location_id: 1]
        // >>> 1712668766534-1: [rider: Norem, speed: 28.800000000000001, position: 3, location_id: 1]
        // STEP_END

        // Tests for 'xrange' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START xread_block
        StreamEntry[] res5 = db.StreamRead("race:france", 0, 100);

        foreach (StreamEntry entry in res4)
        {
            Console.WriteLine(StreamEntryToString(entry));
        }

        // >>> 1712668482289-0: [rider: Castilla, speed: 30.199999999999999, position: 1, location_id: 1]
        // >>> 1712668766534-1: [rider: Norem, speed: 28.800000000000001, position: 3, location_id: 1]       
        // >>> 1712669055705-0: [rider: Prickett, speed: 29.699999999999999, position: 2, location_id: 1]
        // STEP_END

        // Tests for 'xread_block' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START xadd_2
        RedisValue res6 = db.StreamAdd(
            "race:france",
            new NameValueEntry[]{
                new NameValueEntry("rider", "Castilla"),
                new NameValueEntry("speed", 29.9),
                new NameValueEntry("position", 1),
                new NameValueEntry("location_id", 2)
            }
        );

        Console.WriteLine(res6);    // >>> 1712675674750-0
        // STEP_END

        // Tests for 'xadd_2' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START xlen
        long res7 = db.StreamLength("race:france");
        Console.WriteLine(res7);    // >>> 4
        // STEP_END

        // Tests for 'xlen' step.
        // REMOVE_START
        Assert.Equal(4, res7);
        // REMOVE_END


        // STEP_START xadd_id
        RedisValue res8 = db.StreamAdd(
            "race:usa",
            new NameValueEntry[] {
                new NameValueEntry("racer", "Castilla")
            },
            "0-1"
        );
        Console.WriteLine(res8);    // >>> 0-1

        RedisValue res9 = db.StreamAdd(
            "race:usa",
            new NameValueEntry[]{
                new NameValueEntry("racer", "Norem")
            },
            "0-2"
        );
        Console.WriteLine(res9);    // >>> 0-2
        // STEP_END

        // Tests for 'xadd_id' step.
        // REMOVE_START
        Assert.Equal(2, db.StreamLength("race:usa"));
        // REMOVE_END


        // STEP_START xadd_bad_id
        try
        {
            RedisValue res10 = db.StreamAdd(
                "race:usa",
                new NameValueEntry[]{
                    new NameValueEntry("racer", "Prickett")
                },
                "0-1"
            );
        }
        catch (RedisServerException ex)
        {
            Console.WriteLine(ex);  // >>> ERR The ID specified in XADD is equal or smaller than the target stream top item
        }
        // STEP_END

        // Tests for 'xadd_bad_id' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START xadd_7
        RedisValue res11 = "";
        Version version = muxer.GetServer("localhost:6379").Version;
        if (version.Major >= 7)
        {
            res11 = db.StreamAdd(
                "race:usa",
                new NameValueEntry[]{
                    new NameValueEntry("rider", "Norem")
                },
                "0-*"
            );

            Console.WriteLine(res11);   // >>> "0-3"
        }
        // STEP_END

        // Tests for 'xadd_7' step.
        // REMOVE_START
        if (version.Major >= 7)
        {
            Assert.Equal("0-3", res11);
        }
        // REMOVE_END


        // STEP_START xrange_all
        StreamEntry[] res12 = db.StreamRange("race:france", "-", "+");

        foreach (StreamEntry entry in res12)
        {
            Console.WriteLine(StreamEntryToString(entry));
        }
        // >>> 1712668482289-0: [rider: Castilla, speed: 30.199999999999999, position: 1, location_id: 1]
        // >>> 1712668766534-1: [rider: Norem, speed: 28.800000000000001, position: 3, location_id: 1]
        // >>> 1712669055705-0: [rider: Prickett, speed: 29.699999999999999, position: 2, location_id: 1]
        // >>> 1712675674750-0: [rider: Castilla, speed: 29.899999999999999, position: 1, location_id: 2]
        // STEP_END

        // Tests for 'xrange_all' step.
        // REMOVE_START
        Assert.Equal(4, res12.Length);
        // REMOVE_END


        // STEP_START xrange_time
        StreamEntry[] res13 = db.StreamRange("race:france", 1712668482289, 1712668482291);

        foreach (StreamEntry entry in res13)
        {
            Console.WriteLine(StreamEntryToString(entry));
        }
        // >>> 1712668482289-0: [rider: Castilla, speed: 30.199999999999999, position: 1, location_id: 1]
        // STEP_END

        // Tests for 'xrange_time' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START xrange_step_1
        StreamEntry[] res14 = db.StreamRange("race:france", "-", "+", 2);

        foreach (StreamEntry entry in res14)
        {
            Console.WriteLine(StreamEntryToString(entry));
        }
        // >>> 1712668482289-0: [rider: Castilla, speed: 30.199999999999999, position: 1, location_id: 1]
        // >>> 1712668766534-1: [rider: Norem, speed: 28.800000000000001, position: 3, location_id: 1]
        // STEP_END

        // Tests for 'xrange_step_1' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START xrange_step_2
        StreamEntry[] res15 = db.StreamRange("race:france", "(1712668766534-1", "+", 2);

        foreach (StreamEntry entry in res15)
        {
            Console.WriteLine(StreamEntryToString(entry));
        }
        // >>> 1712669055705-0: [rider: Prickett, speed: 29.699999999999999, position: 2, location_id: 1]
        // >>> 1712675674750-0: [rider: Castilla, speed: 29.899999999999999, position: 1, location_id: 2]
        // STEP_END

        // Tests for 'xrange_step_2' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START xrange_empty
        StreamEntry[] res16 = db.StreamRange("race:france", "(1712675674750-0", "+", 2);

        foreach (StreamEntry entry in res16)
        {
            Console.WriteLine(StreamEntryToString(entry));
        }
        // >>> <empty array>
        // STEP_END

        // Tests for 'xrange_empty' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START xrevrange
        StreamEntry[] res17 = db.StreamRange("race:france", "+", "-", 1, Order.Descending);

        foreach (StreamEntry entry in res17)
        {
            Console.WriteLine(StreamEntryToString(entry));
        }
        // >>> 1712675674750-0: [rider: Castilla, speed: 29.899999999999999, position: 1, location_id: 2]
        // STEP_END

        // Tests for 'xrevrange' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START xread
        StreamEntry[] res18 = db.StreamRead("race:france", 0, 2);

        foreach (StreamEntry entry in res18)
        {
            Console.WriteLine(StreamEntryToString(entry));
        }
        // >>> 1712668482289-0: [rider: Castilla, speed: 30.199999999999999, position: 1, location_id: 1]
        // >>> 1712668766534-1: [rider: Norem, speed: 28.800000000000001, position: 3, location_id: 1]
        // STEP_END

        // Tests for 'xread' step.
        // REMOVE_START
        Assert.Equal(2, res18.Length);
        // REMOVE_END


        // STEP_START xgroup_create
        bool res19 = db.StreamCreateConsumerGroup("race:france", "france_riders", "$");
        Console.WriteLine(res19);   // >>> true
        // STEP_END

        // Tests for 'xgroup_create' step.
        // REMOVE_START
        Assert.True(res19);
        // REMOVE_END


        // STEP_START xgroup_create_mkstream
        bool res20 = db.StreamCreateConsumerGroup("race:italy", "italy_riders", "$", true);
        Console.WriteLine(res20);   // >>> true
        // STEP_END

        // Tests for 'xgroup_create_mkstream' step.
        // REMOVE_START
        Assert.True(res20);
        // REMOVE_END


        // STEP_START xgroup_read
        RedisValue groupRes = db.StreamAdd(
            "race:italy",
            new NameValueEntry[] { new NameValueEntry("rider", "Castilla") }
        ); // 1712744323758-0

        groupRes = db.StreamAdd(
            "race:italy",
            new NameValueEntry[] { new NameValueEntry("rider", "Royce") }
        ); // 1712744358384-0

        groupRes = db.StreamAdd(
            "race:italy",
            new NameValueEntry[] { new NameValueEntry("rider", "Sam-Bodden") }
        ); // 1712744379676-0

        groupRes = db.StreamAdd(
            "race:italy",
            new NameValueEntry[] { new NameValueEntry("rider", "Prickett") }
        ); // 1712744399401-0

        groupRes = db.StreamAdd(
            "race:italy",
            new NameValueEntry[] { new NameValueEntry("rider", "Norem") }
        ); // 1712744413117-0

        StreamEntry[] res21 = db.StreamReadGroup("race:italy", "italy_riders", "Alice", ">", 1);

        foreach (StreamEntry entry in res21)
        {
            Console.WriteLine(StreamEntryToString(entry));
        }
        // >>> 1712744323758-0: [rider: Castilla]
        // STEP_END

        // Tests for 'xgroup_read' step.
        // REMOVE_START
        Assert.Single(res21);
        // REMOVE_END


        // STEP_START xgroup_read_id
        StreamEntry[] res22 = db.StreamReadGroup("race:italy", "italy_riders", "Alice", "0");

        foreach (StreamEntry entry in res22)
        {
            Console.WriteLine(StreamEntryToString(entry));
            // >>> 1712744323758-0: [rider: Castilla]
        }
        // STEP_END

        // Tests for 'xgroup_read_id' step.
        // REMOVE_START
        Assert.Single(res22);
        // REMOVE_END


        // STEP_START xack
        long res23 = db.StreamAcknowledge("race:italy", "italy_riders", "1712744323758-0");
        Console.WriteLine(res23);   // >>> 1

        StreamEntry[] res24 = db.StreamReadGroup("race:italy", "italy_riders", "Alice", "0");

        foreach (StreamEntry entry in res24)
        {
            Console.WriteLine(StreamEntryToString(entry));
        }
        // >>> <empty array>
        // STEP_END

        // Tests for 'xack' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START xgroup_read_bob
        StreamEntry[] res25 = db.StreamReadGroup("race:italy", "italy_riders", "Bob", ">", 2);

        foreach (StreamEntry entry in res25)
        {
            Console.WriteLine(StreamEntryToString(entry));
        }
        // >>> 1712744358384-0: [rider: Royce]
        // >>> 1712744379676-0: [rider: Sam-Bodden]
        // STEP_END

        // Tests for 'xgroup_read_bob' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START xpending

        string StreamPendingInfoToString(StreamPendingInfo info)
        {
            string[] consumerStrs = new string[info.Consumers.Length];

            for (int i = 0; i < info.Consumers.Length; i++)
            {
                consumerStrs[i] = $"name: {info.Consumers[i].Name}, pending:{info.Consumers[i].PendingMessageCount}";
            }

            return $"pending: {info.PendingMessageCount}, min: {info.LowestPendingMessageId}, max: {info.HighestPendingMessageId}, consumers:[{string.Join(", ", consumerStrs)}]";
        }

        StreamPendingInfo res26 = db.StreamPending("race:italy", "italy_riders");
        Console.WriteLine(StreamPendingInfoToString(res26));
        // >>> pending: 2, min: 1712747506906-0, max: 1712747506907-0, consumers:[name: Bob, pending:2]
        // STEP_END

        // Tests for 'xpending' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START xpending_plus_minus
        string StreamPendingMessageInfoToString(StreamPendingMessageInfo info)
        {
            return $"message_id: {info.MessageId}, consumer: {info.ConsumerName}, time_since_delivered: {info.IdleTimeInMilliseconds}, times_delivered: {info.DeliveryCount}";
        }

        StreamPendingMessageInfo[] res27 = db.StreamPendingMessages(
            "race:italy", "italy_riders", 10, "", "-", "+"
        );

        foreach (StreamPendingMessageInfo info in res27)
        {
            Console.WriteLine(StreamPendingMessageInfoToString(info));
        }
        // >>> message_id: min: 1712747506906-0, consumer: Bob, time_since_delivered: 31084, times_delivered: 1
        // >>> message_id: min: 1712747506907-0, consumer: Bob, time_since_delivered: 31084, times_delivered: 1
        // STEP_END

        // Tests for 'xpending_plus_minus' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START xrange_pending
        StreamEntry[] res28 = db.StreamRange("race:italy", "1712744358384-0", "1712744358384-0");

        foreach (StreamEntry entry in res28)
        {
            Console.WriteLine(StreamEntryToString(entry));
        }
        // >>> 1712744358384-0: [rider: Royce]
        // STEP_END

        // Tests for 'xrange_pending' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START xclaim
        StreamEntry[] res29 = db.StreamClaim(
            "race:italy", "italy_riders", "Alice", 60000, new RedisValue[] { 1712744358384 - 0 }
        );

        foreach (StreamEntry entry in res29)
        {
            Console.WriteLine(StreamEntryToString(entry));
        }
        // >>> 1712744358384-0: [rider: Royce]
        // STEP_END

        // Tests for 'xclaim' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START xautoclaim
        string StreamAutoClaimResultToString(StreamAutoClaimResult result)
        {
            string[] claimedEntryStrings = new string[result.ClaimedEntries.Length];

            for (int i = 0; i < claimedEntryStrings.Length; i++)
            {
                claimedEntryStrings[i] = StreamEntryToString(result.ClaimedEntries[i]);
            }

            return $"{result.NextStartId}, ({string.Join(", ", claimedEntryStrings)})";
        }

        StreamAutoClaimResult res30 = db.StreamAutoClaim(
            "race:italy", "italy_riders", "Alice", 1, "0-0", 1
        );

        Console.WriteLine(StreamAutoClaimResultToString(res30));
        // >>> 1712744379676-0, (1712744358384-0: [rider: Royce])
        // STEP_END

        // Tests for 'xautoclaim' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START xautoclaim_cursor
        StreamAutoClaimResult res31 = db.StreamAutoClaim(
            "race:italy", "italy_riders", "Alice", 1, "(1712744358384-0", 1
        );

        Console.WriteLine(StreamAutoClaimResultToString(res31));
        // >>> 0-0, (1712744379676-0: [rider: Sam-Bodden])
        // STEP_END

        // Tests for 'xautoclaim_cursor' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START xinfo
        string StreamInfoToString(StreamInfo info)
        {
            return $"length: {info.Length}, radix-tree-keys: {info.RadixTreeKeys}, radix-tree-nodes: {info.RadixTreeNodes}, last-generated-id: {info.LastGeneratedId}, first-entry: {StreamEntryToString(info.FirstEntry)}, last-entry: {StreamEntryToString(info.LastEntry)}";
        }

        StreamInfo res32 = db.StreamInfo("race:italy");
        Console.WriteLine(StreamInfoToString(res32));
        // >>> length: 5, radix-tree-keys: 1, radix-tree-nodes: 2, last-generated-id: 1712756762686-1, first-entry: 1712756762685-0: [rider: Castilla], last-entry: 1712756762686-1: [rider: Norem]
        // STEP_END

        // Tests for 'xinfo' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START xinfo_groups
        string StreamGroupInfoToString(StreamGroupInfo info)
        {
            return $"name: {info.Name}, consumers: {info.ConsumerCount}, pending: {info.PendingMessageCount}, last-delivered-id: {info.LastDeliveredId}";
        }

        StreamGroupInfo[] res33 = db.StreamGroupInfo("race:italy");

        foreach (StreamGroupInfo info in res33)
        {
            Console.WriteLine(StreamGroupInfoToString(info));
        }
        // >>> name: italy_riders, consumers: 2, pending: 2, last-delivered-id: 1712757192730-2
        // STEP_END

        // Tests for 'xinfo_groups' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START xinfo_consumers
        string StreamConsumerInfoToString(StreamConsumerInfo info)
        {
            return $"name: {info.Name}, pending: {info.PendingMessageCount}, idle: {info.IdleTimeInMilliseconds}";
        }

        StreamConsumerInfo[] res34 = db.StreamConsumerInfo("race:italy", "italy_riders");

        foreach (StreamConsumerInfo info in res34)
        {
            Console.WriteLine(StreamConsumerInfoToString(info));
        }
        // >>> name: Alice, pending: 1, idle: 7717
        // >>> name: Bob, pending: 0, idle: 7722
        // STEP_END

        // Tests for 'xinfo_consumers' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START maxlen
        db.StreamAdd(
            "race:italy", new NameValueEntry[] { new NameValueEntry("rider", "Jones") }, null, 2, true
        );

        db.StreamAdd(
            "race:italy", new NameValueEntry[] { new NameValueEntry("rider", "Wood") }, null, 2, true
        );

        db.StreamAdd(
            "race:italy", new NameValueEntry[] { new NameValueEntry("rider", "Henshaw") }, null, 2, true
        );

        long res35 = db.StreamLength("race:italy");
        Console.WriteLine(res35); // >>> 8

        StreamEntry[] res36 = db.StreamRange("race:italy", "-", "+");

        foreach (StreamEntry entry in res36)
        {
            Console.WriteLine(StreamEntryToString(entry));
        }
        // >>> 1712758336128-0: [rider: Castilla]
        // >>> 1712758336128-1: [rider: Royce]
        // >>> 1712758336128-2: [rider: Sam-Bodden]
        // >>> 1712758336129-0: [rider: Prickett]
        // >>> 1712758336139-0: [rider: Norem]
        // >>> 1712758340854-0: [rider: Jones]
        // >>> 1712758341645-0: [rider: Wood]
        // >>> 1712758342134-0: [rider: Henshaw]

        db.StreamAdd(
            "race:italy", new NameValueEntry[] { new NameValueEntry("rider", "Smith") }, null, 2, false
        );

        StreamEntry[] res37 = db.StreamRange("race:italy", "-", "+");

        foreach (StreamEntry entry in res37)
        {
            Console.WriteLine(StreamEntryToString(entry));
        }
        // 1712758746476-1: [rider: Henshaw]
        // 1712758746477-0: [rider: Smith]
        // STEP_END

        // Tests for 'maxlen' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START xtrim
        long res38 = db.StreamTrim("race:italy", 10, false);
        Console.WriteLine(res38);   // >>> 0
        // STEP_END

        // Tests for 'xtrim' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START xtrim2 
        long res39 = db.StreamTrim("race:italy", 10, true);
        Console.WriteLine(res39);   // >>> 0
        // STEP_END

        // Tests for 'xtrim2' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START xdel
        StreamEntry[] res40 = db.StreamRange("race:italy", "-", "+");

        foreach (StreamEntry entry in res40)
        {
            Console.WriteLine(StreamEntryToString(entry));
        }
        // >>> 1712759694003-0: [rider: Henshaw]
        // >>> 1712759694003-1: [rider: Smith]

        long res41 = db.StreamDelete("race:italy", new RedisValue[] { "1712759694003-1" });
        Console.WriteLine(res41);   // >>> 1

        StreamEntry[] res42 = db.StreamRange("race:italy", "-", "+");

        foreach (StreamEntry entry in res42)
        {
            Console.WriteLine(StreamEntryToString(entry));
        }
        // >>> 1712759694003-0: [rider: Henshaw]
        // STEP_END

        // Tests for 'xdel' step.
        // REMOVE_START

        // REMOVE_END


        // HIDE_START
    }
}
// HIDE_END

