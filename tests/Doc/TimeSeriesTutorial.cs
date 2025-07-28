// EXAMPLE: time_series_tutorial
// HIDE_START
/*
Code samples for time series page:
    https://redis.io/docs/latest/develop/data-types/timeseries/
*/
using NRedisStack.DataTypes;
using NRedisStack.Literals.Enums;
using StackExchange.Redis;
// HIDE_END

// REMOVE_START
using NRedisStack.Tests;
using NRedisStack.RedisStackCommands;
using NRedisStack;

namespace Doc;
[Collection("DocsTests")]
// REMOVE_END

public class TimeSeriesTutorial
// REMOVE_START
: AbstractNRedisStackTest, IDisposable
// REMOVE_END
{
    // REMOVE_START
    public TimeSeriesTutorial(EndpointsFixture fixture) : base(fixture) { }

    [SkippableFact]
    // REMOVE_END
    public void run()
    {
        // REMOVE_START
        // This is needed because we're constructing ConfigurationOptions in the test before calling GetConnection
        SkipIfTargetConnectionDoesNotExist(EndpointsFixture.Env.Standalone);
        var _ = GetCleanDatabase(EndpointsFixture.Env.Standalone);
        // REMOVE_END
        // HIDE_START
        var muxer = ConnectionMultiplexer.Connect("localhost:6379");
        var db = muxer.GetDatabase();
        // HIDE_END

        // REMOVE_START
        // Clear any keys here before using them in tests.
        db.KeyDelete(new RedisKey[] {
            "thermometer:1", "thermometer:2", "thermometer:3",
            "rg:1", "rg:2", "rg:3", "rg:4",
            "sensor3",
            "wind:1", "wind:2", "wind:3", "wind:4",
            "hyg:1", "hyg:compacted"
        });
        // REMOVE_END

        // STEP_START create
        bool res1 = db.TS().Create(
            "thermometer:1",
            new TsCreateParamsBuilder().build()
        );
        Console.WriteLine(res1); // >>> True

        RedisType res2 = db.KeyType("thermometer:1");
        Console.WriteLine(res2); // >>> TSDB-TYPE

        TimeSeriesInformation res3 = db.TS().Info("thermometer:1");
        Console.WriteLine(res3.TotalSamples); // >>> 0
        // STEP_END
        // REMOVE_START
        Assert.True(res1);
        Assert.Equal(RedisType.Unknown, res2);
        Assert.Equal(0, res3.TotalSamples);
        // REMOVE_END

        // STEP_START create_retention
        long res4 = db.TS().Add(
            "thermometer:2",
            new TsAddParamsBuilder()
                .AddTimestamp(1)
                .AddValue(10.8)
                .AddRetentionTime(100)
                .build()
        );
        Console.WriteLine(res4); // >>> 1

        TimeSeriesInformation res5 = db.TS().Info("thermometer:2");
        Console.WriteLine(res5.RetentionTime);
        // >>> 100
        // STEP_END
        // REMOVE_START
        Assert.Equal(1, res4);
        Assert.Equal(100, res5.RetentionTime);
        // REMOVE_END

        // STEP_START create_labels
        var labels = new List<TimeSeriesLabel>
        {
            new TimeSeriesLabel("location", "UK"),
            new TimeSeriesLabel("type", "Mercury")
        };
        long res6 = db.TS().Add(
            "thermometer:3",
            new TsAddParamsBuilder()
                .AddTimestamp(1)
                .AddValue(10.4)
                .AddLabels(labels)
                .build()
        );
        Console.WriteLine(res6); // >>> 1

        TimeSeriesInformation res7 = db.TS().Info("thermometer:3");
        Console.WriteLine(
            $"Labels: {string.Join(", ", res7.Labels!.Select(l => $"{l.Key}={l.Value}"))}"
        );
        // >>> Labels: location=UK, type=Mercury
        // STEP_END
        // REMOVE_START
        Assert.Equal(1, res6);
        Assert.Equal(2, res7.Labels!.Count);
        Assert.Contains(res7.Labels, l => l.Key == "location" && l.Value == "UK");
        Assert.Contains(res7.Labels, l => l.Key == "type" && l.Value == "Mercury");
        // REMOVE_END

        // STEP_START madd
        var sequence = new List<(string, TimeStamp, double)>
        {
            ("thermometer:1", 1, 9.2),
            ("thermometer:1", 2, 9.9),
            ("thermometer:2", 2, 10.3)
        };
        IReadOnlyList<TimeStamp> res8 = db.TS().MAdd(sequence);
        Console.WriteLine($"[{string.Join(", ", res8.Select(t => t.Value))}]");
        // >>> [1, 2, 2]
        // STEP_END
        // REMOVE_START
        Assert.Equal(3, res8.Count);
        Assert.Equal(1, (long)res8[0]);
        Assert.Equal(2, (long)res8[1]);
        Assert.Equal(2, (long)res8[2]);
        // REMOVE_END

        // STEP_START get
        // The last recorded temperature for thermometer:2
        // was 10.3 at time 2.
        TimeSeriesTuple? res9 = db.TS().Get("thermometer:2");
        Console.WriteLine($"({res9!.Time.Value}, {res9.Val})");
        // >>> (2, 10.3)
        // STEP_END
        // REMOVE_START
        Assert.NotNull(res9);
        Assert.Equal(2, (long)res9.Time);
        Assert.Equal(10.3, res9.Val);
        // REMOVE_END

        // STEP_START range
        // Add 5 data points to a time series named "rg:1".
        bool res10 = db.TS().Create(
            "rg:1",
            new TsCreateParamsBuilder().build()
        );
        Console.WriteLine(res10); // >>> True

        var sequence2 = new List<(string, TimeStamp, double)>
        {
            ("rg:1", 0, 18),
            ("rg:1", 1, 14),
            ("rg:1", 2, 22),
            ("rg:1", 3, 18),
            ("rg:1", 4, 24)
        };
        IReadOnlyList<TimeStamp> res11 = db.TS().MAdd(sequence2);
        Console.WriteLine(
            $"[{string.Join(", ", res11.Select(t => t.Value))}]"
        );
        // >>> [0, 1, 2, 3, 4]

        // Retrieve all the data points in ascending order.
        IReadOnlyList<TimeSeriesTuple> res12 = db.TS().Range("rg:1", "-", "+");
        Console.WriteLine(
            $"[{string.Join(", ", res12.Select(t => $"({t.Time.Value}, {t.Val})"))}]"
        );
        // >>> [(0, 18), (1, 14), (2, 22), (3, 18), (4, 24)]

        // Retrieve data points up to time 1 (inclusive).
        IReadOnlyList<TimeSeriesTuple> res13 = db.TS().Range("rg:1", "-", 1);
        Console.WriteLine(
            $"[{string.Join(", ", res13.Select(t => $"({t.Time.Value}, {t.Val})"))}]"
        );
        // >>> [(0, 18), (1, 14)]

        // Retrieve data points from time 3 onwards.
        IReadOnlyList<TimeSeriesTuple> res14 = db.TS().Range("rg:1", 3, "+");
        Console.WriteLine(
            $"[{string.Join(", ", res14.Select(t => $"({t.Time.Value}, {t.Val})"))}]"
        );
        // >>> [(3, 18), (4, 24)]

        // Retrieve all the data points in descending order.
        IReadOnlyList<TimeSeriesTuple> res15 = db.TS().RevRange("rg:1", "-", "+");
        Console.WriteLine(
            $"[{string.Join(", ", res15.Select(t => $"({t.Time.Value}, {t.Val})"))}]"
        );
        // >>> [(4, 24), (3, 18), (2, 22), (1, 14), (0, 18)]

        // Retrieve data points up to time 1 (inclusive), but return them
        // in descending order.
        IReadOnlyList<TimeSeriesTuple> res16 = db.TS().RevRange("rg:1", "-", 1);
        Console.WriteLine(
            $"[{string.Join(", ", res16.Select(t => $"({t.Time.Value}, {t.Val})"))}]"
        );
        // >>> [(1, 14), (0, 18)]
        // STEP_END
        // REMOVE_START
        Assert.True(res10);
        Assert.Equal(5, res11.Count);
        Assert.Equal(5, res12.Count);
        Assert.Equal(2, res13.Count);
        Assert.Equal(2, res14.Count);
        Assert.Equal(5, res15.Count);
        Assert.Equal(2, res16.Count);
        // REMOVE_END

        // STEP_START range_filter
        var filterByTs = new List<TimeStamp> { 0, 2, 4 };
        IReadOnlyList<TimeSeriesTuple> res17 = db.TS().Range(
            "rg:1", "-", "+", filterByTs: filterByTs
        );
        Console.WriteLine(
            $"[{string.Join(", ", res17.Select(t => $"({t.Time.Value}, {t.Val})"))}]"
        );
        // >>> [(0, 18), (2, 22), (4, 24)]

        IReadOnlyList<TimeSeriesTuple> res18 = db.TS().RevRange(
            "rg:1", "-", "+",
            filterByTs: filterByTs,
            filterByValue: (20, 25)
        );
        Console.WriteLine(
            $"[{string.Join(", ", res18.Select(t => $"({t.Time.Value}, {t.Val})"))}]"
        );
        // >>> [(4, 24), (2, 22)]

        IReadOnlyList<TimeSeriesTuple> res19 = db.TS().RevRange(
            "rg:1", "-", "+",
            filterByTs: filterByTs,
            filterByValue: (22, 22),
            count: 1
        );
        Console.WriteLine(
            $"[{string.Join(", ", res19.Select(t => $"({t.Time.Value}, {t.Val})"))}]"
        );
        // >>> [(2, 22)]
        // STEP_END
        // REMOVE_START
        Assert.Equal(3, res17.Count);
        Assert.Equal(2, res18.Count);
        Assert.Single(res19);
        // REMOVE_END

        // STEP_START query_multi
        // Create three new "rg:" time series (two in the US
        // and one in the UK, with different units) and add some
        // data points.
        var labels2 = new List<TimeSeriesLabel>
        {
            new TimeSeriesLabel("location", "us"),
            new TimeSeriesLabel("unit", "cm")
        };
        bool res20 = db.TS().Create(
            "rg:2",
            new TsCreateParamsBuilder()
                .AddLabels(labels2)
                .build()
        );
        Console.WriteLine(res20); // >>> True

        var labels3 = new List<TimeSeriesLabel>
        {
            new TimeSeriesLabel("location", "us"),
            new TimeSeriesLabel("unit", "in")
        };

        bool res21 = db.TS().Create(
            "rg:3",
            new TsCreateParamsBuilder()
                .AddLabels(labels3)
                .build()
        );
        Console.WriteLine(res21); // >>> True

        var labels4 = new List<TimeSeriesLabel>
        {
            new TimeSeriesLabel("location", "uk"),
            new TimeSeriesLabel("unit", "mm")
        };
        bool res22 = db.TS().Create(
            "rg:4",
            new TsCreateParamsBuilder()
                .AddLabels(labels4)
                .build()
        );
        Console.WriteLine(res22); // >>> True

        var sequence3 = new List<(string, TimeStamp, double)>
        {
            ("rg:2", 0, 1.8),
            ("rg:3", 0, 0.9),
            ("rg:4", 0, 25)
        };
        IReadOnlyList<TimeStamp> res23 = db.TS().MAdd(sequence3);
        Console.WriteLine(
            $"[{string.Join(", ", res23.Select(t => t.Value))}]"
        );
        // >>> [0, 0, 0]

        var sequence4 = new List<(string, TimeStamp, double)>
        {
            ("rg:2", 1, 2.1),
            ("rg:3", 1, 0.77),
            ("rg:4", 1, 18)
        };

        IReadOnlyList<TimeStamp> res24 = db.TS().MAdd(sequence4);
        Console.WriteLine(
            $"[{string.Join(", ", res24.Select(t => t.Value))}]"
        );
        // >>> [1, 1, 1]

        var sequence5 = new List<(string, TimeStamp, double)>
        {
            ("rg:2", 2, 2.3),
            ("rg:3", 2, 1.1),
            ("rg:4", 2, 21)
        };

        IReadOnlyList<TimeStamp> res25 = db.TS().MAdd(sequence5);
        Console.WriteLine(
            $"[{string.Join(", ", res25.Select(t => t.Value))}]"
        );
        // >>> [2, 2, 2]

        var sequence6 = new List<(string, TimeStamp, double)>
        {
            ("rg:2", 3, 1.9),
            ("rg:3", 3, 0.81),
            ("rg:4", 3, 19)
        };

        IReadOnlyList<TimeStamp> res26 = db.TS().MAdd(sequence6);
        Console.WriteLine(
            $"[{string.Join(", ", res26.Select(t => t.Value))}]"
        );
        // >>> [3, 3, 3]

        var sequence7 = new List<(string, TimeStamp, double)>
        {
            ("rg:2", 4, 1.78),
            ("rg:3", 4, 0.74),
            ("rg:4", 4, 23)
        };
        IReadOnlyList<TimeStamp> res27 = db.TS().MAdd(sequence7);
        Console.WriteLine(
            $"[{string.Join(", ", res27.Select(t => t.Value))}]"
        );
        // >>> [4, 4, 4]

        // Retrieve the last data point from each US time series. If
        // you don't specify any labels, an empty array is returned
        // for the labels.
        var filters = new List<string> { "location=us" };
        var res28 = db.TS().MGet(filters);
        Console.WriteLine(res28.Count); // >>> 2

        foreach (var (key, labels_result, value) in res28)
        {
            Console.WriteLine($"{key}: ({value.Time.Value}, {value.Val})");
        }
        // >>> rg:2: (4, 1.78)
        // >>> rg:3: (4, 0.74)

        // Retrieve the same data points, but include the `unit`
        // label in the results.
        var selectUnitLabel = new List<string> { "unit" };

        var res29 = db.TS().MGet(
            filters,
            selectedLabels: selectUnitLabel
        );
        Console.WriteLine(res29.Count); // >>> 2

        foreach (var (key, labels_result, value) in res29)
        {
            var unitLabel = labels_result.FirstOrDefault(l => l.Key == "unit");
            Console.WriteLine($"{key} (unit: {unitLabel?.Value}): ({value.Time.Value}, {value.Val})");
        }
        // >>> rg:2 (unit: cm): (4, 1.78)
        // >>> rg:3 (unit: in): (4, 0.74)

        // Retrieve data points up to time 2 (inclusive) from all
        // time series that use millimeters as the unit. Include all
        // labels in the results.
        var mmFilters = new List<string> { "unit=mm" };

        IReadOnlyList<
            (string, IReadOnlyList<TimeSeriesLabel>, IReadOnlyList<TimeSeriesTuple>)
        > res30 = db.TS().MRange(
            "-", 2, mmFilters, withLabels: true
        );
        Console.WriteLine(res30.Count); // >>> 1

        foreach (var (key, labels_result, values) in res30)
        {
            Console.WriteLine($"{key}:");
            Console.WriteLine($"  Labels: ({string.Join(", ", labels_result.Select(l => $"{l.Key}={l.Value}"))})");
            Console.WriteLine($"  Values: [{string.Join(", ", values.Select(t => $"({t.Time.Value}, {t.Val})"))}]");
        }
        // >>> rg:4:
        // >>>   Labels:location=uk,unit=mm
        // >>>   Values: [(1, 23), (2, 21), (3, 19)]

        // Retrieve data points from time 1 to time 3 (inclusive) from
        // all time series that use centimeters or millimeters as the unit,
        // but only return the `location` label. Return the results
        // in descending order of timestamp.
        var cmMmFilters = new List<string> { "unit=(cm,mm)" };
        var locationLabels = new List<string> { "location" };
        IReadOnlyList<
            (string, IReadOnlyList<TimeSeriesLabel>, IReadOnlyList<TimeSeriesTuple>)
        > res31 = db.TS().MRevRange(
            1, 3, cmMmFilters, selectLabels: locationLabels
        );
        Console.WriteLine(res31.Count); // >>> 2

        foreach (var (key, labels_result, values) in res31)
        {
            var locationLabel = labels_result.FirstOrDefault(l => l.Key == "location");
            Console.WriteLine($"{key} (location: {locationLabel?.Value})");
            Console.WriteLine($"  Values: [{string.Join(", ", values.Select(t => $"({t.Time.Value}, {t.Val})"))}]");
        }
        // >>> rg:4 (location: uk)
        // >>>   Values: [(3, 19), (2, 21), (1, 23)]
        // >>> rg:2 (location: us)
        // >>>   Values: [(3, 2.3), (2, 2.1), (1, 1.8)]
        // STEP_END
        // REMOVE_START
        Assert.True(res20);
        Assert.True(res21);
        Assert.True(res22);
        Assert.Equal(3, res23.Count);
        Assert.Equal(3, res24.Count);
        Assert.Equal(3, res25.Count);
        Assert.Equal(3, res26.Count);
        Assert.Equal(3, res27.Count);
        Assert.Equal(2, res28.Count);
        Assert.Equal(2, res29.Count);
        Assert.Single(res30);
        Assert.Equal(2, res31.Count);
        // REMOVE_END

        // STEP_START agg
        IReadOnlyList<TimeSeriesTuple> res32 = db.TS().Range(
            "rg:2", "-", "+",
            aggregation: TsAggregation.Avg,
            timeBucket: 2
        );
        Console.WriteLine($"[{string.Join(", ", res32.Select(t => $"({t.Time.Value}, {t.Val})"))}]");
        // >>> [(0, 1.95), (2, 2.1), (4, 1.78)]
        // STEP_END
        // REMOVE_START
        Assert.Equal(3, res32.Count);
        // REMOVE_END

        // STEP_START agg_bucket
        bool res33 = db.TS().Create(
            "sensor3",
            new TsCreateParamsBuilder()
                .build()
        );
        Console.WriteLine(res33); // >>> True

        var sensorSequence = new List<(string, TimeStamp, double)>
        {
            ("sensor3", 10, 1000),
            ("sensor3", 20, 2000),
            ("sensor3", 30, 3000),
            ("sensor3", 40, 4000),
            ("sensor3", 50, 5000),
            ("sensor3", 60, 6000),
            ("sensor3", 70, 7000)
        };
        IReadOnlyList<TimeStamp> res34 = db.TS().MAdd(sensorSequence);
        Console.WriteLine($"[{string.Join(", ", res34.Select(t => t.Value))}]");
        // >>> [10, 20, 30, 40, 50, 60, 70]

        IReadOnlyList<TimeSeriesTuple> res35 = db.TS().Range(
            "sensor3", 10, 70,
            aggregation: TsAggregation.Min,
            timeBucket: 25
        );
        Console.WriteLine($"[{string.Join(", ", res35.Select(t => $"({t.Time.Value}, {t.Val})"))}]");
        // >>> [(0, 1000), (25, 3000), (50, 5000)]
        // STEP_END
        // REMOVE_START
        Assert.True(res33);
        Assert.Equal(7, res34.Count);
        Assert.Equal(3, res35.Count);
        // REMOVE_END

        // STEP_START agg_align
        IReadOnlyList<TimeSeriesTuple> res36 = db.TS().Range(
            "sensor3", 10, 70,
            aggregation: TsAggregation.Min,
            timeBucket: 25,
            align: "-"
        );
        Console.WriteLine(
            $"[{string.Join(", ", res36.Select(t => $"({t.Time.Value}, {t.Val})"))}]"
        );
        // >>> [(10, 1000), (35, 4000), (60, 6000)]
        // STEP_END
        // REMOVE_START
        Assert.Equal(3, res36.Count);
        // REMOVE_END

        // STEP_START agg_multi
        var ukLabels = new List<TimeSeriesLabel> { new TimeSeriesLabel("country", "uk") };

        bool res37 = db.TS().Create(
            "wind:1",
            new TsCreateParamsBuilder()
                .AddLabels(ukLabels)
                .build()
        );
        Console.WriteLine(res37); // >>> True

        bool res38 = db.TS().Create(
            "wind:2",
            new TsCreateParamsBuilder()
                .AddLabels(ukLabels)
                .build()
        );
        Console.WriteLine(res38); // >>> True

        var usLabels = new List<TimeSeriesLabel> { new TimeSeriesLabel("country", "us") };
        bool res39 = db.TS().Create(
            "wind:3",
            new TsCreateParamsBuilder()
                .AddLabels(usLabels)
                .build()
        );
        Console.WriteLine(res39); // >>> True

        bool res40 = db.TS().Create(
            "wind:4",
            new TsCreateParamsBuilder()
                .AddLabels(usLabels)
                .build()
        );
        Console.WriteLine(res40); // >>> True

        var windSequence1 = new List<(string, TimeStamp, double)>
        {
            ("wind:1", 1, 12),
            ("wind:2", 1, 18),
            ("wind:3", 1, 5),
            ("wind:4", 1, 20)
        };
        IReadOnlyList<TimeStamp> res41 = db.TS().MAdd(windSequence1);
        Console.WriteLine(
            $"[{string.Join(", ", res41.Select(t => t.Value))}]"
        );
        // >>> [1, 1, 1, 1]

        var windSequence2 = new List<(string, TimeStamp, double)>
        {
            ("wind:1", 2, 14),
            ("wind:2", 2, 21),
            ("wind:3", 2, 4),
            ("wind:4", 2, 25)
        };
        IReadOnlyList<TimeStamp> res42 = db.TS().MAdd(windSequence2);
        Console.WriteLine(
            $"[{string.Join(", ", res42.Select(t => t.Value))}]"
        );
        // >>> [2, 2, 2, 2]

        var windSequence3 = new List<(string, TimeStamp, double)>
        {
            ("wind:1", 3, 10),
            ("wind:2", 3, 24),
            ("wind:3", 3, 8),
            ("wind:4", 3, 18)
        };
        IReadOnlyList<TimeStamp> res43 = db.TS().MAdd(windSequence3);
        Console.WriteLine(
            $"[{string.Join(", ", res43.Select(t => t.Value))}]"
        );
        // >>> [3, 3, 3, 3]

        // The result pairs contain the timestamp and the maximum sample value
        // for the country at that timestamp.
        var countryFilters = new List<string> { "country=(us,uk)" };
        IReadOnlyList<
            (string, IReadOnlyList<TimeSeriesLabel>, IReadOnlyList<TimeSeriesTuple>)
        > res44 = db.TS().MRange(
            "-", "+",
            countryFilters,
            groupbyTuple: ("country", TsReduce.Max)
        );
        Console.WriteLine(res44.Count); // >>> 2

        foreach (var (key, labels_result, values) in res44)
        {
            Console.WriteLine($"{key}:");
            Console.WriteLine($"  Values: [{string.Join(", ", values.Select(t => $"({t.Time.Value}, {t.Val})"))}]");
        }
        // >>> country=uk
        // >>>   Values: [(1, 18), (2, 21), (3, 24)]
        // >>> country=us
        // >>>   Values: [(1, 20), (2, 25), (3, 18)]

        // The result pairs contain the timestamp and the average sample value
        // for the country at that timestamp.
        IReadOnlyList<
            (string, IReadOnlyList<TimeSeriesLabel>, IReadOnlyList<TimeSeriesTuple>)
        > res45 = db.TS().MRange(
            "-", "+",
            countryFilters,
            groupbyTuple: ("country", TsReduce.Avg)
        );
        Console.WriteLine(res45.Count); // >>> 2

        foreach (var (key, labels_result, values) in res45)
        {
            Console.WriteLine($"{key}:");
            Console.WriteLine($"  Values: [{string.Join(", ", values.Select(t => $"({t.Time.Value}, {t.Val})"))}]");
        }
        // >>>   country=uk
        // >>>      Values: [(1, 14), (2, 18), (3, 10)]
        // >>>   country=us
        // >>>      Values: [(1, 16), (2, 22), (3, 14)]
        // STEP_END
        // REMOVE_START
        Assert.True(res37);
        Assert.True(res38);
        Assert.True(res39);
        Assert.True(res40);
        Assert.Equal(4, res41.Count);
        Assert.Equal(4, res42.Count);
        Assert.Equal(4, res43.Count);
        Assert.Equal(2, res44.Count);
        Assert.Equal(2, res45.Count);
        // REMOVE_END

        // STEP_START create_compaction
        bool res46 = db.TS().Create(
            "hyg:1",
            new TsCreateParamsBuilder().build()
        );
        Console.WriteLine(res46); // >>> True

        bool res47 = db.TS().Create(
            "hyg:compacted",
            new TsCreateParamsBuilder().build()
        );
        Console.WriteLine(res47); // >>> True

        var compactionRule = new TimeSeriesRule("hyg:compacted", 3, TsAggregation.Min);
        bool res48 = db.TS().CreateRule("hyg:1", compactionRule);
        Console.WriteLine(res48); // >>> True

        TimeSeriesInformation res49 = db.TS().Info("hyg:1");
        Console.WriteLine(res49.Rules!.Count);
        // >>> 1

        TimeSeriesInformation res50 = db.TS().Info("hyg:compacted");
        Console.WriteLine(res50.SourceKey);
        // >>> hyg:1
        // STEP_END
        // REMOVE_START
        Assert.True(res46);
        Assert.True(res47);
        Assert.True(res48);
        Assert.Single(res49.Rules!);
        Assert.Equal("hyg:1", res50.SourceKey);
        // REMOVE_END

        // STEP_START comp_add
        var hygSequence1 = new List<(string, TimeStamp, double)>
        {
            ("hyg:1", 0, 75),
            ("hyg:1", 1, 77),
            ("hyg:1", 2, 78)
        };
        IReadOnlyList<TimeStamp> res51 = db.TS().MAdd(hygSequence1);
        Console.WriteLine($"[{string.Join(", ", res51.Select(t => t.Value))}]");
        // >>> [0, 1, 2]

        IReadOnlyList<TimeSeriesTuple> res52 = db.TS().Range("hyg:compacted", "-", "+");
        Console.WriteLine(res52.Count); // >>> 0

        TimeStamp res53 = db.TS().Add(
            "hyg:1",
            new TsAddParamsBuilder()
                .AddTimestamp(3)
                .AddValue(79)
                .build()
        );
        Console.WriteLine(res53.Value); // >>> 3

        IReadOnlyList<TimeSeriesTuple> res54 = db.TS().Range("hyg:compacted", "-", "+");
        Console.WriteLine(
            $"[{string.Join(", ", res54.Select(t => $"({t.Time.Value}, {t.Val})"))}]"
        );
        // >>> [(0, 75)]
        // STEP_END
        // REMOVE_START
        Assert.Equal(3, res51.Count);
        Assert.Empty(res52);
        Assert.Equal(3, (long)res53);
        Assert.Single(res54);
        // REMOVE_END

        // STEP_START del
        TimeSeriesInformation res55 = db.TS().Info("thermometer:1");
        Console.WriteLine(res55.TotalSamples); // >>> 2
        Console.WriteLine(res55.FirstTimeStamp!); // >>> 1
        Console.WriteLine(res55.LastTimeStamp!); // >>> 2

        TimeStamp res56 = db.TS().Add(
            "thermometer:1",
            new TsAddParamsBuilder()
                .AddTimestamp(3)
                .AddValue(9.7)
                .build()
        );
        Console.WriteLine(res56.Value); // >>> 3

        TimeSeriesInformation res57 = db.TS().Info("thermometer:1");
        Console.WriteLine(res57.TotalSamples); // >>> 3
        Console.WriteLine(res57.FirstTimeStamp!); // >>> 1
        Console.WriteLine(res57.LastTimeStamp!); // >>> 3

        long res58 = db.TS().Del("thermometer:1", 1, 2);
        Console.WriteLine(res58); // >>> 2

        TimeSeriesInformation res59 = db.TS().Info("thermometer:1");
        Console.WriteLine(res59.TotalSamples); // >>> 1
        Console.WriteLine(res59.FirstTimeStamp!); // >>> 3
        Console.WriteLine(res59.LastTimeStamp!); // >>> 3

        long res60 = db.TS().Del("thermometer:1", 3, 3);
        Console.WriteLine(res60); // >>> 1

        TimeSeriesInformation res61 = db.TS().Info("thermometer:1");
        Console.WriteLine(res61.TotalSamples); // >>> 0
        // STEP_END
        // REMOVE_START
        Assert.Equal(2, res55.TotalSamples);
        Assert.Equal(1, (long)res55.FirstTimeStamp!);
        Assert.Equal(2, (long)res55.LastTimeStamp!);
        Assert.Equal(3, (long)res56);
        Assert.Equal(3, res57.TotalSamples);
        Assert.Equal(1, (long)res57.FirstTimeStamp!);
        Assert.Equal(3, (long)res57.LastTimeStamp!);
        Assert.Equal(2, res58);
        Assert.Equal(1, res59.TotalSamples);
        Assert.Equal(3, (long)res59.FirstTimeStamp!);
        Assert.Equal(3, (long)res59.LastTimeStamp!);
        Assert.Equal(1, res60);
        Assert.Equal(0, res61.TotalSamples);
        // REMOVE_END
        // HIDE_START
    }
}
// HIDE_END
