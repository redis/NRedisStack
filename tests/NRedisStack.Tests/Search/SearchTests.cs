#pragma  warning disable CS0618, CS0612 // allow testing obsolete methods
using Xunit;
using StackExchange.Redis;
using NRedisStack.RedisStackCommands;
using NRedisStack.Search;
using static NRedisStack.Search.Schema;
using NRedisStack.Search.Aggregation;
using NRedisStack.Search.Literals.Enums;
using System.Runtime.InteropServices;
using NetTopologySuite.IO;
using NetTopologySuite.Geometries;
using Xunit.Abstractions;

namespace NRedisStack.Tests.Search;

public class SearchTests(EndpointsFixture endpointsFixture, ITestOutputHelper log)
    : AbstractNRedisStackTest(endpointsFixture, log), IDisposable
{
    // private readonly string key = "SEARCH_TESTS";
    private readonly string index = "TEST_INDEX";

    private void AddDocument(IDatabase db, Document doc)
    {
        var hash = doc.GetProperties()
            .Select(pair => new HashEntry(pair.Key, pair.Value))
            .ToArray();
        db.HashSet(doc.Id, hash);
    }

    private void AddDocument(IDatabase db, string key, Dictionary<string, object> objDictionary)
    {
        Dictionary<string, string> strDictionary = new();
        var hash = objDictionary
            .Select(pair => new HashEntry(pair.Key, pair.Value switch
            {
                string s => (RedisValue)s,
                byte[] b => b,
                int i => i,
                long l => l,
                double d => d,
                _ => throw new ArgumentException($"Unsupported type: {pair.Value.GetType()}"),
            }))
            .ToArray();
        db.HashSet(key, hash);
    }

    private void AssertDatabaseSize(IDatabase db, int expected)
    {
        // in part, this is to allow replication to catch up
        for (int i = 0; i < 10; i++)
        {
            Assert.Equal(expected, DatabaseSize(db));
        }
    }

    private async Task AssertDatabaseSizeAsync(IDatabase db, int expected)
    {
        // in part, this is to allow replication to catch up
        for (int i = 0; i < 10; i++)
        {
            Assert.Equal(expected, await DatabaseSizeAsync(db));
        }
    }

    [SkipIfRedisTheory(Is.Enterprise)]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestAggregationRequestVerbatim(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new();
        sc.AddTextField("name", 1.0, sortable: true);
        ft.Create(index, FTCreateParams.CreateParams(), sc);
        AddDocument(db, new Document("data1").Set("name", "hello kitty"));

        AggregationRequest r = new("kitti");

        AggregationResult res = ft.Aggregate(index, r);
        Assert.Equal(1, res.TotalResults);

        r = new AggregationRequest("kitti")
                .Verbatim();

        res = ft.Aggregate(index, r);
        Assert.Equal(0, res.TotalResults);
    }

    [SkipIfRedisTheory(Is.Enterprise)]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestAggregationRequestVerbatimAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new();
        sc.AddTextField("name", 1.0, sortable: true);
        ft.Create(index, FTCreateParams.CreateParams(), sc);
        AddDocument(db, new Document("data1").Set("name", "hello kitty"));

        AggregationRequest r = new("kitti");

        AggregationResult res = await ft.AggregateAsync(index, r);
        Assert.Equal(1, res.TotalResults);

        r = new AggregationRequest("kitti")
                .Verbatim();

        res = await ft.AggregateAsync(index, r);
        Assert.Equal(0, res.TotalResults);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestAggregationRequestTimeout(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new();
        sc.AddTextField("name", 1.0, sortable: true);
        sc.AddNumericField("count", sortable: true);
        ft.Create(index, FTCreateParams.CreateParams(), sc);
        AddDocument(db, new Document("data1").Set("name", "abc").Set("count", 10).SetScore(1.0));
        AddDocument(db, new Document("data2").Set("name", "def").Set("count", 5));
        AddDocument(db, new Document("data3").Set("name", "def").Set("count", 25));

        AggregationRequest r = new AggregationRequest()
                .GroupBy("@name", Reducers.Sum("@count").As("sum"))
                .Timeout(5000);

        AggregationResult res = ft.Aggregate(index, r);
        Assert.Equal(2, res.TotalResults);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestAggregationRequestTimeoutAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new();
        sc.AddTextField("name", 1.0, sortable: true);
        sc.AddNumericField("count", sortable: true);
        ft.Create(index, FTCreateParams.CreateParams(), sc);
        AddDocument(db, new Document("data1").Set("name", "abc").Set("count", 10).SetScore(1.0));
        AddDocument(db, new Document("data2").Set("name", "def").Set("count", 5));
        AddDocument(db, new Document("data3").Set("name", "def").Set("count", 25));

        AggregationRequest r = new AggregationRequest()
                .GroupBy("@name", Reducers.Sum("@count").As("sum"))
                .Timeout(5000);

        AggregationResult res = await ft.AggregateAsync(index, r);
        Assert.Equal(2, res.TotalResults);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestAggregations(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new();
        sc.AddTextField("name", 1.0, true);
        sc.AddNumericField("count", true);
        ft.Create(index, FTCreateParams.CreateParams(), sc);
        //    client.AddDocument(new Document("data1").Set("name", "abc").Set("count", 10));
        //    client.AddDocument(new Document("data2").Set("name", "def").Set("count", 5));
        //    client.AddDocument(new Document("data3").Set("name", "def").Set("count", 25));
        AddDocument(db, new Document("data1").Set("name", "abc").Set("count", 10));
        AddDocument(db, new Document("data2").Set("name", "def").Set("count", 5));
        AddDocument(db, new Document("data3").Set("name", "def").Set("count", 25));

        AggregationRequest r = new AggregationRequest()
            .GroupBy("@name", Reducers.Sum("@count").As("sum"))
        .SortBy(10, SortedField.Desc("@sum"));

        // actual search
        var res = ft.Aggregate(index, r);
        Assert.Equal(2, res.TotalResults);

        Row r1 = res.GetRow(0);
        Assert.Equal("def", r1.GetString("name"));
        Assert.Equal(30, r1.GetLong("sum"));
        Assert.Equal(30, r1.GetDouble("sum"), 0);

        Assert.Equal(0L, r1.GetLong("nosuchcol"));
        Assert.Equal(0.0, r1.GetDouble("nosuchcol"), 0);
        Assert.Null(r1.GetString("nosuchcol"));

        Row r2 = res.GetRow(1);
        Assert.Equal("abc", r2.GetString("name"));
        Assert.Equal(10, r2.GetLong("sum"));
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestAggregationsAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new();
        sc.AddTextField("name", 1.0, true);
        sc.AddNumericField("count", true);
        await ft.CreateAsync(index, FTCreateParams.CreateParams(), sc);
        //    client.AddDocument(new Document("data1").Set("name", "abc").Set("count", 10));
        //    client.AddDocument(new Document("data2").Set("name", "def").Set("count", 5));
        //    client.AddDocument(new Document("data3").Set("name", "def").Set("count", 25));
        AddDocument(db, new Document("data1").Set("name", "abc").Set("count", 10));
        AddDocument(db, new Document("data2").Set("name", "def").Set("count", 5));
        AddDocument(db, new Document("data3").Set("name", "def").Set("count", 25));

        AggregationRequest r = new AggregationRequest()
            .GroupBy("@name", Reducers.Sum("@count").As("sum"))
        .SortBy(10, SortedField.Desc("@sum"));

        // actual search
        var res = await ft.AggregateAsync(index, r);
        Assert.Equal(2, res.TotalResults);

        Row r1 = res.GetRow(0);
        Assert.Equal("def", r1.GetString("name"));
        Assert.Equal(30, r1.GetLong("sum"));
        Assert.Equal(30, r1.GetDouble("sum"), 0);

        Assert.Equal(0L, r1.GetLong("nosuchcol"));
        Assert.Equal(0.0, r1.GetDouble("nosuchcol"), 0);
        Assert.Null(r1.GetString("nosuchcol"));

        Row r2 = res.GetRow(1);
        Assert.Equal("abc", r2.GetString("name"));
        Assert.Equal(10, r2.GetLong("sum"));
    }


    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestAggregationsLoad(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        var sc = new Schema().AddTextField("t1").AddTextField("t2");
        ft.Create("idx", new(), sc);

        AddDocument(db, new Document("doc1").Set("t1", "hello").Set("t2", "world"));
        AssertDatabaseSize(db, 1);

        // load t1
        var req = new AggregationRequest("*").Load(new FieldName("t1"));
        var res = ft.Aggregate("idx", req);
        Assert.NotNull(res[0]?["t1"]);
        Assert.Equal("hello", res[0]!["t1"].ToString());

        // load t2
        req = new AggregationRequest("*").Load(new FieldName("t2"));
        res = ft.Aggregate("idx", req);
        Assert.NotNull(res[0]?["t2"]);
        Assert.Equal("world", res[0]!["t2"]);

        // load all
        req = new AggregationRequest("*").LoadAll();
        res = ft.Aggregate("idx", req);
        Assert.NotNull(res[0]?["t1"]);
        Assert.Equal("hello", res[0]!["t1"].ToString());
        Assert.NotNull(res[0]?["t2"]);
        Assert.Equal("world", res[0]!["t2"]);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestAggregationsLoadAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        var sc = new Schema().AddTextField("t1").AddTextField("t2");
        await ft.CreateAsync("idx", new(), sc);

        AddDocument(db, new Document("doc1").Set("t1", "hello").Set("t2", "world"));
        await AssertDatabaseSizeAsync(db, 1);

        // load t1
        var req = new AggregationRequest("*").Load(new FieldName("t1"));
        var res = await ft.AggregateAsync("idx", req);
        Assert.NotNull(res[0]?["t1"]);
        Assert.Equal("hello", res[0]!["t1"].ToString());

        // load t2
        req = new AggregationRequest("*").Load(new FieldName("t2"));
        res = await ft.AggregateAsync("idx", req);
        Assert.NotNull(res[0]?["t2"]);
        Assert.Equal("world", res[0]!["t2"]);

        // load all
        req = new AggregationRequest("*").LoadAll();
        res = await ft.AggregateAsync("idx", req);
        Assert.NotNull(res[0]?["t1"]);
        Assert.Equal("hello", res[0]!["t1"].ToString());
        Assert.NotNull(res[0]?["t2"]);
        Assert.Equal("world", res[0]!["t2"]);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestAggregationRequestParamsDialect(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new();
        sc.AddTextField("name", 1.0, sortable: true);
        sc.AddNumericField("count", sortable: true);
        ft.Create(index, FTCreateParams.CreateParams(), sc);
        AddDocument(db, new Document("data1").Set("name", "abc").Set("count", 10));
        AddDocument(db, new Document("data2").Set("name", "def").Set("count", 5));
        AddDocument(db, new Document("data3").Set("name", "def").Set("count", 25));

        Dictionary<string, object> parameters = new();
        parameters.Add("name", "abc");
        parameters.Add("count", "10");

        AggregationRequest r = new AggregationRequest("$name")
                .GroupBy("@name", Reducers.Sum("@count").As("sum"))
                .Params(parameters)
                .Dialect(2); // From documentation - To use PARAMS, DIALECT must be set to 2

        AggregationResult res = ft.Aggregate(index, r);
        Assert.Equal(1, res.TotalResults);

        Row r1 = res.GetRow(0);
        Assert.Equal("abc", r1.GetString("name"));
        Assert.Equal(10, r1.GetLong("sum"));
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestAggregationRequestParamsDialectAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new();
        sc.AddTextField("name", 1.0, sortable: true);
        sc.AddNumericField("count", sortable: true);
        ft.Create(index, FTCreateParams.CreateParams(), sc);
        AddDocument(db, new Document("data1").Set("name", "abc").Set("count", 10));
        AddDocument(db, new Document("data2").Set("name", "def").Set("count", 5));
        AddDocument(db, new Document("data3").Set("name", "def").Set("count", 25));

        Dictionary<string, object> parameters = new();
        parameters.Add("name", "abc");
        parameters.Add("count", "10");


        AggregationRequest r = new AggregationRequest("$name")
                .GroupBy("@name", Reducers.Sum("@count").As("sum"))
                .Params(parameters)
                .Dialect(2); // From documentation - To use PARAMS, DIALECT must be set to 2

        AggregationResult res = await ft.AggregateAsync(index, r);
        Assert.Equal(1, res.TotalResults);

        Row r1 = res.GetRow(0);
        Assert.Equal("abc", r1.GetString("name"));
        Assert.Equal(10, r1.GetLong("sum"));
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestAggregationRequestParamsWithDefaultDialect(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT(2);
        Schema sc = new();
        sc.AddTextField("name", 1.0, sortable: true);
        sc.AddNumericField("count", sortable: true);
        ft.Create(index, FTCreateParams.CreateParams(), sc);
        AddDocument(db, new Document("data1").Set("name", "abc").Set("count", 10));
        AddDocument(db, new Document("data2").Set("name", "def").Set("count", 5));
        AddDocument(db, new Document("data3").Set("name", "def").Set("count", 25));

        Dictionary<string, object> parameters = new();
        parameters.Add("name", "abc");
        parameters.Add("count", "10");

        AggregationRequest r = new AggregationRequest("$name")
                .GroupBy("@name", Reducers.Sum("@count").As("sum"))
                .Params(parameters); // From documentation - To use PARAMS, DIALECT must be set to 2
                                     // which is the default as we set in the constructor (FT(2))

        AggregationResult res = ft.Aggregate(index, r);
        Assert.Equal(1, res.TotalResults);

        Row r1 = res.GetRow(0);
        Assert.Equal("abc", r1.GetString("name"));
        Assert.Equal(10, r1.GetLong("sum"));
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestAggregationRequestParamsWithDefaultDialectAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT(2);
        Schema sc = new();
        sc.AddTextField("name", 1.0, sortable: true);
        sc.AddNumericField("count", sortable: true);
        ft.Create(index, FTCreateParams.CreateParams(), sc);
        AddDocument(db, new Document("data1").Set("name", "abc").Set("count", 10));
        AddDocument(db, new Document("data2").Set("name", "def").Set("count", 5));
        AddDocument(db, new Document("data3").Set("name", "def").Set("count", 25));

        Dictionary<string, object> parameters = new();
        parameters.Add("name", "abc");
        parameters.Add("count", "10");

        AggregationRequest r = new AggregationRequest("$name")
                .GroupBy("@name", Reducers.Sum("@count").As("sum"))
                .Params(parameters); // From documentation - To use PARAMS, DIALECT must be set to 2
                                     // which is the default as we set in the constructor (FT(2))

        AggregationResult res = await ft.AggregateAsync(index, r);
        Assert.Equal(1, res.TotalResults);

        Row r1 = res.GetRow(0);
        Assert.Equal("abc", r1.GetString("name"));
        Assert.Equal(10, r1.GetLong("sum"));
    }

    [Fact]
    public void TestDefaultDialectError()
    {
        IDatabase db = GetCleanDatabase();
        // test error on invalid dialect:
        Assert.Throws<ArgumentOutOfRangeException>(() => db.FT(0));
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestAlias(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new Schema().AddTextField("field1");

        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));

        Dictionary<string, object> doc = new();
        doc.Add("field1", "value");
        AddDocument(db, "doc1", doc);

        try
        {
            Assert.True(ft.AliasAdd("ALIAS1", index));
        }
        catch (RedisServerException rse)
        {
            Skip.If(rse.Message.StartsWith("CROSSSLOT"), "legacy failure");
            throw;
        }

        SearchResult res1 = ft.Search("ALIAS1", new Query("*").ReturnFields("field1"));
        Assert.Equal(1, res1.TotalResults);
        Assert.Equal("value", res1.Documents[0]["field1"]);

        Assert.True(ft.AliasUpdate("ALIAS2", index));
        SearchResult res2 = ft.Search("ALIAS2", new Query("*").ReturnFields("field1"));
        Assert.Equal(1, res2.TotalResults);
        Assert.Equal("value", res2.Documents[0]["field1"]);

        Assert.Throws<RedisServerException>(() => ft.AliasDel("ALIAS3"));
        Assert.True(ft.AliasDel("ALIAS2"));
        Assert.Throws<RedisServerException>(() => ft.AliasDel("ALIAS2"));
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestAliasAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new Schema().AddTextField("field1");

        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));

        Dictionary<string, object> doc = new();
        doc.Add("field1", "value");
        AddDocument(db, "doc1", doc);

        try
        {
            Assert.True(await ft.AliasAddAsync("ALIAS1", index));
        }
        catch (RedisServerException rse)
        {
            Skip.If(rse.Message.StartsWith("CROSSSLOT"), "legacy failure");
            throw;
        }

        SearchResult res1 = ft.Search("ALIAS1", new Query("*").ReturnFields("field1"));
        Assert.Equal(1, res1.TotalResults);
        Assert.Equal("value", res1.Documents[0]["field1"]);

        Assert.True(await ft.AliasUpdateAsync("ALIAS2", index));
        SearchResult res2 = ft.Search("ALIAS2", new Query("*").ReturnFields("field1"));
        Assert.Equal(1, res2.TotalResults);
        Assert.Equal("value", res2.Documents[0]["field1"]);

        await Assert.ThrowsAsync<RedisServerException>(async () => await ft.AliasDelAsync("ALIAS3"));
        Assert.True(await ft.AliasDelAsync("ALIAS2"));
        await Assert.ThrowsAsync<RedisServerException>(async () => await ft.AliasDelAsync("ALIAS2"));
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestApplyAndFilterAggregations(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new();
        sc.AddTextField("name", 1.0, sortable: true);
        sc.AddNumericField("subj1", sortable: true);
        sc.AddNumericField("subj2", sortable: true);
        ft.Create(index, FTCreateParams.CreateParams(), sc);
        //    client.AddDocument(db, new Document("data1").Set("name", "abc").Set("subj1", 20).Set("subj2", 70));
        //    client.AddDocument(db, new Document("data2").Set("name", "def").Set("subj1", 60).Set("subj2", 40));
        //    client.AddDocument(db, new Document("data3").Set("name", "ghi").Set("subj1", 50).Set("subj2", 80));
        //    client.AddDocument(db, new Document("data4").Set("name", "abc").Set("subj1", 30).Set("subj2", 20));
        //    client.AddDocument(db, new Document("data5").Set("name", "def").Set("subj1", 65).Set("subj2", 45));
        //    client.AddDocument(db, new Document("data6").Set("name", "ghi").Set("subj1", 70).Set("subj2", 70));
        AddDocument(db, new Document("data1").Set("name", "abc").Set("subj1", 20).Set("subj2", 70));
        AddDocument(db, new Document("data2").Set("name", "def").Set("subj1", 60).Set("subj2", 40));
        AddDocument(db, new Document("data3").Set("name", "ghi").Set("subj1", 50).Set("subj2", 80));
        AddDocument(db, new Document("data4").Set("name", "abc").Set("subj1", 30).Set("subj2", 20));
        AddDocument(db, new Document("data5").Set("name", "def").Set("subj1", 65).Set("subj2", 45));
        AddDocument(db, new Document("data6").Set("name", "ghi").Set("subj1", 70).Set("subj2", 70));
        AssertDatabaseSize(db, 6);

        int maxAttempts = endpointId == EndpointsFixture.Env.Cluster ? 10 : 3;
        for (int attempt = 1; attempt <= maxAttempts; attempt++)
        {
            AggregationRequest r = new AggregationRequest().Apply("(@subj1+@subj2)/2", "attemptavg")
                .GroupBy("@name", Reducers.Avg("@attemptavg").As("avgscore"))
                .Filter("@avgscore>=50")
                .SortBy(10, SortedField.Asc("@name"));

            // abc: 20+70 => 45, 30+20 => 25, filtered out
            // def: 60+40 => 50, 65+45 => 55, avg 52.5
            // ghi: 50+80 => 65, 70+70 => 70, avg 67.5

            // actual search
            AggregationResult res = ft.Aggregate(index, r);
            Assert.Equal(2, res.TotalResults);

            Row r1 = res.GetRow(0);
            Row r2 = res.GetRow(1);
            Log($"Attempt {attempt} of {maxAttempts}: avgscore {r2.GetDouble("avgscore")}");
            if (!IsNear(r2.GetDouble("avgscore"), 67.5)) continue; // this test can be flakey on cluster

            Assert.Equal("def", r1.GetString("name"));
            Assert.Equal(52.5, r1.GetDouble("avgscore"), 0);

            Assert.Equal("ghi", r2.GetString("name"));
            Assert.Equal(67.5, r2.GetDouble("avgscore"), 0);
            break; // success!
        }
    }

    private static bool IsNear(double a, double b, double epsilon = 0.1) => Math.Abs(a - b) < epsilon;

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestCreate(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        var schema = new Schema().AddTextField("first").AddTextField("last").AddNumericField("age");
        var parameters = FTCreateParams.CreateParams().Filter("@age>16").Prefix("student:", "pupil:");

        Assert.True(ft.Create(index, parameters, schema));

        db.HashSet("profesor:5555", [new("first", "Albert"), new("last", "Blue"), new("age", "55")]);
        db.HashSet("student:1111", [new("first", "Joe"), new("last", "Dod"), new("age", "18")]);
        db.HashSet("pupil:2222", [new("first", "Jen"), new("last", "Rod"), new("age", "14")]);
        db.HashSet("student:3333", [new("first", "El"), new("last", "Mark"), new("age", "17")]);
        db.HashSet("pupil:4444", [new("first", "Pat"), new("last", "Shu"), new("age", "21")]);
        db.HashSet("student:5555", [new("first", "Joen"), new("last", "Ko"), new("age", "20")]);
        db.HashSet("teacher:6666", [new("first", "Pat"), new("last", "Rod"), new("age", "20")]);
        AssertDatabaseSize(db, 7);

        var noFilters = ft.Search(index, new());
        Assert.Equal(4, noFilters.TotalResults);

        var res1 = ft.Search(index, new("@first:Jo*"));
        Assert.Equal(2, res1.TotalResults);

        var res2 = ft.Search(index, new("@first:Pat"));
        Assert.Equal(1, res2.TotalResults);

        var res3 = ft.Search(index, new("@last:Rod"));
        Assert.Equal(0, res3.TotalResults);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestCreateAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        var schema = new Schema().AddTextField("first").AddTextField("last").AddNumericField("age");
        var parameters = FTCreateParams.CreateParams().Filter("@age>16").Prefix("student:", "pupil:");
        Assert.True(await ft.CreateAsync(index, parameters, schema));
        db.HashSet("profesor:5555", [new("first", "Albert"), new("last", "Blue"), new("age", "55")]);
        db.HashSet("student:1111", [new("first", "Joe"), new("last", "Dod"), new("age", "18")]);
        db.HashSet("pupil:2222", [new("first", "Jen"), new("last", "Rod"), new("age", "14")]);
        db.HashSet("student:3333", [new("first", "El"), new("last", "Mark"), new("age", "17")]);
        db.HashSet("pupil:4444", [new("first", "Pat"), new("last", "Shu"), new("age", "21")]);
        db.HashSet("student:5555", [new("first", "Joen"), new("last", "Ko"), new("age", "20")]);
        db.HashSet("teacher:6666", [new("first", "Pat"), new("last", "Rod"), new("age", "20")]);
        await AssertDatabaseSizeAsync(db, 7);

        var noFilters = ft.Search(index, new());
        Assert.Equal(4, noFilters.TotalResults);
        var res1 = ft.Search(index, new("@first:Jo*"));
        Assert.Equal(2, res1.TotalResults);
        var res2 = ft.Search(index, new("@first:Pat"));
        Assert.Equal(1, res2.TotalResults);
        var res3 = ft.Search(index, new("@last:Rod"));
        Assert.Equal(0, res3.TotalResults);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void CreateNoParams(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();

        Schema sc = new Schema().AddTextField("first", 1.0).AddTextField("last", 1.0).AddNumericField("age");
        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));

        db.HashSet("student:1111", [new("first", "Joe"), new("last", "Dod"), new("age", 18)]);
        db.HashSet("student:3333", [new("first", "El"), new("last", "Mark"), new("age", 17)]);
        db.HashSet("pupil:4444", [new("first", "Pat"), new("last", "Shu"), new("age", 21)]);
        db.HashSet("student:5555", [new("first", "Joen"), new("last", "Ko"), new("age", 20)]);
        AssertDatabaseSize(db, 4);

        SearchResult noFilters = ft.Search(index, new());
        Assert.Equal(4, noFilters.TotalResults);

        SearchResult res1 = ft.Search(index, new("@first:Jo*"));
        Assert.Equal(2, res1.TotalResults);

        SearchResult res2 = ft.Search(index, new("@first:Pat"));
        Assert.Equal(1, res2.TotalResults);

        SearchResult res3 = ft.Search(index, new("@last:Rod"));
        Assert.Equal(0, res3.TotalResults);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task CreateNoParamsAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();

        Schema sc = new Schema().AddTextField("first", 1.0).AddTextField("last", 1.0).AddNumericField("age");
        Assert.True(await ft.CreateAsync(index, FTCreateParams.CreateParams(), sc));

        db.HashSet("student:1111", [new("first", "Joe"), new("last", "Dod"), new("age", 18)]);
        db.HashSet("student:3333", [new("first", "El"), new("last", "Mark"), new("age", 17)]);
        db.HashSet("pupil:4444", [new("first", "Pat"), new("last", "Shu"), new("age", 21)]);
        db.HashSet("student:5555", [new("first", "Joen"), new("last", "Ko"), new("age", 20)]);
        await AssertDatabaseSizeAsync(db, 4);

        SearchResult noFilters = ft.Search(index, new());
        Assert.Equal(4, noFilters.TotalResults);

        SearchResult res1 = ft.Search(index, new("@first:Jo*"));
        Assert.Equal(2, res1.TotalResults);

        SearchResult res2 = ft.Search(index, new("@first:Pat"));
        Assert.Equal(1, res2.TotalResults);

        SearchResult res3 = ft.Search(index, new("@last:Rod"));
        Assert.Equal(0, res3.TotalResults);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void CreateWithFieldNames(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new Schema().AddField(new TextField(FieldName.Of("first").As("given")))
            .AddField(new TextField(FieldName.Of("last")));

        Assert.True(ft.Create(index, FTCreateParams.CreateParams().Prefix("student:", "pupil:"), sc));

        db.HashSet("profesor:5555", [new("first", "Albert"), new("last", "Blue"), new("age", "55")]);
        db.HashSet("student:1111", [new("first", "Joe"), new("last", "Dod"), new("age", "18")]);
        db.HashSet("pupil:2222", [new("first", "Jen"), new("last", "Rod"), new("age", "14")]);
        db.HashSet("student:3333", [new("first", "El"), new("last", "Mark"), new("age", "17")]);
        db.HashSet("pupil:4444", [new("first", "Pat"), new("last", "Shu"), new("age", "21")]);
        db.HashSet("student:5555", [new("first", "Joen"), new("last", "Ko"), new("age", "20")]);
        db.HashSet("teacher:6666", [new("first", "Pat"), new("last", "Rod"), new("age", "20")]);
        AssertDatabaseSize(db, 7);

        SearchResult noFilters = ft.Search(index, new());
        Assert.Equal(5, noFilters.TotalResults);

        SearchResult asAttribute = ft.Search(index, new("@given:Jo*"));
        Assert.Equal(2, asAttribute.TotalResults);

        SearchResult nonAttribute = ft.Search(index, new("@last:Rod"));
        Assert.Equal(1, nonAttribute.TotalResults);
    }

    [SkipIfRedisTheory(Comparison.LessThan, "7.9.0")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void FailWhenAttributeNotExist(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new Schema().AddField(new TextField(FieldName.Of("first").As("given")))
            .AddField(new TextField(FieldName.Of("last")));

        Assert.True(ft.Create(index, FTCreateParams.CreateParams().Prefix("student:", "pupil:"), sc));
        RedisServerException exc = Assert.Throws<RedisServerException>(() => ft.Search(index, new("@first:Jo*")));
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task CreateWithFieldNamesAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new Schema().AddField(new TextField(FieldName.Of("first").As("given")))
            .AddField(new TextField(FieldName.Of("last")));

        Assert.True(await ft.CreateAsync(index, FTCreateParams.CreateParams().Prefix("student:", "pupil:"), sc));

        db.HashSet("profesor:5555", [new("first", "Albert"), new("last", "Blue"), new("age", "55")]);
        db.HashSet("student:1111", [new("first", "Joe"), new("last", "Dod"), new("age", "18")]);
        db.HashSet("pupil:2222", [new("first", "Jen"), new("last", "Rod"), new("age", "14")]);
        db.HashSet("student:3333", [new("first", "El"), new("last", "Mark"), new("age", "17")]);
        db.HashSet("pupil:4444", [new("first", "Pat"), new("last", "Shu"), new("age", "21")]);
        db.HashSet("student:5555", [new("first", "Joen"), new("last", "Ko"), new("age", "20")]);
        db.HashSet("teacher:6666", [new("first", "Pat"), new("last", "Rod"), new("age", "20")]);

        SearchResult noFilters = await ft.SearchAsync(index, new());
        Assert.Equal(5, noFilters.TotalResults);

        SearchResult asAttribute = await ft.SearchAsync(index, new("@given:Jo*"));
        Assert.Equal(2, asAttribute.TotalResults);

        SearchResult nonAttribute = await ft.SearchAsync(index, new("@last:Rod"));
        Assert.Equal(1, nonAttribute.TotalResults);
    }

    [SkipIfRedisTheory(Comparison.LessThan, "7.9.0")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task FailWhenAttributeNotExistAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new Schema().AddField(new TextField(FieldName.Of("first").As("given")))
            .AddField(new TextField(FieldName.Of("last")));

        Assert.True(await ft.CreateAsync(index, FTCreateParams.CreateParams().Prefix("student:", "pupil:"), sc));
        RedisServerException exc = await Assert.ThrowsAsync<RedisServerException>(async () => await ft.SearchAsync(index, new("@first:Jo*")));
    }

    [SkipIfRedisTheory(Is.Enterprise)]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void AlterAdd(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new Schema().AddTextField("title", 1.0);

        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));

        //sleep:
        Thread.Sleep(2000);

        var fields = new HashEntry("title", "hello world");
        //fields.("title", "hello world");
        AssertDatabaseSize(db, 0);
        for (int i = 0; i < 100; i++)
        {
            db.HashSet($"doc{i}", fields.Name, fields.Value);
        }
        AssertDatabaseSize(db, 100);
        var info = ft.Info(index);
        Assert.Equal(index, info.IndexName);
        if (endpointId == EndpointsFixture.Env.Cluster)
        {
            Assert.True(info.NumDocs is 100 or 200, $"NumDocs: {info.NumDocs}");
        }
        else
        {
            Assert.Equal(100, info.NumDocs);
        }

        SearchResult res = ft.Search(index, new("hello world"));
        Assert.Equal(100, res.TotalResults);

        Assert.True(ft.Alter(index, new Schema().AddTagField("tags").AddTextField("name", weight: 0.5)));
        for (int i = 0; i < 100; i++)
        {
            var fields2 = new HashEntry[] { new("name", "name" + i),
                                      new("tags", $"tagA,tagB,tag{i}") };
            //      assertTrue(client.updateDocument(string.format("doc%d", i), 1.0, fields2));
            db.HashSet($"doc{i}", fields2);
        }
        SearchResult res2 = ft.Search(index, new("@tags:{tagA}"));
        Assert.Equal(100, res2.TotalResults);

        AssertDatabaseSize(db, 100);

        info = ft.Info(index);
        Assert.Equal(index, info.IndexName);
        Assert.Empty(info.IndexOption);
        // Assert.Equal(,info.IndexDefinition);
        Assert.Equal("title", info.Attributes[0]["identifier"].ToString());
        Assert.Equal("TAG", info.Attributes[1]["type"].ToString());
        Assert.Equal("name", info.Attributes[2]["attribute"].ToString());

        if (endpointId == EndpointsFixture.Env.Cluster)
        {
            Assert.True(info.NumDocs is 100 or 200, $"NumDocs: {info.NumDocs}");
        }
        else
        {
            Assert.Equal(100, info.NumDocs);

            // these numbers don't make sense when considering a shard
            Assert.NotNull(info.MaxDocId);
            Assert.Equal(102, info.NumTerms);
            Assert.True(info.NumRecords >= 200);
            Assert.True(info.InvertedSzMebibytes < 1); // TODO: check this line and all the <1 lines
            Assert.Equal(0, info.VectorIndexSzMebibytes);
            Assert.Equal(208, info.TotalInvertedIndexBlocks);
            Assert.True(info.OffsetVectorsSzMebibytes < 1);
            Assert.True(info.DocTableSizeMebibytes < 1);
            Assert.Equal(0, info.SortableValueSizeMebibytes);
            Assert.True(info.KeyTableSizeMebibytes < 1);
            Assert.Equal(8, (int)info.RecordsPerDocAvg);
            Assert.True(info.BytesPerRecordAvg > 5);
            Assert.True(info.OffsetsPerTermAvg > 0.8);
            Assert.Equal(8, info.OffsetBitsPerRecordAvg);
            Assert.Equal(0, info.HashIndexingFailures);
            Assert.Equal(0, info.Indexing);
            Assert.Equal(1, info.PercentIndexed);
            Assert.Equal(5, info.NumberOfUses);
            Assert.Equal(7, info.GcStats.Count);
            Assert.Equal(4, info.CursorStats.Count);
        }
    }

    [SkipIfRedisTheory(Is.Enterprise)]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task AlterAddAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new Schema().AddTextField("title", 1.0);

        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));

        //sleep:
        Thread.Sleep(2000);

        var fields = new HashEntry("title", "hello world");
        //fields.("title", "hello world");
        for (int i = 0; i < 100; i++)
        {
            db.HashSet($"doc{i}", fields.Name, fields.Value);
        }
        SearchResult res = ft.Search(index, new("hello world"));
        Assert.Equal(100, res.TotalResults);
        var info = ft.Info(index);
        Assert.Equal(index, info.IndexName);
        if (endpointId == EndpointsFixture.Env.Cluster)
        {
            Assert.True(info.NumDocs is 100 or 200, $"NumDocs: {info.NumDocs}");
        }
        else
        {
            Assert.Equal(100, info.NumDocs);
        }

        Assert.True(await ft.AlterAsync(index, new Schema().AddTagField("tags").AddTextField("name", weight: 0.5)));
        for (int i = 0; i < 100; i++)
        {
            var fields2 = new HashEntry[] { new("name", "name" + i),
                                      new("tags", $"tagA,tagB,tag{i}") };
            //      assertTrue(client.updateDocument(string.format("doc%d", i), 1.0, fields2));
            db.HashSet($"doc{i}", fields2);
        }
        SearchResult res2 = ft.Search(index, new("@tags:{tagA}"));
        Assert.Equal(100, res2.TotalResults);

        await AssertDatabaseSizeAsync(db, 100);

        info = await ft.InfoAsync(index);
        Assert.Equal(index, info.IndexName);
        Assert.Equal("title", info.Attributes[0]["identifier"].ToString());
        Assert.Equal("TAG", info.Attributes[1]["type"].ToString());
        Assert.Equal("name", info.Attributes[2]["attribute"].ToString());
        if (endpointId == EndpointsFixture.Env.Cluster)
        {
            Assert.True(info.NumDocs is 100 or 200, $"NumDocs: {info.NumDocs}");
        }
        else
        {
            Assert.Equal(100, info.NumDocs);

            // these numbers don't make sense when considering a shard
            Assert.Equal("300", info.MaxDocId);
            Assert.Equal(102, info.NumTerms);
            Assert.True(info.NumRecords >= 200);
            Assert.True(info.InvertedSzMebibytes < 1); // TODO: check this line and all the <1 lines
            Assert.Equal(0, info.VectorIndexSzMebibytes);
            Assert.Equal(208, info.TotalInvertedIndexBlocks);
            Assert.True(info.OffsetVectorsSzMebibytes < 1);
            Assert.True(info.DocTableSizeMebibytes < 1);
            Assert.Equal(0, info.SortableValueSizeMebibytes);
            Assert.True(info.KeyTableSizeMebibytes < 1);
            Assert.Equal(8, (int)info.RecordsPerDocAvg);
            Assert.True(info.BytesPerRecordAvg > 5);
            Assert.True(info.OffsetsPerTermAvg > 0.8);
            Assert.Equal(8, info.OffsetBitsPerRecordAvg);
            Assert.Equal(0, info.HashIndexingFailures);
            Assert.Equal(0, info.Indexing);
            Assert.Equal(1, info.PercentIndexed);
            Assert.Equal(5, info.NumberOfUses);
            Assert.Equal(7, info.GcStats.Count);
            Assert.Equal(4, info.CursorStats.Count);
        }
    }

    [SkipIfRedisTheory(Is.Enterprise)]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void AlterAddSortable(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new Schema().AddTextField("title", 1.0, sortable: true);

        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));

        //sleep:
        Thread.Sleep(2000);

        var fields = new HashEntry("title", "hello world");
        //fields.("title", "hello world");
        for (int i = 0; i < 100; i++)
        {
            db.HashSet($"doc{i}", fields.Name, fields.Value);
        }
        SearchResult res = ft.Search(index, new("hello world"));
        Assert.Equal(100, res.TotalResults);

        Assert.True(ft.Alter(index, new Schema().AddTagField("tags").AddTextField("name", weight: 0.5)));
        for (int i = 0; i < 100; i++)
        {
            var fields2 = new HashEntry[] { new("name", "name" + i),
                                      new("tags", $"tagA,tagB,tag{i}") };
            //      assertTrue(client.updateDocument(string.format("doc%d", i), 1.0, fields2));
            db.HashSet($"doc{i}", fields2);
        }
        SearchResult res2 = ft.Search(index, new("@tags:{tagA}"));
        Assert.Equal(100, res2.TotalResults);

        var info = ft.Info(index);
        Assert.Equal(index, info.IndexName);
        Assert.Empty(info.IndexOption);
        // Assert.Equal(,info.IndexDefinition);
        Assert.Equal("title", info.Attributes[0]["identifier"].ToString());
        Assert.Equal("TAG", info.Attributes[1]["type"].ToString());
        Assert.Equal("name", info.Attributes[2]["attribute"].ToString());
        if (endpointId == EndpointsFixture.Env.Cluster)
        {
            Assert.True(info.NumDocs is 100 or 200, $"NumDocs: {info.NumDocs}");
        }
        else
        {
            Assert.Equal(100, info.NumDocs);

            // these numbers don't make sense when considering a shard
            Assert.NotNull(info.MaxDocId);
            Assert.Equal(102, info.NumTerms);
            Assert.True(info.NumRecords >= 200);
            Assert.True(info.InvertedSzMebibytes < 1); // TODO: check this line and all the <1 lines
            Assert.Equal(0, info.VectorIndexSzMebibytes);
            Assert.Equal(208, info.TotalInvertedIndexBlocks);
            Assert.True(info.OffsetVectorsSzMebibytes < 1);
            Assert.True(info.DocTableSizeMebibytes < 1);
            Assert.Equal(0, info.SortableValueSizeMebibytes);
            Assert.True(info.KeyTableSizeMebibytes < 1);
            Assert.Equal(8, (int)info.RecordsPerDocAvg);
            Assert.True(info.BytesPerRecordAvg > 5);
            Assert.True(info.OffsetsPerTermAvg > 0.8);
            Assert.Equal(8, info.OffsetBitsPerRecordAvg);
            Assert.Equal(0, info.HashIndexingFailures);
            Assert.Equal(0, info.Indexing);
            Assert.Equal(1, info.PercentIndexed);
            Assert.Equal(4, info.NumberOfUses);
            Assert.Equal(7, info.GcStats.Count);
            Assert.Equal(4, info.CursorStats.Count);
        }
    }

    [SkipIfRedisTheory(Comparison.LessThan, "7.3.0")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void InfoWithIndexEmptyAndIndexMissing(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        db.Execute("FLUSHALL");
        var ft = db.FT(2);
        var vectorAttrs = new Dictionary<string, object>()
        {
            ["TYPE"] = "FLOAT32",
            ["DIM"] = "2",
            ["DISTANCE_METRIC"] = "L2",
        };

        Schema sc = new Schema()
            .AddTextField("text1", 1.0, emptyIndex: true, missingIndex: true)
            .AddTagField("tag1", emptyIndex: true, missingIndex: true)
            .AddNumericField("numeric1", missingIndex: true)
            .AddGeoField("geo1", missingIndex: true)
            .AddGeoShapeField("geoshape1", GeoShapeField.CoordinateSystem.FLAT, missingIndex: true)
            .AddVectorField("vector1", VectorField.VectorAlgo.FLAT, vectorAttrs, missingIndex: true);
        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));

        var info = ft.Info(index);
        var attributes = info.Attributes;
        foreach (var attribute in attributes)
        {
            Assert.True(attribute.ContainsKey("INDEXMISSING"));
            if (attribute["attribute"].ToString() == "text1" || attribute["attribute"].ToString() == "tag1")
            {
                Assert.True(attribute.ContainsKey("INDEXEMPTY"));
            }
        }
    }

    [SkipIfRedisTheory(Is.Enterprise)]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task AlterAddSortableAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new Schema().AddTextField("title", 1.0, sortable: true);

        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));

        //sleep:
        Thread.Sleep(2000);

        var fields = new HashEntry("title", "hello world");
        //fields.("title", "hello world");
        for (int i = 0; i < 100; i++)
        {
            db.HashSet($"doc{i}", fields.Name, fields.Value);
        }
        SearchResult res = ft.Search(index, new("hello world"));
        Assert.Equal(100, res.TotalResults);

        Assert.True(await ft.AlterAsync(index, new Schema().AddTagField("tags").AddTextField("name", weight: 0.5)));
        for (int i = 0; i < 100; i++)
        {
            var fields2 = new HashEntry[] { new("name", "name" + i),
                                      new("tags", $"tagA,tagB,tag{i}") };
            //      assertTrue(client.updateDocument(string.format("doc%d", i), 1.0, fields2));
            db.HashSet($"doc{i}", fields2);
        }
        SearchResult res2 = ft.Search(index, new("@tags:{tagA}"));
        Assert.Equal(100, res2.TotalResults);

        var info = await ft.InfoAsync(index);
        Assert.Equal(index, info.IndexName);
        Assert.Equal("title", info.Attributes[0]["identifier"].ToString());
        Assert.Equal("TAG", info.Attributes[1]["type"].ToString());
        Assert.Equal("name", info.Attributes[2]["attribute"].ToString());
        if (endpointId == EndpointsFixture.Env.Cluster)
        {
            Assert.True(info.NumDocs is 100 or 200, $"NumDocs: {info.NumDocs}");
        }
        else
        {
            Assert.Equal(100, info.NumDocs);

            // these numbers don't make sense when considering a shard
            Assert.Equal("300", info.MaxDocId);
            Assert.Equal(102, info.NumTerms);
            Assert.True(info.NumRecords >= 200);
            Assert.True(info.InvertedSzMebibytes < 1); // TODO: check this line and all the <1 lines
            Assert.Equal(0, info.VectorIndexSzMebibytes);
            Assert.Equal(208, info.TotalInvertedIndexBlocks);
            Assert.True(info.OffsetVectorsSzMebibytes < 1);
            Assert.True(info.DocTableSizeMebibytes < 1);
            Assert.Equal(0, info.SortableValueSizeMebibytes);
            Assert.True(info.KeyTableSizeMebibytes < 1);
            Assert.Equal(8, (int)info.RecordsPerDocAvg);
            Assert.True(info.BytesPerRecordAvg > 5);
            Assert.True(info.OffsetsPerTermAvg > 0.8);
            Assert.Equal(8, info.OffsetBitsPerRecordAvg);
            Assert.Equal(0, info.HashIndexingFailures);
            Assert.Equal(0, info.Indexing);
            Assert.Equal(1, info.PercentIndexed);
            Assert.Equal(4, info.NumberOfUses);
            Assert.Equal(7, info.GcStats.Count);
            Assert.Equal(4, info.CursorStats.Count);
        }
    }

    // TODO : fix with FT.CONFIG response change
    [SkipIfRedisTheory(Is.Enterprise, Comparison.GreaterThanOrEqual, "7.3.240")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestConfig(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Assert.True(ft.ConfigSet("TIMEOUT", "100"));
        Dictionary<string, string> configMap = ft.ConfigGet("*");
        Assert.Equal("100", configMap["TIMEOUT"].ToString());
    }

    // TODO : fix with FT.CONFIG response change
    [SkipIfRedisTheory(Is.Enterprise, Comparison.GreaterThanOrEqual, "7.3.240")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestConfigAsnyc(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Assert.True(await ft.ConfigSetAsync("TIMEOUT", "100"));
        Dictionary<string, string> configMap = await ft.ConfigGetAsync("*");
        Assert.Equal("100", configMap["TIMEOUT"].ToString());
    }

    // TODO : fix with FT.CONFIG response change
    [SkipIfRedisTheory(Is.Enterprise, Comparison.GreaterThanOrEqual, "7.3.240")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void configOnTimeout(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Assert.True(ft.ConfigSet("ON_TIMEOUT", "fail"));
        Assert.Equal("fail", ft.ConfigGet("ON_TIMEOUT")["ON_TIMEOUT"]);

        try { ft.ConfigSet("ON_TIMEOUT", "null"); } catch (RedisServerException) { }
    }

    // TODO : fix with FT.CONFIG response change
    [SkipIfRedisTheory(Is.Enterprise, Comparison.GreaterThanOrEqual, "7.3.240")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public async Task configOnTimeoutAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Assert.True(await ft.ConfigSetAsync("ON_TIMEOUT", "fail"));
        Assert.Equal("fail", (await ft.ConfigGetAsync("ON_TIMEOUT"))["ON_TIMEOUT"]);

        try { ft.ConfigSet("ON_TIMEOUT", "null"); } catch (RedisServerException) { }
    }

    // TODO : fix with FT.CONFIG response change
    [SkipIfRedisTheory(Is.Enterprise, Comparison.GreaterThanOrEqual, "7.3.240")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestDialectConfig(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        // confirm default
        var result = ft.ConfigGet("DEFAULT_DIALECT");
        Assert.Equal("1", result["DEFAULT_DIALECT"]); // TODO: should be "1" ?

        Assert.True(ft.ConfigSet("DEFAULT_DIALECT", "2"));
        Assert.Equal("2", ft.ConfigGet("DEFAULT_DIALECT")["DEFAULT_DIALECT"]);
        // try { ft.ConfigSet("DEFAULT_DIALECT", "0"); } catch (RedisServerException) { }
        // try { ft.ConfigSet("DEFAULT_DIALECT", "3"); } catch (RedisServerException) { }

        // Assert.Throws<RedisServerException>(() => ft.ConfigSet("DEFAULT_DIALECT", "0"));
        // Assert.Throws<RedisServerException>(() => ft.ConfigSet("DEFAULT_DIALECT", "3"));

        // Restore to default
        Assert.True(ft.ConfigSet("DEFAULT_DIALECT", "1"));
    }

    // TODO : fix with FT.CONFIG response change
    [SkipIfRedisTheory(Is.Enterprise, Comparison.GreaterThanOrEqual, "7.3.240")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestDialectConfigAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        // confirm default
        var result = await ft.ConfigGetAsync("DEFAULT_DIALECT");
        Assert.Equal("1", result["DEFAULT_DIALECT"]); // TODO: should be "1" ?

        Assert.True(await ft.ConfigSetAsync("DEFAULT_DIALECT", "2"));
        Assert.Equal("2", (await ft.ConfigGetAsync("DEFAULT_DIALECT"))["DEFAULT_DIALECT"]);
        // try { await ft.ConfigSetAsync("DEFAULT_DIALECT", "0"); } catch (RedisServerException) { }
        // try { await ft.ConfigSetAsync("DEFAULT_DIALECT", "3"); } catch (RedisServerException) { }

        // Assert.Throws<RedisServerException>(() => ft.ConfigSet("DEFAULT_DIALECT", "0"));
        // Assert.Throws<RedisServerException>(() => ft.ConfigSet("DEFAULT_DIALECT", "3"));

        // Restore to default
        Assert.True(ft.ConfigSet("DEFAULT_DIALECT", "1"));
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestCursor(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new();
        sc.AddTextField("name", 1.0, sortable: true);
        sc.AddNumericField("count", sortable: true);
        ft.Create(index, FTCreateParams.CreateParams(), sc);
        AddDocument(db, new Document("data1").Set("name", "abc").Set("count", 10));
        AddDocument(db, new Document("data2").Set("name", "def").Set("count", 5));
        AddDocument(db, new Document("data3").Set("name", "def").Set("count", 25));
        AssertDatabaseSize(db, 3);

        AggregationRequest r = new AggregationRequest()
            .GroupBy("@name", Reducers.Sum("@count").As("sum"))
            .SortBy(10, SortedField.Desc("@sum"))
            .Cursor(1, 3000);

        // actual search
        AggregationResult res = ft.Aggregate(index, r);
        Row? row = res.GetRow(0);
        Assert.NotNull(row);
        Assert.Equal("def", row.Value.GetString("name"));
        Assert.Equal(30, row.Value.GetLong("sum"));
        Assert.Equal(30.0, row.Value.GetDouble("sum"));

        Assert.Equal(0L, row.Value.GetLong("nosuchcol"));
        Assert.Equal(0.0, row.Value.GetDouble("nosuchcol"));
        Assert.Null(row.Value.GetString("nosuchcol"));

        res = ft.CursorRead(res, 1);
        Row? row2 = res.GetRow(0);

        Assert.NotNull(row2);
        Assert.Equal("abc", row2.Value.GetString("name"));
        Assert.Equal(10, row2.Value.GetLong("sum"));

        Assert.True(ft.CursorDel(res));

        var ex = Assert.Throws<RedisServerException>(() => ft.CursorRead(res, 1));
        Assert.Contains("Cursor not found", ex.Message, StringComparison.OrdinalIgnoreCase);

        _ = new AggregationRequest()
            .GroupBy("@name", Reducers.Sum("@count").As("sum"))
            .SortBy(10, SortedField.Desc("@sum"))
            .Cursor(1, 1000);

        await Task.Delay(1000).ConfigureAwait(false);

        ex = Assert.Throws<RedisServerException>(() => ft.CursorRead(res, 1));
        Assert.Contains("Cursor not found", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestCursorEnumerable(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new();
        sc.AddTextField("name", 1.0, sortable: true);
        sc.AddNumericField("count", sortable: true);
        ft.Create(index, FTCreateParams.CreateParams(), sc);
        AddDocument(db, new Document("data1").Set("name", "abc").Set("count", 10));
        AddDocument(db, new Document("data2").Set("name", "def").Set("count", 5));
        AddDocument(db, new Document("data3").Set("name", "def").Set("count", 25));
        AssertDatabaseSize(db, 3);

        AggregationRequest r = new AggregationRequest()
            .GroupBy("@name", Reducers.Sum("@count").As("sum"))
            .SortBy(10, SortedField.Desc("@sum"))
            .Cursor(1, 3000);

        // actual search
        using var iter = ft.AggregateEnumerable(index, r).GetEnumerator();
        Assert.True(iter.MoveNext());
        var row = iter.Current;
        Assert.Equal("def", row.GetString("name"));
        Assert.Equal(30, row.GetLong("sum"));
        Assert.Equal(30.0, row.GetDouble("sum"));

        Assert.Equal(0L, row.GetLong("nosuchcol"));
        Assert.Equal(0.0, row.GetDouble("nosuchcol"));
        Assert.Null(row.GetString("nosuchcol"));

        Assert.True(iter.MoveNext());
        row = iter.Current;
        Assert.Equal("abc", row.GetString("name"));
        Assert.Equal(10, row.GetLong("sum"));
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestCursorAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new();
        sc.AddTextField("name", 1.0, sortable: true);
        sc.AddNumericField("count", sortable: true);
        ft.Create(index, FTCreateParams.CreateParams(), sc);
        AddDocument(db, new Document("data1").Set("name", "abc").Set("count", 10));
        AddDocument(db, new Document("data2").Set("name", "def").Set("count", 5));
        AddDocument(db, new Document("data3").Set("name", "def").Set("count", 25));
        await AssertDatabaseSizeAsync(db, 3);

        AggregationRequest r = new AggregationRequest()
            .GroupBy("@name", Reducers.Sum("@count").As("sum"))
            .SortBy(10, SortedField.Desc("@sum"))
            .Cursor(1, 3000);

        // actual search
        AggregationResult res = await ft.AggregateAsync(index, r);
        Row? row = res.GetRow(0);
        Assert.NotNull(row);
        Assert.Equal("def", row.Value.GetString("name"));
        Assert.Equal(30, row.Value.GetLong("sum"));
        Assert.Equal(30.0, row.Value.GetDouble("sum"));

        Assert.Equal(0L, row.Value.GetLong("nosuchcol"));
        Assert.Equal(0.0, row.Value.GetDouble("nosuchcol"));
        Assert.Null(row.Value.GetString("nosuchcol"));

        res = await ft.CursorReadAsync(res, 1);
        Row? row2 = res.GetRow(0);

        Assert.NotNull(row2);
        Assert.Equal("abc", row2.Value.GetString("name"));
        Assert.Equal(10, row2.Value.GetLong("sum"));

        Assert.True(await ft.CursorDelAsync(res));

        var ex = await Assert.ThrowsAsync<RedisServerException>(async () => await ft.CursorReadAsync(res, 1));
        Assert.Contains("Cursor not found", ex.Message, StringComparison.OrdinalIgnoreCase);

        _ = new AggregationRequest()
            .GroupBy("@name", Reducers.Sum("@count").As("sum"))
            .SortBy(10, SortedField.Desc("@sum"))
            .Cursor(1, 1000);

        await Task.Delay(1000).ConfigureAwait(false);

        ex = await Assert.ThrowsAsync<RedisServerException>(async () => await ft.CursorReadAsync(res, 1));
        Assert.Contains("Cursor not found", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestCursorEnumerableAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new();
        sc.AddTextField("name", 1.0, sortable: true);
        sc.AddNumericField("count", sortable: true);
        ft.Create(index, FTCreateParams.CreateParams(), sc);
        AddDocument(db, new Document("data1").Set("name", "abc").Set("count", 10));
        AddDocument(db, new Document("data2").Set("name", "def").Set("count", 5));
        AddDocument(db, new Document("data3").Set("name", "def").Set("count", 25));
        await AssertDatabaseSizeAsync(db, 3);

        AggregationRequest r = new AggregationRequest()
            .GroupBy("@name", Reducers.Sum("@count").As("sum"))
            .SortBy(10, SortedField.Desc("@sum"))
            .Cursor(1, 3000);

        // actual search
        await using var iter = ft.AggregateAsyncEnumerable(index, r).GetAsyncEnumerator();
        Assert.True(await iter.MoveNextAsync());
        var row = iter.Current;
        Assert.Equal("def", row.GetString("name"));
        Assert.Equal(30, row.GetLong("sum"));
        Assert.Equal(30.0, row.GetDouble("sum"));

        Assert.Equal(0L, row.GetLong("nosuchcol"));
        Assert.Equal(0.0, row.GetDouble("nosuchcol"));
        Assert.Null(row.GetString("nosuchcol"));

        Assert.True(await iter.MoveNextAsync());
        row = iter.Current;
        Assert.Equal("abc", row.GetString("name"));
        Assert.Equal(10, row.GetLong("sum"));
    }

    [SkipIfRedisTheory(Is.Enterprise)]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestAggregationGroupBy(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();

        // Creating the index definition and schema
        ft.Create("idx", new(), new Schema().AddNumericField("random_num")
                                                           .AddTextField("title")
                                                           .AddTextField("body")
                                                           .AddTextField("parent"));

        // Indexing a document
        AddDocument(db, "search", new(){
        { "title", "RediSearch" },
        { "body", "Redisearch impements a search engine on top of redis" },
        { "parent", "redis" },
        { "random_num", 10 }});

        AddDocument(db, "ai", new()
        {
        { "title", "RedisAI" },
        { "body", "RedisAI executes Deep Learning/Machine Learning models and managing their data." },
        { "parent", "redis" },
        { "random_num", 3 }});

        AddDocument(db, "json", new()
        {
        { "title", "RedisJson" },
        { "body", "RedisJSON implements ECMA-404 The JSON Data Interchange Standard as a native data type." },
        { "parent", "redis" },
        { "random_num", 8 }});

        var req = new AggregationRequest("redis").GroupBy("@parent", Reducers.Count());
        var res = ft.Aggregate("idx", req).GetRow(0);
        Assert.True(res.ContainsKey("parent"));
        Assert.Equal("redis", res["parent"]);
        // Assert.Equal(res["__generated_aliascount"], "3");

        req = new AggregationRequest("redis").GroupBy("@parent", Reducers.CountDistinct("@title"));
        res = ft.Aggregate("idx", req).GetRow(0);
        Assert.Equal("redis", res["parent"]);
        Assert.Equal(3, res.GetLong("__generated_aliascount_distincttitle"));

        req = new AggregationRequest("redis").GroupBy("@parent", Reducers.CountDistinctish("@title"));
        res = ft.Aggregate("idx", req).GetRow(0);
        Assert.Equal("redis", res["parent"]);
        Assert.Equal(3, res.GetLong("__generated_aliascount_distinctishtitle"));

        req = new AggregationRequest("redis").GroupBy("@parent", Reducers.Sum("@random_num"));
        res = ft.Aggregate("idx", req).GetRow(0);
        Assert.Equal("redis", res["parent"]);
        Assert.Equal(21, res.GetLong("__generated_aliassumrandom_num")); // 10+8+3

        req = new AggregationRequest("redis").GroupBy("@parent", Reducers.Min("@random_num"));
        res = ft.Aggregate("idx", req).GetRow(0);
        Assert.Equal("redis", res["parent"]);
        Assert.Equal(3, res.GetLong("__generated_aliasminrandom_num")); // min(10,8,3)

        req = new AggregationRequest("redis").GroupBy("@parent", Reducers.Max("@random_num"));
        res = ft.Aggregate("idx", req).GetRow(0);
        Assert.Equal("redis", res["parent"]);
        Assert.Equal(10, res.GetLong("__generated_aliasmaxrandom_num")); // max(10,8,3)

        req = new AggregationRequest("redis").GroupBy("@parent", Reducers.Avg("@random_num"));
        res = ft.Aggregate("idx", req).GetRow(0);
        Assert.Equal("redis", res["parent"]);
        Assert.Equal(7, res.GetLong("__generated_aliasavgrandom_num")); // (10+3+8)/3

        req = new AggregationRequest("redis").GroupBy("@parent", Reducers.StdDev("@random_num"));
        res = ft.Aggregate("idx", req).GetRow(0);
        Assert.Equal("redis", res["parent"]);
        Assert.Equal(3.60555127546, res.GetDouble("__generated_aliasstddevrandom_num"));

        req = new AggregationRequest("redis").GroupBy(
            "@parent", Reducers.Quantile("@random_num", 0.5));
        res = ft.Aggregate("idx", req).GetRow(0);
        Assert.Equal("redis", res["parent"]);
        Assert.Equal(8, res.GetLong("__generated_aliasquantilerandom_num,0.5"));  // median of 3,8,10

        req = new AggregationRequest("redis").GroupBy(
            "@parent", Reducers.ToList("@title"));
        var rawRes = ft.Aggregate("idx", req);
        res = rawRes.GetRow(0);
        Assert.Equal("redis", res["parent"]);
        // TODO: complete this assert after handling multi bulk reply
        var expected = new List<object> { "RediSearch", "RedisAI", "RedisJson" };
        var actual = (List<object>)res.Get("__generated_aliastolisttitle");
        Assert.True(!expected.Except(actual).Any() && expected.Count == actual.Count);

        req = new AggregationRequest("redis").GroupBy(
            "@parent", Reducers.FirstValue("@title").As("first"));
        var agg = ft.Aggregate("idx", req);
        Log($"results: {agg.TotalResults}");
        for (int i = 0; i < agg.TotalResults; i++)
        {
            Log($"parent: {agg.GetRow(i)["parent"]}, first: {agg.GetRow(i)["first"]}");
        }
        res = agg.GetRow(0);
        Assert.Equal("redis", res["parent"]);
        Assert.Equal("RediSearch", res["first"]);

        req = new AggregationRequest("redis").GroupBy(
            "@parent", Reducers.RandomSample("@title", 2).As("random"));
        res = ft.Aggregate("idx", req).GetRow(0);
        Assert.Equal("redis", res["parent"]);
        // TODO: complete this assert after handling multi bulk reply
        actual = (List<object>)res.Get("random");
        Assert.Equal(2, actual.Count);
        List<string> possibleValues = ["RediSearch", "RedisAI", "RedisJson"];
        Assert.Contains(actual[0].ToString(), possibleValues);
        Assert.Contains(actual[1].ToString(), possibleValues);

        req = new AggregationRequest("redis")
                .Load(new FieldName("__key"))
                .GroupBy("@parent", Reducers.ToList("__key").As("docs"));

        res = db.FT().Aggregate("idx", req).GetRow(0);
        actual = (List<object>)res.Get("docs");
        expected = ["ai", "search", "json"];
        Assert.True(!expected.Except(actual).Any() && expected.Count == actual.Count);
    }


    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestDictionary(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();

        Assert.Equal(3L, ft.DictAdd("dict", "bar", "foo", "hello world"));
        AssertDatabaseSize(db, 0);
        var dumResult = ft.DictDump("dict");
        int i = 0;
        Assert.Equal("bar", dumResult[i++].ToString());
        Assert.Equal("foo", dumResult[i++].ToString());
        Assert.Equal("hello world", dumResult[i].ToString());

        Assert.Equal(3L, ft.DictDel("dict", "foo", "bar", "hello world"));
        AssertDatabaseSize(db, 0);
        Assert.Empty(ft.DictDump("dict"));
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestDropIndex(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new Schema().AddTextField("title", 1.0);
        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));

        Dictionary<string, object> fields = new();
        fields.Add("title", "hello world");
        for (int i = 0; i < 100; i++)
        {
            AddDocument(db, $"doc{i}", fields);
        }

        SearchResult res = ft.Search(index, new("hello world"));
        Assert.Equal(100, res.TotalResults);

        Assert.True(ft.DropIndex(index));

        try
        {
            ft.Search(index, new("hello world"));
            //fail("Index should not exist.");
        }
        catch (RedisServerException ex)
        {
            Assert.Contains("no such index", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        AssertDatabaseSize(db, 100);
    }

    private int DatabaseSize(IDatabase db) => DatabaseSize(db, out _);

    private int DatabaseSize(IDatabase db, out int replicaCount)
    {
        replicaCount = 0;
        var count = 0L;
        foreach (var server in db.Multiplexer.GetServers())
        {
            if (server.IsReplica || !server.IsConnected)
            {
                replicaCount++;
            }
            else
            {
                count += server.DatabaseSize();
            }
        }

        return checked((int)count);
    }

    private async Task<int> DatabaseSizeAsync(IDatabase db)
    {
        var count = 0L;
        foreach (var server in db.Multiplexer.GetServers())
        {
            if (!server.IsReplica && server.IsConnected)
            {
                count += await server.DatabaseSizeAsync();
            }
        }

        return checked((int)count);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestDropIndexAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new Schema().AddTextField("title", 1.0);
        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));

        Dictionary<string, object> fields = new();
        fields.Add("title", "hello world");
        for (int i = 0; i < 100; i++)
        {
            AddDocument(db, $"doc{i}", fields);
        }

        SearchResult res = ft.Search(index, new("hello world"));
        Assert.Equal(100, res.TotalResults);

        Assert.True(await ft.DropIndexAsync(index));

        try
        {
            ft.Search(index, new("hello world"));
            //fail("Index should not exist.");
        }
        catch (RedisServerException ex)
        {
            Assert.Contains("no such index", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        AssertDatabaseSize(db, 100);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void dropIndexDD(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new Schema().AddTextField("title", 1.0);
        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));

        Dictionary<string, object> fields = new();
        fields.Add("title", "hello world");
        for (int i = 0; i < 100; i++)
        {
            AddDocument(db, $"doc{i}", fields);
        }

        SearchResult res = ft.Search(index, new("hello world"));
        Assert.Equal(100, res.TotalResults);

        Assert.True(ft.DropIndex(index, true));

        RedisResult[] keys = (RedisResult[])db.Execute("KEYS", "*")!;
        Assert.Empty(keys);
        AssertDatabaseSize(db, 0);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task dropIndexDDAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new Schema().AddTextField("title", 1.0);
        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));

        Dictionary<string, object> fields = new();
        fields.Add("title", "hello world");
        for (int i = 0; i < 100; i++)
        {
            AddDocument(db, $"doc{i}", fields);
        }

        SearchResult res = ft.Search(index, new("hello world"));
        Assert.Equal(100, res.TotalResults);

        Assert.True(await ft.DropIndexAsync(index, true));

        RedisResult[] keys = (RedisResult[])db.Execute("KEYS", "*")!;
        Assert.Empty(keys);
        AssertDatabaseSize(db, 0);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestDictionaryAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();

        Assert.Equal(3L, await ft.DictAddAsync("dict", "bar", "foo", "hello world"));
        await AssertDatabaseSizeAsync(db, 0);
        var dumResult = await ft.DictDumpAsync("dict");
        int i = 0;
        Assert.Equal("bar", dumResult[i++].ToString());
        Assert.Equal("foo", dumResult[i++].ToString());
        Assert.Equal("hello world", dumResult[i].ToString());

        Assert.Equal(3L, await ft.DictDelAsync("dict", "foo", "bar", "hello world"));
        await AssertDatabaseSizeAsync(db, 0);
        Assert.Empty((await ft.DictDumpAsync("dict")));
    }

    readonly string explainQuery = "@f3:f3_val @f2:f2_val @f1:f1_val";
    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestExplain(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new Schema()
            .AddTextField("f1", 1.0)
            .AddTextField("f2", 1.0)
            .AddTextField("f3", 1.0);
        ft.Create(index, FTCreateParams.CreateParams(), sc);

        string res = ft.Explain(index, explainQuery);
        Assert.NotNull(res);
        Assert.False(res.Length == 0);

        // Test with dialect:
        res = ft.Explain(index, explainQuery, 2);
        Assert.NotNull(res);
        Assert.False(res.Length == 0);


    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestExplainAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new Schema()
            .AddTextField("f1", 1.0)
            .AddTextField("f2", 1.0)
            .AddTextField("f3", 1.0);
        ft.Create(index, FTCreateParams.CreateParams(), sc);


        string res = await ft.ExplainAsync(index, explainQuery);
        Assert.NotNull(res);
        Assert.False(res.Length == 0);

        // Test with dialect:
        res = await ft.ExplainAsync(index, explainQuery, 2);
        Assert.NotNull(res);
        Assert.False(res.Length == 0);
    }

    [SkipIfRedisTheory(Is.Enterprise)]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestExplainCli(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT(2);
        Schema sc = new Schema()
            .AddTextField("f1", 1.0)
            .AddTextField("f2", 1.0)
            .AddTextField("f3", 1.0);
        ft.Create(index, FTCreateParams.CreateParams(), sc);


        var res = ft.ExplainCli(index, explainQuery);
        Assert.NotNull(res);
        Assert.False(res.Length == 0);

        // Test with dialect (ovveride the dialect 2):
        res = ft.ExplainCli(index, explainQuery, 1);
        Assert.NotNull(res);
        Assert.False(res.Length == 0);
    }

    [SkipIfRedisTheory(Is.Enterprise)]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestExplainCliAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT(2);
        Schema sc = new Schema()
            .AddTextField("f1", 1.0)
            .AddTextField("f2", 1.0)
            .AddTextField("f3", 1.0);
        ft.Create(index, FTCreateParams.CreateParams(), sc);


        var res = await ft.ExplainCliAsync(index, explainQuery);
        Assert.NotNull(res);
        Assert.False(res.Length == 0);

        // Test with dialect (ovveride the dialect 2):
        res = await ft.ExplainCliAsync(index, explainQuery, 1);
        Assert.NotNull(res);
        Assert.False(res.Length == 0);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestExplainWithDefaultDialect(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT(1);
        Schema sc = new Schema()
            .AddTextField("f1", 1.0)
            .AddTextField("f2", 1.0)
            .AddTextField("f3", 1.0);
        ft.Create(index, FTCreateParams.CreateParams(), sc);

        String res = ft.Explain(index, "@f3:f3_val @f2:f2_val @f1:f1_val");
        Assert.NotNull(res);
        Assert.False(res.Length == 0);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestExplainWithDefaultDialectAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT(1);
        Schema sc = new Schema()
            .AddTextField("f1", 1.0)
            .AddTextField("f2", 1.0)
            .AddTextField("f3", 1.0);
        ft.Create(index, FTCreateParams.CreateParams(), sc);

        String res = await ft.ExplainAsync(index, "@f3:f3_val @f2:f2_val @f1:f1_val");
        Assert.NotNull(res);
        Assert.False(res.Length == 0);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestSynonym(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        var sc = new Schema().AddTextField("name", 1.0).AddTextField("addr", 1.0);
        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));

        long group1 = 345L;
        long group2 = 789L;
        string group1_str = group1.ToString();
        string group2_str = group2.ToString();
        Assert.True(ft.SynUpdate(index, group1_str, false, "girl", "baby"));
        Assert.True(ft.SynUpdate(index, group1_str, false, "child"));
        Assert.True(ft.SynUpdate(index, group2_str, false, "child"));

        Dictionary<string, List<string>> dump = ft.SynDump(index);

        Dictionary<string, List<string>> expected = new();
        expected.Add("girl", [group1_str]);
        expected.Add("baby", [group1_str]);
        expected.Add("child", [group1_str, group2_str]);
        Assert.Equal(expected, dump);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestSynonymAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        var sc = new Schema().AddTextField("name", 1.0).AddTextField("addr", 1.0);
        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));

        long group1 = 345L;
        long group2 = 789L;
        string group1_str = group1.ToString();
        string group2_str = group2.ToString();
        Assert.True(await ft.SynUpdateAsync(index, group1_str, false, "girl", "baby"));
        Assert.True(await ft.SynUpdateAsync(index, group1_str, false, "child"));
        Assert.True(await ft.SynUpdateAsync(index, group2_str, false, "child"));

        Dictionary<string, List<string>> dump = await ft.SynDumpAsync(index);

        Dictionary<string, List<string>> expected = new();
        expected.Add("girl", [group1_str]);
        expected.Add("baby", [group1_str]);
        expected.Add("child", [group1_str, group2_str]);
        Assert.Equal(expected, dump);
    }

    [Fact]
    public void TestModulePrefixs()
    {
        var redis = GetConnection();
        IDatabase db1 = redis.GetDatabase();
        IDatabase db2 = redis.GetDatabase();

        var ft1 = db1.FT();
        var ft2 = db2.FT();

        Assert.NotEqual(ft1.GetHashCode(), ft2.GetHashCode());
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task GetTagFieldSyncAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new Schema()
            .AddTextField("title", 1.0)
            .AddTagField("category");

        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));
        Dictionary<string, object> fields1 = new();
        fields1.Add("title", "hello world");
        fields1.Add("category", "red");
        //    assertTrue(client.AddDocument(db, "foo", fields1));
        AddDocument(db, "foo", fields1);
        Dictionary<string, object> fields2 = new();
        fields2.Add("title", "hello world");
        fields2.Add("category", "blue");
        //    assertTrue(client.AddDocument(db, "bar", fields2));
        AddDocument(db, "bar", fields2);
        Dictionary<string, object> fields3 = new();
        fields3.Add("title", "hello world");
        fields3.Add("category", "green,yellow");
        //    assertTrue(client.AddDocument(db, "baz", fields3));
        AddDocument(db, "baz", fields3);
        Dictionary<string, object> fields4 = new();
        fields4.Add("title", "hello world");
        fields4.Add("category", "orange;purple");
        //    assertTrue(client.AddDocument(db, "qux", fields4));
        AddDocument(db, "qux", fields4);

        Assert.Equal(1, ft.Search(index, new("@category:{red}")).TotalResults);
        Assert.Equal(1, ft.Search(index, new("@category:{blue}")).TotalResults);
        Assert.Equal(1, ft.Search(index, new("hello @category:{red}")).TotalResults);
        Assert.Equal(1, ft.Search(index, new("hello @category:{blue}")).TotalResults);
        Assert.Equal(1, ft.Search(index, new("@category:{yellow}")).TotalResults);
        Assert.Equal(0, ft.Search(index, new("@category:{purple}")).TotalResults);
        Assert.Equal(1, ft.Search(index, new("@category:{orange\\;purple}")).TotalResults);
        Assert.Equal(4, ft.Search(index, new("hello")).TotalResults);

        var SyncRes = ft.TagVals(index, "category");
        int i = 0;
        Assert.Equal("blue", SyncRes[i++].ToString());
        Assert.Equal("green", SyncRes[i++].ToString());
        Assert.Equal("orange;purple", SyncRes[i++].ToString());
        Assert.Equal("red", SyncRes[i++].ToString());
        Assert.Equal("yellow", SyncRes[i++].ToString());

        var AsyncRes = await ft.TagValsAsync(index, "category");
        i = 0;
        Assert.Equal("blue", SyncRes[i++].ToString());
        Assert.Equal("green", SyncRes[i++].ToString());
        Assert.Equal("orange;purple", SyncRes[i++].ToString());
        Assert.Equal("red", SyncRes[i++].ToString());
        Assert.Equal("yellow", SyncRes[i++].ToString());
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestGetTagFieldWithNonDefaultSeparatorSyncAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        Schema sc = new Schema()
            .AddTextField("title", 1.0)
            .AddTagField("category", separator: ";");

        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));
        Dictionary<string, object> fields1 = new();
        fields1.Add("title", "hello world");
        fields1.Add("category", "red");
        //    assertTrue(client.AddDocument(db, "foo", fields1));
        AddDocument(db, "foo", fields1);
        Dictionary<string, object> fields2 = new();
        fields2.Add("title", "hello world");
        fields2.Add("category", "blue");
        //    assertTrue(client.AddDocument(db, "bar", fields2));
        AddDocument(db, "bar", fields2);
        Dictionary<string, object> fields3 = new();
        fields3.Add("title", "hello world");
        fields3.Add("category", "green;yellow");
        AddDocument(db, "baz", fields3);
        //    assertTrue(client.AddDocument(db, "baz", fields3));
        Dictionary<string, object> fields4 = new();
        fields4.Add("title", "hello world");
        fields4.Add("category", "orange,purple");
        //    assertTrue(client.AddDocument(db, "qux", fields4));
        AddDocument(db, "qux", fields4);

        Assert.Equal(1, ft.Search(index, new("@category:{red}")).TotalResults);
        Assert.Equal(1, ft.Search(index, new("@category:{blue}")).TotalResults);
        Assert.Equal(1, ft.Search(index, new("hello @category:{red}")).TotalResults);
        Assert.Equal(1, ft.Search(index, new("hello @category:{blue}")).TotalResults);
        Assert.Equal(1, ft.Search(index, new("hello @category:{yellow}")).TotalResults);
        Assert.Equal(0, ft.Search(index, new("@category:{purple}")).TotalResults);
        Assert.Equal(1, ft.Search(index, new("@category:{orange\\,purple}")).TotalResults);
        Assert.Equal(4, ft.Search(index, new("hello")).TotalResults);

        var SyncRes = ft.TagVals(index, "category");
        int i = 0;
        Assert.Equal("blue", SyncRes[i++].ToString());
        Assert.Equal("green", SyncRes[i++].ToString());
        Assert.Equal("orange,purple", SyncRes[i++].ToString());
        Assert.Equal("red", SyncRes[i++].ToString());
        Assert.Equal("yellow", SyncRes[i++].ToString());

        var AsyncRes = await ft.TagValsAsync(index, "category");
        i = 0;
        Assert.Equal("blue", SyncRes[i++].ToString());
        Assert.Equal("green", SyncRes[i++].ToString());
        Assert.Equal("orange,purple", SyncRes[i++].ToString());
        Assert.Equal("red", SyncRes[i++].ToString());
        Assert.Equal("yellow", SyncRes[i++].ToString());
    }


    [Fact]
    public void TestFTCreateParamsCommandBuilder()
    {
        IDatabase db = GetCleanDatabase();
        var ft = db.FT();
        Schema sc = new Schema()
            .AddTextField("title", 1.0)
            .AddTagField("category", separator: ";");

        var ftCreateParams = FTCreateParams.CreateParams().On(IndexDataType.JSON)
                                                          .AddPrefix("doc:")
                                                          .Filter("@category:{red}")
                                                          .Language("English")
                                                          .LanguageField("play")
                                                          .Score(1.0)
                                                          .ScoreField("chapter")
                                                          .PayloadField("txt")
                                                          .MaxTextFields()
                                                          .NoOffsets()
                                                          .Temporary(10)
                                                          .NoHighlights()
                                                          .NoFields()
                                                          .NoFreqs()
                                                          .Stopwords(new[] { "foo", "bar" })
                                                          .SkipInitialScan();

        var builedCommand = SearchCommandBuilder.Create(index, ftCreateParams, sc);
        var expectedArgs = new object[] { "TEST_INDEX", "ON", "JSON", "PREFIX", 1,
                                           "doc:", "FILTER", "@category:{red}", "LANGUAGE",
                                           "English", "LANGUAGE_FIELD", "play", "SCORE", 1,
                                           "SCORE_FIELD", "chapter", "PAYLOAD_FIELD", "txt",
                                           "MAXTEXTFIELDS", "NOOFFSETS", "TEMPORARY", 10,
                                           "NOHL", "NOFIELDS", "NOFREQS", "STOPWORDS", 2,
                                           "foo", "bar", "SKIPINITIALSCAN", "SCHEMA", "title",
                                           "TEXT", "category", "TAG", "SEPARATOR", ";" };

        for (int i = 0; i < expectedArgs.Length; i++)
        {
            Assert.Equal(expectedArgs[i].ToString(), builedCommand.Args[i].ToString());
        }
        Assert.Equal("FT.CREATE", builedCommand.Command.ToString());
    }

    [Fact]
    public void TestFTCreateParamsCommandBuilderNoStopwords()
    {
        IDatabase db = GetCleanDatabase();
        var ft = db.FT();
        Schema sc = new Schema()
            .AddTextField("title", 1.0)
            .AddTagField("category", separator: ";");

        var ftCreateParams = FTCreateParams.CreateParams().NoStopwords();

        var expectedArgs = new object[] { "TEST_INDEX", "STOPWORDS", 0, "SCHEMA", "title",
                                          "TEXT", "category", "TAG", "SEPARATOR", ";" };
        var builedCommand = SearchCommandBuilder.Create(index, ftCreateParams, sc);


        for (int i = 0; i < expectedArgs.Length; i++)
        {
            Assert.Equal(expectedArgs[i].ToString(), builedCommand.Args[i].ToString());
        }
        Assert.Equal("FT.CREATE", builedCommand.Command.ToString());
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestFilters(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        // Create the index with the same fields as in the original test
        var sc = new Schema()
            .AddTextField("txt")
            .AddNumericField("num")
            .AddGeoField("loc");
        ft.Create("idx", new(), sc);

        // Add the two documents to the index
        AddDocument(db, "doc1", new()
        {
                { "txt", "foo bar" },
                { "num", "3.141" },
                { "loc", "-0.441,51.458" }
            });
        AddDocument(db, "doc2", new()
        {
                { "txt", "foo baz" },
                { "num", "2" },
                { "loc", "-0.1,51.2" }
            });
        // WaitForIndex(client, ft.IndexName ?? "idx");

        // Test numerical filter
        var q1 = new Query("foo").AddFilter(new Query.NumericFilter("num", 0, 2));
        var q2 = new Query("foo").AddFilter(new Query.NumericFilter("num", 2, true, double.MaxValue, false));
        q1.NoContent = q2.NoContent = true;
        var res1 = ft.Search("idx", q1);
        var res2 = ft.Search("idx", q2);

        Assert.Equal(1, res1.TotalResults);
        Assert.Equal(1, res2.TotalResults);
        Assert.Equal("doc2", res1.Documents[0].Id);
        Assert.Equal("doc1", res2.Documents[0].Id);

        // Test geo filter
        q1 = new Query("foo").AddFilter(new Query.GeoFilter("loc", -0.44, 51.45, 10, Query.GeoFilter.KILOMETERS));
        q2 = new Query("foo").AddFilter(new Query.GeoFilter("loc", -0.44, 51.45, 100, Query.GeoFilter.KILOMETERS));
        q1.NoContent = q2.NoContent = true;
        res1 = ft.Search("idx", q1);
        res2 = ft.Search("idx", q2);

        Assert.Equal(1, res1.TotalResults);
        Assert.Equal(2, res2.TotalResults);
        Assert.Equal("doc1", res1.Documents[0].Id);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestFiltersAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        // Create the index with the same fields as in the original test
        var sc = new Schema()
            .AddTextField("txt")
            .AddNumericField("num")
            .AddGeoField("loc");
        await ft.CreateAsync("idx", new(), sc);

        // Add the two documents to the index
        AddDocument(db, "doc1", new()
        {
                { "txt", "foo bar" },
                { "num", "3.141" },
                { "loc", "-0.441,51.458" }
            });
        AddDocument(db, "doc2", new()
        {
                { "txt", "foo baz" },
                { "num", "2" },
                { "loc", "-0.1,51.2" }
            });
        // WaitForIndex(client, ft.IndexName ?? "idx");

        // Test numerical filter
        var q1 = new Query("foo").AddFilter(new Query.NumericFilter("num", 0, 2));
        var q2 = new Query("foo").AddFilter(new Query.NumericFilter("num", 2, true, double.MaxValue, false));
        q1.NoContent = q2.NoContent = true;
        var res1 = await ft.SearchAsync("idx", q1);
        var res2 = await ft.SearchAsync("idx", q2);

        Assert.Equal(1, res1.TotalResults);
        Assert.Equal(1, res2.TotalResults);
        Assert.Equal("doc2", res1.Documents[0].Id);
        Assert.Equal("doc1", res2.Documents[0].Id);

        // Test geo filter
        q1 = new Query("foo").AddFilter(new Query.GeoFilter("loc", -0.44, 51.45, 10, Query.GeoFilter.KILOMETERS));
        q2 = new Query("foo").AddFilter(new Query.GeoFilter("loc", -0.44, 51.45, 100, Query.GeoFilter.KILOMETERS));
        q1.NoContent = q2.NoContent = true;
        res1 = await ft.SearchAsync("idx", q1);
        res2 = await ft.SearchAsync("idx", q2);

        Assert.Equal(1, res1.TotalResults);
        Assert.Equal(2, res2.TotalResults);
        Assert.Equal("doc1", res1.Documents[0].Id);
    }

    [Fact]
    public void TestQueryCommandBuilder()
    {
        var testQuery = new Query("foo").HighlightFields(new Query.HighlightTags("<b>", "</b>"), "txt")
                                            .SetVerbatim()
                                            .SetNoStopwords()
                                            .SetWithScores()
                                            .SetPayload("txt")
                                            .SetLanguage("English")
                                            .SetScorer("TFIDF")
                                            //.SetExplainScore()
                                            .SetWithPayloads()
                                            .SetSortBy("txt", true)
                                            .Limit(0, 11)
                                            .SummarizeFields(20, 3, ";", "txt")
                                            .LimitKeys("key1", "key2")
                                            .LimitFields("txt")
                                            .ReturnFields("txt")
                                            .AddParam("name", "value")
                                            .Dialect(1)
                                            .Slop(0)
                                            .Timeout(1000)
                                            .SetInOrder()
                                            .SetExpander("myexpander");
        var buildCommand = SearchCommandBuilder.Search("idx", testQuery);
        var expectedArgs = new List<object> {"idx",
                                             "foo",
                                             "VERBATIM",
                                             "NOSTOPWORDS",
                                             "WITHSCORES",
                                             "WITHPAYLOADS",
                                             "LANGUAGE",
                                             "English",
                                             "SCORER",
                                             "TFIDF",
                                             "INFIELDS",
                                             "1",
                                             "txt",
                                             "SORTBY",
                                             "txt",
                                             "ASC",
                                             "PAYLOAD",
                                             "txt",
                                             "LIMIT",
                                             "0",
                                             "11",
                                             "HIGHLIGHT",
                                             "FIELDS",
                                             "1",
                                             "txt",
                                             "TAGS",
                                             "<b>",
                                             "</b>",
                                             "SUMMARIZE",
                                             "FIELDS",
                                             "1",
                                             "txt",
                                             "FRAGS",
                                             "3",
                                             "LEN",
                                             "20",
                                             "SEPARATOR",
                                             ";",
                                             "INKEYS",
                                             "2",
                                             "key1",
                                             "key2",
                                             "RETURN",
                                             "1",
                                             "txt",
                                             "PARAMS",
                                             "2",
                                             "name",
                                             "value",
                                             "DIALECT",
                                             "1",
                                             "SLOP",
                                             "0",
                                             "TIMEOUT",
                                             "1000",
                                             "INORDER",
                                             "EXPANDER",
                                             "myexpander"};

        for (int i = 0; i < buildCommand.Args.Count(); i++)
        {
            Assert.Equal(expectedArgs[i].ToString(), buildCommand.Args[i].ToString());
        }
        Assert.Equal("FT.SEARCH", buildCommand.Command);
        // test that the command not throw an exception:
        var db = GetCleanDatabase();
        var ft = db.FT();
        ft.Create("idx", new(), new Schema().AddTextField("txt"));
        var res = ft.Search("idx", testQuery);
        Assert.Empty(res.Documents);
    }

    [Fact]
    public void TestQueryCommandBuilderReturnField()
    {
        var testQuery = new Query("foo").HighlightFields("txt")
                                            .ReturnFields(new FieldName("txt"))
                                            .SetNoContent();


        var buildCommand = SearchCommandBuilder.Search("idx", testQuery);
        var expectedArgs = new List<object> {"idx",
                                             "foo",
                                             "NOCONTENT",
                                             "HIGHLIGHT",
                                             "FIELDS",
                                             "1",
                                             "txt",
                                             "RETURN",
                                             "1",
                                             "txt"};

        Assert.Equal(expectedArgs.Count(), buildCommand.Args.Count());
        for (int i = 0; i < buildCommand.Args.Count(); i++)
        {
            Assert.Equal(expectedArgs[i].ToString(), buildCommand.Args[i].ToString());
        }
        Assert.Equal("FT.SEARCH", buildCommand.Command);

        // test that the command not throw an exception:
        var db = GetCleanDatabase();
        var ft = db.FT();
        ft.Create("idx", new(), new Schema().AddTextField("txt"));
        var res = ft.Search("idx", testQuery);
        Assert.Empty(res.Documents);
    }

    [Fact]
    public void TestQueryCommandBuilderScore()
    {
        // TODO: write better test for scores and payloads
        IDatabase db = GetCleanDatabase();
        var ft = db.FT();

        db.Execute("JSON.SET", (RedisKey)"doc:1", "$", "[{\"arr\": [1, 2, 3]}, {\"val\": \"hello\"}, {\"val\": \"world\"}]");
        db.Execute("FT.CREATE", "idx", "ON", "JSON", "PREFIX", "1", "doc:", "SCHEMA", "$..arr", "AS", "arr", "NUMERIC", "$..val", "AS", "val", "TEXT");
        // sleep:
        Thread.Sleep(2000);

        var res = ft.Search("idx", new Query("*").ReturnFields("arr", "val").SetWithScores().SetPayload("arr"));
        Assert.Equal(1, res.TotalResults);
    }

    [Fact]
    public void TestFieldsCommandBuilder()
    {
        IDatabase db = GetCleanDatabase();
        var ft = db.FT();
        // Create the index with the same fields as in the original test
        var sc = new Schema()
            .AddTextField(FieldName.Of("txt"), 1.0, true, true, true, "dm:en", true, true)
            .AddNumericField(FieldName.Of("num"), true, true)
            .AddGeoField(FieldName.Of("loc"), true, true)
            .AddTagField(FieldName.Of("tag"), true, true, true, ";", true, true)
            .AddVectorField("vec", VectorField.VectorAlgo.FLAT, new() { { "dim", 10 } });
        var buildCommand = SearchCommandBuilder.Create("idx", new(), sc);
        var expectedArgs = new List<object> {
            "idx",
            "SCHEMA",
            "txt",
            "TEXT",
            "NOSTEM",
            "NOINDEX",
            "PHONETIC",
            "dm:en",
            "WITHSUFFIXTRIE",
            "UNF",
            "SORTABLE",
            "num",
            "NUMERIC",
            "NOINDEX",
            "SORTABLE",
            "loc",
            "GEO",
            "NOINDEX",
            "SORTABLE",
            "tag",
            "TAG",
            "NOINDEX",
            "WITHSUFFIXTRIE",
            "SEPARATOR",
            ";",
            "CASESENSITIVE",
            "UNF",
            "SORTABLE",
            "vec",
            "VECTOR",
            "FLAT",
            "2",
            "dim",
            "10"
        };

        Assert.Equal("FT.CREATE", buildCommand.Command);
        for (int i = 0; i < expectedArgs.Count; i++)
        {
            Assert.Equal(expectedArgs[i], buildCommand.Args[i].ToString());
        }
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestLimit(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();

        ft.Create("idx", new(), new Schema().AddTextField("t1").AddTextField("t2"));
        Document doc1 = new("doc1", new() { { "t1", "a" }, { "t2", "b" } });
        Document doc2 = new("doc2", new() { { "t1", "b" }, { "t2", "a" } });
        AddDocument(db, doc1);
        AddDocument(db, doc2);
        AssertDatabaseSize(db, 2);

        var req = new AggregationRequest("*").SortBy("@t1").Limit(1);
        var res = ft.Aggregate("idx", req);

        Assert.Single(res.GetResults());
        Assert.Equal("a", res.GetResults()[0]["t1"].ToString());
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestLimitAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();

        ft.Create("idx", new(), new Schema().AddTextField("t1").AddTextField("t2"));
        Document doc1 = new("doc1", new() { { "t1", "a" }, { "t2", "b" } });
        Document doc2 = new("doc2", new() { { "t1", "b" }, { "t2", "a" } });
        AddDocument(db, doc1);
        AddDocument(db, doc2);
        await AssertDatabaseSizeAsync(db, 2);

        var req = new AggregationRequest("*").SortBy("@t1").Limit(1, 1);
        var res = await ft.AggregateAsync("idx", req);

        Assert.Single(res.GetResults());
        Assert.Equal("b", res.GetResults()[0]["t1"].ToString());
    }

    [Fact]
    public void Test_List()
    {
        IDatabase db = GetCleanDatabase();
        var ft = db.FT();
        Assert.Equal(ft._List(), []);
    }

    [Fact]
    public async Task Test_ListAsync()
    {
        IDatabase db = GetCleanDatabase();
        var ft = db.FT();
        Assert.Equal(await ft._ListAsync(), []);
    }

    [Fact]
    public void TestVectorCount_Issue70()
    {
        var schema = new Schema().AddVectorField("fieldTest", VectorField.VectorAlgo.HNSW, new()
        {
            ["TYPE"] = "FLOAT32",
            ["DIM"] = "128",
            ["DISTANCE_METRIC"] = "COSINE"
        });

        var actual = SearchCommandBuilder.Create("test", new(), schema);
        var expected = new List<object>()
        {
            "test",
            "SCHEMA",
            "fieldTest",
            "VECTOR",
            "HNSW",
            "6",
            "TYPE",
            "FLOAT32",
            "DIM",
            "128",
            "DISTANCE_METRIC",
            "COSINE"
        };
        Assert.Equal("FT.CREATE", actual.Command);
        for (int i = 0; i < actual.Args.Length; i++)
        {
            Assert.Equal(expected[i].ToString(), actual.Args[i].ToString());
        }
        Assert.Equal(expected.Count(), actual.Args.Length);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void VectorSimilaritySearch(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        var json = db.JSON();

        json.Set("vec:1", "$", "{\"vector\":[1,1,1,1]}");
        json.Set("vec:2", "$", "{\"vector\":[2,2,2,2]}");
        json.Set("vec:3", "$", "{\"vector\":[3,3,3,3]}");
        json.Set("vec:4", "$", "{\"vector\":[4,4,4,4]}");

        var schema = new Schema().AddVectorField(FieldName.Of("$.vector").As("vector"), VectorField.VectorAlgo.FLAT, new()
        {
            ["TYPE"] = "FLOAT32",
            ["DIM"] = "4",
            ["DISTANCE_METRIC"] = "L2",
        });

        var idxDef = new FTCreateParams().On(IndexDataType.JSON).Prefix("vec:");
        Assert.True(ft.Create("vss_idx", idxDef, schema));

        float[] vec = [2, 2, 2, 2];
        byte[] queryVec = MemoryMarshal.Cast<float, byte>(vec).ToArray();

        AssertDatabaseSize(db, 4);
        var query = new Query("*=>[KNN 3 @vector $query_vec]")
                            .AddParam("query_vec", queryVec)
                            .SetSortBy("__vector_score")
                            .Dialect(2);
        var res = ft.Search("vss_idx", query);

        Assert.Equal(3, res.TotalResults);

        Assert.Equal("vec:2", res.Documents[0].Id.ToString());

        Assert.Equal(0, res.Documents[0]["__vector_score"]);

        var jsonRes = res.ToJson();
        Assert.Equal("{\"vector\":[2,2,2,2]}", jsonRes[0]);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void QueryingVectorFields(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        var json = db.JSON();

        var schema = new Schema().AddVectorField("v", VectorField.VectorAlgo.HNSW, new()
        {
            ["TYPE"] = "FLOAT32",
            ["DIM"] = "2",
            ["DISTANCE_METRIC"] = "L2",
        });

        ft.Create("idx", new(), schema);

        db.HashSet("a", "v", "aaaaaaaa");
        db.HashSet("b", "v", "aaaabaaa");
        db.HashSet("c", "v", "aaaaabaa");

        AssertDatabaseSize(db, 3);
        var q = new Query("*=>[KNN 2 @v $vec]").ReturnFields("__v_score").Dialect(2);
        var res = ft.Search("idx", q.AddParam("vec", "aaaaaaaa"));
        Assert.Equal(2, res.TotalResults);
    }

    [Fact]
    public async Task TestVectorFieldJson_Issue102Async()
    {
        IDatabase db = GetCleanDatabase();
        var ft = db.FT();
        var json = db.JSON();

        // JSON.SET 1 $ '{"vec":[1,2,3,4]}'
        await json.SetAsync("1", "$", "{\"vec\":[1,2,3,4]}");

        // FT.CREATE my_index ON JSON SCHEMA $.vec as vector VECTOR FLAT 6 TYPE FLOAT32 DIM 4 DISTANCE_METRIC L2
        var schema = new Schema().AddVectorField(FieldName.Of("$.vec").As("vector"), VectorField.VectorAlgo.FLAT, new()
        {
            ["TYPE"] = "FLOAT32",
            ["DIM"] = "4",
            ["DISTANCE_METRIC"] = "L2",
        });

        Assert.True(await ft.CreateAsync("my_index", new FTCreateParams().On(IndexDataType.JSON), schema));
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestQueryAddParam_DefaultDialect(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT(2);

        var sc = new Schema().AddNumericField("numval");
        Assert.True(ft.Create("idx", new(), sc));

        db.HashSet("1", "numval", 1);
        db.HashSet("2", "numval", 2);
        db.HashSet("3", "numval", 3);

        AssertDatabaseSize(db, 3);
        Query query = new Query("@numval:[$min $max]").AddParam("min", 1).AddParam("max", 2);
        var res = ft.Search("idx", query);
        Assert.Equal(2, res.TotalResults);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestQueryAddParam_DefaultDialectAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT(2);

        var sc = new Schema().AddNumericField("numval");
        Assert.True(await ft.CreateAsync("idx", new(), sc));

        db.HashSet("1", "numval", 1);
        db.HashSet("2", "numval", 2);
        db.HashSet("3", "numval", 3);

        await AssertDatabaseSizeAsync(db, 3);
        Query query = new Query("@numval:[$min $max]").AddParam("min", 1).AddParam("max", 2);
        var res = await ft.SearchAsync("idx", query);
        Assert.Equal(2, res.TotalResults);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestQueryParamsWithParams_DefaultDialect(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT(2);

        var sc = new Schema().AddNumericField("numval");
        Assert.True(ft.Create("idx", new(), sc));

        db.HashSet("1", "numval", 1);
        db.HashSet("2", "numval", 2);
        db.HashSet("3", "numval", 3);

        AssertDatabaseSize(db, 3);
        Query query = new Query("@numval:[$min $max]").AddParam("min", 1).AddParam("max", 2);
        var res = ft.Search("idx", query);
        Assert.Equal(2, res.TotalResults);

        var paramValue = new Dictionary<string, object>()
        {
            ["min"] = 1,
            ["max"] = 2
        };
        query = new("@numval:[$min $max]");
        res = ft.Search("idx", query.Params(paramValue));
        Assert.Equal(2, res.TotalResults);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestBasicSpellCheck(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();

        ft.Create(index, new(), new Schema().AddTextField("name").AddTextField("body"));

        db.HashSet("doc1", [new("name", "name1"), new("body", "body1")]);
        db.HashSet("doc1", [new("name", "name2"), new("body", "body2")]);
        db.HashSet("doc1", [new("name", "name2"), new("body", "name2")]);

        AssertDatabaseSize(db, 1);
        var reply = ft.SpellCheck(index, "name");
        Assert.Single(reply.Keys);
        Assert.Equal("name", reply.Keys.First());
        Assert.Equal(1, reply["name"]["name1"]);
        Assert.Equal(2, reply["name"]["name2"]);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestBasicSpellCheckAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();

        ft.Create(index, new(), new Schema().AddTextField("name").AddTextField("body"));

        db.HashSet("doc1", [new("name", "name1"), new("body", "body1")]);
        db.HashSet("doc1", [new("name", "name2"), new("body", "body2")]);
        db.HashSet("doc1", [new("name", "name2"), new("body", "name2")]);

        await AssertDatabaseSizeAsync(db, 1);
        var reply = await ft.SpellCheckAsync(index, "name");
        Assert.Single(reply.Keys);
        Assert.Equal("name", reply.Keys.First());
        Assert.Equal(1, reply["name"]["name1"]);
        Assert.Equal(2, reply["name"]["name2"]);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestCrossTermDictionary(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();

        ft.Create(index, new(), new Schema().AddTextField("name").AddTextField("body"));
        ft.DictAdd("slang", "timmies", "toque", "toonie", "serviette", "kerfuffle", "chesterfield");
        var expected = new Dictionary<string, Dictionary<string, double>>()
        {
            ["tooni"] = new()
            {
                ["toonie"] = 0d
            }
        };

        AssertDatabaseSize(db, 0);

        Assert.Equal(expected, ft.SpellCheck(index,
                                             "Tooni toque kerfuffle",
                                             new FTSpellCheckParams()
                                             .IncludeTerm("slang")
                                             .ExcludeTerm("slang")));
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestCrossTermDictionaryAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();

        ft.Create(index, new(), new Schema().AddTextField("name").AddTextField("body"));
        ft.DictAdd("slang", "timmies", "toque", "toonie", "serviette", "kerfuffle", "chesterfield");
        var expected = new Dictionary<string, Dictionary<string, double>>()
        {
            ["tooni"] = new()
            {
                ["toonie"] = 0d
            }
        };

        AssertDatabaseSize(db, 0);
        Assert.Equal(expected, await ft.SpellCheckAsync(index,
                                             "Tooni toque kerfuffle",
                                             new FTSpellCheckParams()
                                             .IncludeTerm("slang")
                                             .ExcludeTerm("slang")));
    }

    [Fact]
    public void TestDistanceBound()
    {
        IDatabase db = GetCleanDatabase();
        var ft = db.FT();

        ft.Create(index, new(), new Schema().AddTextField("name").AddTextField("body"));
        // distance suppose to be between 1 and 4
        Assert.Throws<RedisServerException>(() => ft.SpellCheck(index, "name", new FTSpellCheckParams().Distance(0)));
    }

    [Fact]
    public async Task TestDistanceBoundAsync()
    {
        IDatabase db = GetCleanDatabase();
        var ft = db.FT();

        ft.Create(index, new(), new Schema().AddTextField("name").AddTextField("body"));
        // distance suppose to be between 1 and 4
        await Assert.ThrowsAsync<RedisServerException>(async () => await ft.SpellCheckAsync(index, "name", new FTSpellCheckParams().Distance(0)));
    }

    [Fact]
    public void TestDialectBound()
    {
        IDatabase db = GetCleanDatabase();
        var ft = db.FT();

        ft.Create(index, new(), new Schema().AddTextField("t"));
        // dialect 0 is not valid
        Assert.Throws<RedisServerException>(() => ft.SpellCheck(index, "name", new FTSpellCheckParams().Dialect(0)));
    }

    [Fact]
    public async Task TestDialectBoundAsync()
    {
        IDatabase db = GetCleanDatabase();
        var ft = db.FT();

        ft.Create(index, new(), new Schema().AddTextField("t"));
        // dialect 0 is not valid
        await Assert.ThrowsAsync<RedisServerException>(async () => await ft.SpellCheckAsync(index, "name", new FTSpellCheckParams().Dialect(0)));
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestQueryParamsWithParams_DefaultDialectAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT(2);

        var sc = new Schema().AddNumericField("numval");
        Assert.True(await ft.CreateAsync("idx", new(), sc));

        db.HashSet("1", "numval", 1);
        db.HashSet("2", "numval", 2);
        db.HashSet("3", "numval", 3);

        AssertDatabaseSize(db, 3);
        Query query = new Query("@numval:[$min $max]").AddParam("min", 1).AddParam("max", 2);
        var res = await ft.SearchAsync("idx", query);
        Assert.Equal(2, res.TotalResults);

        var paramValue = new Dictionary<string, object>()
        {
            ["min"] = 1,
            ["max"] = 2
        };
        query = new("@numval:[$min $max]");
        res = await ft.SearchAsync("idx", query.Params(paramValue));
        Assert.Equal(2, res.TotalResults);
    }

    readonly string key = "SugTestKey";

    [Fact]
    public void TestAddAndGetSuggestion()
    {
        IDatabase db = GetCleanDatabase();
        var ft = db.FT();

        string suggestion = "ANOTHER_WORD";
        string noMatch = "_WORD MISSED";

        Assert.True(ft.SugAdd(key, suggestion, 1d) > 0);
        Assert.True(ft.SugAdd(key, noMatch, 1d) > 0);

        // test that with a partial part of that string will have the entire word returned
        Assert.Single(ft.SugGet(key, suggestion.Substring(0, 3), true, max: 5));

        // turn off fuzzy start at second word no hit
        Assert.Empty(ft.SugGet(key, noMatch.Substring(1, 6), false, max: 5));

        // my attempt to trigger the fuzzy by 1 character
        Assert.Single(ft.SugGet(key, noMatch.Substring(1, 6), true, max: 5));
    }

    [Fact]
    public async Task TestAddAndGetSuggestionAsync()
    {
        IDatabase db = GetCleanDatabase();
        var ft = db.FT();

        string suggestion = "ANOTHER_WORD";
        string noMatch = "_WORD MISSED";

        Assert.True(await ft.SugAddAsync(key, suggestion, 1d) > 0);
        Assert.True(await ft.SugAddAsync(key, noMatch, 1d) > 0);

        // test that with a partial part of that string will have the entire word returned
        Assert.Single(await ft.SugGetAsync(key, suggestion.Substring(0, 3), true, max: 5));

        // turn off fuzzy start at second word no hit
        Assert.Empty((await ft.SugGetAsync(key, noMatch.Substring(1, 6), false, max: 5)));

        // my attempt to trigger the fuzzy by 1 character
        Assert.Single(await ft.SugGetAsync(key, noMatch.Substring(1, 6), true, max: 5));
    }

    [Fact]
    public void AddSuggestionIncrAndGetSuggestionFuzzy()
    {
        IDatabase db = GetCleanDatabase();
        var ft = db.FT();
        string suggestion = "TOPIC OF WORDS";

        // test can add a suggestion string
        Assert.True(ft.SugAdd(key, suggestion, 1d, increment: true) > 0);

        // test that the partial part of that string will be returned using fuzzy
        Assert.Equal(suggestion, ft.SugGet(key, suggestion.Substring(0, 3))[0]);
    }

    [Fact]
    public async Task AddSuggestionIncrAndGetSuggestionFuzzyAsync()
    {
        IDatabase db = GetCleanDatabase();
        var ft = db.FT();
        string suggestion = "TOPIC OF WORDS";

        // test can add a suggestion string
        Assert.True(await ft.SugAddAsync(key, suggestion, 1d, increment: true) > 0);

        // test that the partial part of that string will be returned using fuzzy
        Assert.Equal(suggestion, (await ft.SugGetAsync(key, suggestion.Substring(0, 3)))[0]);
    }

    [Fact]
    public void getSuggestionScores()
    {
        IDatabase db = GetCleanDatabase();
        var ft = db.FT();
        ft.SugAdd(key, "COUNT_ME TOO", 1);
        ft.SugAdd(key, "COUNT", 1);
        ft.SugAdd(key, "COUNT_ANOTHER", 1);

        string noScoreOrPayload = "COUNT NO PAYLOAD OR COUNT";
        Assert.True(ft.SugAdd(key, noScoreOrPayload, 1, increment: true) > 1);

        var result = ft.SugGetWithScores(key, "COU");
        Assert.Equal(4, result.Count);
        foreach (var tuple in result)
        {
            Assert.True(tuple.Item2 < .999);
        }
    }

    [Fact]
    public async Task getSuggestionScoresAsync()
    {
        IDatabase db = GetCleanDatabase();
        var ft = db.FT();
        await ft.SugAddAsync(key, "COUNT_ME TOO", 1);
        await ft.SugAddAsync(key, "COUNT", 1);
        await ft.SugAddAsync(key, "COUNT_ANOTHER", 1);

        string noScoreOrPayload = "COUNT NO PAYLOAD OR COUNT";
        Assert.True(await ft.SugAddAsync(key, noScoreOrPayload, 1, increment: true) > 1);

        var result = await ft.SugGetWithScoresAsync(key, "COU");
        Assert.Equal(4, result.Count);
        foreach (var tuple in result)
        {
            Assert.True(tuple.Item2 < .999);
        }
    }

    [Fact]
    public void getSuggestionMax()
    {
        IDatabase db = GetCleanDatabase();
        var ft = db.FT();
        ft.SugAdd(key, "COUNT_ME TOO", 1);
        ft.SugAdd(key, "COUNT", 1);
        ft.SugAdd(key, "COUNTNO PAYLOAD OR COUNT", 1);

        // test that with a partial part of that string will have the entire word returned
        Assert.Equal(3, ft.SugGetWithScores(key, "COU", true, max: 10).Count);
        Assert.Equal(2, ft.SugGetWithScores(key, "COU", true, max: 2).Count);
    }

    [Fact]
    public async Task getSuggestionMaxAsync()
    {
        IDatabase db = GetCleanDatabase();
        var ft = db.FT();
        await ft.SugAddAsync(key, "COUNT_ME TOO", 1);
        await ft.SugAddAsync(key, "COUNT", 1);
        await ft.SugAddAsync(key, "COUNTNO PAYLOAD OR COUNT", 1);

        // test that with a partial part of that string will have the entire word returned
        Assert.Equal(3, (await ft.SugGetWithScoresAsync(key, "COU", true, max: 10)).Count);
        Assert.Equal(2, (await ft.SugGetWithScoresAsync(key, "COU", true, max: 2)).Count);
    }

    [Fact]
    public void getSuggestionNoHit()
    {
        IDatabase db = GetCleanDatabase();
        var ft = db.FT();
        ft.SugAdd(key, "NO WORD", 0.4);

        Assert.Empty(ft.SugGetWithScores(key, "DIF"));
        Assert.Empty(ft.SugGet(key, "DIF"));
    }

    [Fact]
    public async Task getSuggestionNoHitAsync()
    {
        IDatabase db = GetCleanDatabase();
        var ft = db.FT();
        await ft.SugAddAsync(key, "NO WORD", 0.4);

        Assert.Empty((await ft.SugGetWithScoresAsync(key, "DIF")));
        Assert.Empty((await ft.SugGetAsync(key, "DIF")));
    }

    [Fact]
    public void getSuggestionLengthAndDeleteSuggestion()
    {
        IDatabase db = GetCleanDatabase();
        var ft = db.FT();
        ft.SugAdd(key, "TOPIC OF WORDS", 1, increment: true);
        ft.SugAdd(key, "ANOTHER ENTRY", 1, increment: true);
        Assert.Equal(2L, ft.SugLen(key));

        Assert.True(ft.SugDel(key, "ANOTHER ENTRY"));
        Assert.Equal(1L, ft.SugLen(key));

        Assert.False(ft.SugDel(key, "ANOTHER ENTRY"));
        Assert.Equal(1L, ft.SugLen(key));

        Assert.False(ft.SugDel(key, "ANOTHER ENTRY THAT IS NOT PRESENT"));
        Assert.Equal(1L, ft.SugLen(key));

        ft.SugAdd(key, "LAST ENTRY", 1);
        Assert.Equal(2L, ft.SugLen(key));
    }

    [Fact]
    public async Task getSuggestionLengthAndDeleteSuggestionAsync()
    {
        IDatabase db = GetCleanDatabase();
        var ft = db.FT();
        await ft.SugAddAsync(key, "TOPIC OF WORDS", 1, increment: true);
        await ft.SugAddAsync(key, "ANOTHER ENTRY", 1, increment: true);
        Assert.Equal(2L, await ft.SugLenAsync(key));

        Assert.True(await ft.SugDelAsync(key, "ANOTHER ENTRY"));
        Assert.Equal(1L, await ft.SugLenAsync(key));

        Assert.False(await ft.SugDelAsync(key, "ANOTHER ENTRY"));
        Assert.Equal(1L, await ft.SugLenAsync(key));

        Assert.False(await ft.SugDelAsync(key, "ANOTHER ENTRY THAT IS NOT PRESENT"));
        Assert.Equal(1L, await ft.SugLenAsync(key));

        ft.SugAdd(key, "LAST ENTRY", 1);
        Assert.Equal(2L, await ft.SugLenAsync(key));
    }

    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "7.9")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestProfileSearch(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();

        Schema sc = new Schema().AddTextField("t1", 1.0).AddTextField("t2", 1.0);
        Assert.True(ft.Create(index, new(), sc));

        db.HashSet("doc1", [
            new("t1", "foo"),
                                new("t2", "bar")
        ]);

        var profile = ft.ProfileOnSearch(index, new("foo"));
        // Iterators profile={Type=TEXT, Time=0.0, Term=foo, Counter=1, Size=1}
        var info = (RedisResult[])profile.Item2.Info!;
        int shardsIndex = Array.FindIndex(info, item => item.ToString() == "Shards");
        int coordinatorIndex = Array.FindIndex(info, item => item.ToString() == "Coordinator");
        CustomAssertions.GreaterThan(shardsIndex, -1);
        CustomAssertions.GreaterThan(coordinatorIndex, -1);
    }

    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "7.9")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestProfileSearchAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();

        Schema sc = new Schema().AddTextField("t1", 1.0).AddTextField("t2", 1.0);
        Assert.True(ft.Create(index, new(), sc));

        db.HashSet("doc1", [
            new("t1", "foo"),
                                new("t2", "bar")
        ]);

        var profile = await ft.ProfileOnSearchAsync(index, new("foo"));
        var info = (RedisResult[])profile.Item2.Info!;
        int shardsIndex = Array.FindIndex(info, item => item.ToString() == "Shards");
        int coordinatorIndex = Array.FindIndex(info, item => item.ToString() == "Coordinator");
        CustomAssertions.GreaterThan(shardsIndex, -1);
        CustomAssertions.GreaterThan(coordinatorIndex, -1);
    }

    [SkipIfRedisTheory(Is.Enterprise, Comparison.GreaterThanOrEqual, "7.9")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestProfileSearch_WithoutCoordinator(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();

        Schema sc = new Schema().AddTextField("t1", 1.0).AddTextField("t2", 1.0);
        Assert.True(ft.Create(index, new(), sc));

        db.HashSet("doc1", [
            new("t1", "foo"),
                                new("t2", "bar")
        ]);

        var profile = ft.ProfileSearch(index, new("foo"));
        var info = profile.Item2;
        CustomAssertions.GreaterThan(info.Count, 4);
    }

    [SkipIfRedisTheory(Is.Enterprise, Comparison.GreaterThanOrEqual, "7.9")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestProfileSearchAsync_WithoutCoordinator(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();

        Schema sc = new Schema().AddTextField("t1", 1.0).AddTextField("t2", 1.0);
        Assert.True(ft.Create(index, new(), sc));

        db.HashSet("doc1", [
            new("t1", "foo"),
                                new("t2", "bar")
        ]);

        var profile = await ft.ProfileSearchAsync(index, new("foo"));
        var info = profile.Item2;
        CustomAssertions.GreaterThan(info.Count, 4);
    }

    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "7.9")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestProfile(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();

        ft.Create(index, new Schema().AddTextField("t")); // Calling FT.CREATR without FTCreateParams
        db.HashSet("1", "t", "hello");
        db.HashSet("2", "t", "world");

        // check using Query
        var q = new Query("hello|world").SetNoContent();
        var profileSearch = ft.ProfileOnSearch(index, q);
        var searchRes = profileSearch.Item1;
        var searchDet = (RedisResult[])profileSearch.Item2.Info!;

        Assert.Equal(2, searchRes.Documents.Count);
        int shardsIndex = Array.FindIndex(searchDet, item => item.ToString() == "Shards");
        int coordinatorIndex = Array.FindIndex(searchDet, item => item.ToString() == "Coordinator");
        CustomAssertions.GreaterThan(shardsIndex, -1);
        CustomAssertions.GreaterThan(coordinatorIndex, -1);

        // check using AggregationRequest
        var aggReq = new AggregationRequest("*").Load(FieldName.Of("t")).Apply("startswith(@t, 'hel')", "prefix");
        var profileAggregate = ft.ProfileOnAggregate(index, aggReq);
        var aggregateRes = profileAggregate.Item1;
        var aggregateDet = (RedisResult[])profileAggregate.Item2.Info!;

        Assert.Equal(2, aggregateRes.TotalResults);
        shardsIndex = Array.FindIndex(aggregateDet, item => item.ToString() == "Shards");
        coordinatorIndex = Array.FindIndex(aggregateDet, item => item.ToString() == "Coordinator");
        CustomAssertions.GreaterThan(shardsIndex, -1);
        CustomAssertions.GreaterThan(coordinatorIndex, -1);
    }

    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "7.9")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestProfileAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();

        await ft.CreateAsync(index, new Schema().AddTextField("t")); // Calling FT.CREATR without FTCreateParams
        db.HashSet("1", "t", "hello");
        db.HashSet("2", "t", "world");

        // check using Query
        var q = new Query("hello|world").SetNoContent();
        var profileSearch = await ft.ProfileOnSearchAsync(index, q);
        var searchRes = profileSearch.Item1;
        var searchDet = (RedisResult[])profileSearch.Item2.Info!;

        Assert.Equal(2, searchRes.Documents.Count);
        int shardsIndex = Array.FindIndex(searchDet, item => item.ToString() == "Shards");
        int coordinatorIndex = Array.FindIndex(searchDet, item => item.ToString() == "Coordinator");
        CustomAssertions.GreaterThan(shardsIndex, -1);
        CustomAssertions.GreaterThan(coordinatorIndex, -1);

        // check using AggregationRequest
        var aggReq = new AggregationRequest("*").Load(FieldName.Of("t")).Apply("startswith(@t, 'hel')", "prefix");
        var profileAggregate = await ft.ProfileOnAggregateAsync(index, aggReq);
        var aggregateRes = profileAggregate.Item1;
        var aggregateDet = (RedisResult[])profileAggregate.Item2.Info!;

        Assert.Equal(2, aggregateRes.TotalResults);
        shardsIndex = Array.FindIndex(aggregateDet, item => item.ToString() == "Shards");
        coordinatorIndex = Array.FindIndex(aggregateDet, item => item.ToString() == "Coordinator");
        CustomAssertions.GreaterThan(shardsIndex, -1);
        CustomAssertions.GreaterThan(coordinatorIndex, -1);
    }

    [SkipIfRedisTheory(Is.Enterprise, Comparison.GreaterThanOrEqual, "7.9")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestProfile_WithoutCoordinator(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();

        ft.Create(index, new Schema().AddTextField("t")); // Calling FT.CREATR without FTCreateParams
        db.HashSet("1", "t", "hello");
        db.HashSet("2", "t", "world");

        // check using Query
        var q = new Query("hello|world").SetNoContent();
        var profileSearch = ft.ProfileSearch(index, q);
        var searchRes = profileSearch.Item1;
        var searchDet = profileSearch.Item2;

        Assert.Equal(2, searchRes.Documents.Count);
        CustomAssertions.GreaterThan(searchDet.Count, 4);

        // check using AggregationRequest
        var aggReq = new AggregationRequest("*").Load(FieldName.Of("t")).Apply("startswith(@t, 'hel')", "prefix");
        var profileAggregate = ft.ProfileAggregate(index, aggReq);
        var aggregateRes = profileAggregate.Item1;
        var aggregateDet = profileAggregate.Item2;

        Assert.Equal(2, aggregateRes.TotalResults);
        CustomAssertions.GreaterThan(aggregateDet.Count, 4);
    }

    [SkipIfRedisTheory(Is.Enterprise, Comparison.GreaterThanOrEqual, "7.9")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestProfileAsync_WithoutCoordinator(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();

        await ft.CreateAsync(index, new Schema().AddTextField("t")); // Calling FT.CREATR without FTCreateParams
        db.HashSet("1", "t", "hello");
        db.HashSet("2", "t", "world");

        // check using Query
        var q = new Query("hello|world").SetNoContent();
        var profileSearch = await ft.ProfileSearchAsync(index, q);
        var searchRes = profileSearch.Item1;
        var searchDet = profileSearch.Item2;

        Assert.Equal(2, searchRes.Documents.Count);
        CustomAssertions.GreaterThan(searchDet.Count, 4);

        // check using AggregationRequest
        var aggReq = new AggregationRequest("*").Load(FieldName.Of("t")).Apply("startswith(@t, 'hel')", "prefix");
        var profileAggregate = await ft.ProfileAggregateAsync(index, aggReq);
        var aggregateRes = profileAggregate.Item1;
        var aggregateDet = profileAggregate.Item2;

        Assert.Equal(2, aggregateRes.TotalResults);
        CustomAssertions.GreaterThan(searchDet.Count, 4);
    }

    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "7.3.240")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestProfileIssue306(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();

        ft.Create(index, new Schema().AddTextField("t", sortable: true)); // Calling FT.CREATR without FTCreateParams
        db.HashSet("1", "t", "hello");
        db.HashSet("2", "t", "world");

        // check using Query
        var q = new Query("hello|world").SetNoContent();
        var profileSearch = ft.ProfileOnSearch(index, q);
        var searchRes = profileSearch.Item1;
        var searchDet = (RedisResult[])profileSearch.Item2.Info!;

        CustomAssertions.GreaterThan(searchDet.Length, 3);
        Assert.Equal(2, searchRes.Documents.Count);


        // check using AggregationRequest
        var aggReq = new AggregationRequest("*").Load(FieldName.Of("t")).Apply("startswith(@t, 'hel')", "prefix");
        var profileAggregate = ft.ProfileOnAggregate(index, aggReq);
        var aggregateRes = profileAggregate.Item1;
        var aggregateDet = (RedisResult[])profileAggregate.Item2.Info!;
        CustomAssertions.GreaterThan(aggregateDet.Length, 3);
        Assert.Equal(2, aggregateRes.TotalResults);
    }

    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "7.3.240")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestProfileAsyncIssue306(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();

        await ft.CreateAsync(index, new Schema().AddTextField("t", sortable: true)); // Calling FT.CREATR without FTCreateParams
        db.HashSet("1", "t", "hello");
        db.HashSet("2", "t", "world");

        // check using Query
        var q = new Query("hello|world").SetNoContent();
        var profileSearch = await ft.ProfileOnSearchAsync(index, q);
        var searchRes = profileSearch.Item1;
        var searchDet = (RedisResult[])profileSearch.Item2.Info!;

        CustomAssertions.GreaterThan(searchDet.Length, 3);
        Assert.Equal(2, searchRes.Documents.Count);

        // check using AggregationRequest
        var aggReq = new AggregationRequest("*").Load(FieldName.Of("t")).Apply("startswith(@t, 'hel')", "prefix");
        var profileAggregate = await ft.ProfileOnAggregateAsync(index, aggReq);
        var aggregateRes = profileAggregate.Item1;
        var aggregateDet = (RedisResult[])profileAggregate.Item2.Info!;
        CustomAssertions.GreaterThan(aggregateDet.Length, 3);
        Assert.Equal(2, aggregateRes.TotalResults);
    }

    [Fact]
    public void TestProfileCommandBuilder()
    {
        var search = SearchCommandBuilder.ProfileSearch("index", new(), true);
        var aggregate = SearchCommandBuilder.ProfileAggregate("index", new(), true);

        Assert.Equal("FT.PROFILE", search.Command);
        Assert.Equal("FT.PROFILE", aggregate.Command);
        Assert.Equal(["index", "SEARCH", "LIMITED", "QUERY", "*"], search.Args);
        Assert.Equal(["index", "AGGREGATE", "LIMITED", "QUERY", "*"], aggregate.Args);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void Issue175(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);

        SearchCommands ft = db.FT();

        var sortable = true;
        var ftParams = new FTCreateParams()
                .On(IndexDataType.JSON)
                .Prefix("doc:");
        var schema = new Schema().AddTagField("tag", sortable, false, false, "|")
                                 .AddTextField("text", 1, sortable);

        Assert.True(ft.Create("myIndex", ftParams, schema));
    }

    [SkipIfRedisTheory(Comparison.LessThan, "7.2.1")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void GeoShapeFilterSpherical(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();

        WKTReader reader = new();
        GeometryFactory factory = new();

        Assert.True(ft.Create(index, new Schema().AddGeoShapeField("geom", GeoShapeField.CoordinateSystem.SPHERICAL)));

        // Create polygons
        Polygon small = factory.CreatePolygon([
            new(34.9001, 29.7001),
            new(34.9001, 29.7100),
            new(34.9100, 29.7100),
            new(34.9100, 29.7001),
            new(34.9001, 29.7001)
        ]);
        db.HashSet("small", "geom", small.ToString());

        Polygon large = factory.CreatePolygon([
            new(34.9001, 29.7001),
            new(34.9001, 29.7200),
            new(34.9200, 29.7200),
            new(34.9200, 29.7001),
            new(34.9001, 29.7001)
        ]);
        db.HashSet("large", "geom", large.ToString());

        Polygon within = factory.CreatePolygon([
            new(34.9000, 29.7000),
            new(34.9000, 29.7150),
            new(34.9150, 29.7150),
            new(34.9150, 29.7000),
            new(34.9000, 29.7000)
        ]);

        var res = ft.Search(index, new Query($"@geom:[within $poly]").AddParam("poly", within.ToString()).Dialect(3));
        Assert.Equal(1, res.TotalResults);
        Assert.Single(res.Documents);
        Assert.Equal(small, reader.Read(res.Documents[0]["geom"].ToString()));

        Polygon contains = factory.CreatePolygon([
            new(34.9002, 29.7002),
            new(34.9002, 29.7050),
            new(34.9050, 29.7050),
            new(34.9050, 29.7002),
            new(34.9002, 29.7002)
        ]);

        res = ft.Search(index, new Query($"@geom:[contains $poly]").AddParam("poly", contains.ToString()).Dialect(3));
        Assert.Equal(2, res.TotalResults);
        Assert.Equal(2, res.Documents.Count);

        // Create a point
        Point point = factory.CreatePoint(new Coordinate(34.9010, 29.7010));
        db.HashSet("point", "geom", point.ToString());

        res = ft.Search(index, new Query($"@geom:[within $poly]").AddParam("poly", within.ToString()).Dialect(3));
        Assert.Equal(2, res.TotalResults);
        Assert.Equal(2, res.Documents.Count);
    }

    [SkipIfRedisTheory(Comparison.LessThan, "7.2.1")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task GeoShapeFilterSphericalAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();

        WKTReader reader = new();
        GeometryFactory factory = new();

        Assert.True(await ft.CreateAsync(index, new Schema().AddGeoShapeField("geom", GeoShapeField.CoordinateSystem.SPHERICAL)));

        // Create polygons
        Polygon small = factory.CreatePolygon([
            new(34.9001, 29.7001),
            new(34.9001, 29.7100),
            new(34.9100, 29.7100),
            new(34.9100, 29.7001),
            new(34.9001, 29.7001)
        ]);
        db.HashSet("small", "geom", small.ToString());

        Polygon large = factory.CreatePolygon([
            new(34.9001, 29.7001),
            new(34.9001, 29.7200),
            new(34.9200, 29.7200),
            new(34.9200, 29.7001),
            new(34.9001, 29.7001)
        ]);
        db.HashSet("large", "geom", large.ToString());

        Polygon within = factory.CreatePolygon([
            new(34.9000, 29.7000),
            new(34.9000, 29.7150),
            new(34.9150, 29.7150),
            new(34.9150, 29.7000),
            new(34.9000, 29.7000)
        ]);

        var res = await ft.SearchAsync(index, new Query($"@geom:[within $poly]").AddParam("poly", within.ToString()).Dialect(3));
        Assert.Equal(1, res.TotalResults);
        Assert.Single(res.Documents);
        Assert.Equal(small, reader.Read(res.Documents[0]["geom"].ToString()));

        Polygon contains = factory.CreatePolygon([
            new(34.9002, 29.7002),
            new(34.9002, 29.7050),
            new(34.9050, 29.7050),
            new(34.9050, 29.7002),
            new(34.9002, 29.7002)
        ]);

        res = await ft.SearchAsync(index, new Query($"@geom:[contains $poly]").AddParam("poly", contains.ToString()).Dialect(3));
        Assert.Equal(2, res.TotalResults);
        Assert.Equal(2, res.Documents.Count);

        // Create a point
        Point point = factory.CreatePoint(new Coordinate(34.9010, 29.7010));
        db.HashSet("point", "geom", point.ToString());

        res = await ft.SearchAsync(index, new Query($"@geom:[within $poly]").AddParam("poly", within.ToString()).Dialect(3));
        Assert.Equal(2, res.TotalResults);
        Assert.Equal(2, res.Documents.Count);
    }

    [SkipIfRedisTheory(Comparison.LessThan, "7.2.1")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void GeoShapeFilterFlat(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        WKTReader reader = new();
        GeometryFactory factory = new();

        Assert.True(ft.Create(index, new Schema().AddGeoShapeField("geom", GeoShapeField.CoordinateSystem.FLAT)));

        // polygon type
        Polygon small = factory.CreatePolygon([
            new(1, 1),
        new(1, 100), new(100, 100), new(100, 1), new(1, 1)
        ]);
        db.HashSet("small", "geom", small.ToString());

        Polygon large = factory.CreatePolygon([
            new(1, 1),
        new(1, 200), new(200, 200), new(200, 1), new(1, 1)
        ]);
        db.HashSet("large", "geom", large.ToString());

        // within condition
        Polygon within = factory.CreatePolygon([
            new(0, 0),
        new(0, 150), new(150, 150), new(150, 0), new(0, 0)
        ]);

        SearchResult res = ft.Search(index, new Query("@geom:[within $poly]").AddParam("poly", within.ToString()).Dialect(3));
        Assert.Equal(1, res.TotalResults);
        Assert.Single(res.Documents);
        Assert.Equal(small, reader.Read(res.Documents[0]["geom"].ToString()));

        // contains condition
        Polygon contains = factory.CreatePolygon([
            new(2, 2),
        new(2, 50), new(50, 50), new(50, 2), new(2, 2)
        ]);

        res = ft.Search(index, new Query("@geom:[contains $poly]").AddParam("poly", contains.ToString()).Dialect(3));
        Assert.Equal(2, res.TotalResults);
        Assert.Equal(2, res.Documents.Count);

        // point type
        Point point = factory.CreatePoint(new Coordinate(10, 10));
        db.HashSet("point", "geom", point.ToString());

        res = ft.Search(index, new Query("@geom:[within $poly]").AddParam("poly", within.ToString()).Dialect(3));
        Assert.Equal(2, res.TotalResults);
        Assert.Equal(2, res.Documents.Count);
    }

    [SkipIfRedisTheory(Comparison.LessThan, "7.2.1")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task GeoShapeFilterFlatAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();
        WKTReader reader = new();
        GeometryFactory factory = new();

        Assert.True(await ft.CreateAsync(index, new Schema().AddGeoShapeField("geom", GeoShapeField.CoordinateSystem.FLAT)));

        // polygon type
        Polygon small = factory.CreatePolygon([
            new(1, 1),
        new(1, 100), new(100, 100), new(100, 1), new(1, 1)
        ]);
        db.HashSet("small", "geom", small.ToString());

        Polygon large = factory.CreatePolygon([
            new(1, 1),
        new(1, 200), new(200, 200), new(200, 1), new(1, 1)
        ]);
        db.HashSet("large", "geom", large.ToString());

        // within condition
        Polygon within = factory.CreatePolygon([
            new(0, 0),
        new(0, 150), new(150, 150), new(150, 0), new(0, 0)
        ]);

        SearchResult res = await ft.SearchAsync(index, new Query("@geom:[within $poly]").AddParam("poly", within.ToString()).Dialect(3));
        Assert.Equal(1, res.TotalResults);
        Assert.Single(res.Documents);
        Assert.Equal(small, reader.Read(res.Documents[0]["geom"].ToString()));

        // contains condition
        Polygon contains = factory.CreatePolygon([
            new(2, 2),
        new(2, 50), new(50, 50), new(50, 2), new(2, 2)
        ]);

        res = await ft.SearchAsync(index, new Query("@geom:[contains $poly]").AddParam("poly", contains.ToString()).Dialect(3));
        Assert.Equal(2, res.TotalResults);
        Assert.Equal(2, res.Documents.Count);

        // point type
        Point point = factory.CreatePoint(new Coordinate(10, 10));
        db.HashSet("point", "geom", point.ToString());

        res = await ft.SearchAsync(index, new Query("@geom:[within $poly]").AddParam("poly", within.ToString()).Dialect(3));
        Assert.Equal(2, res.TotalResults);
        Assert.Equal(2, res.Documents.Count);
    }

    [Fact]
    public void Issue230()
    {
        var request = new AggregationRequest("*", 3).Filter("@StatusId==1")
                .GroupBy("@CreatedDay", Reducers.CountDistinct("@UserId"), Reducers.Count().As("count"));

        var buildCommand = SearchCommandBuilder.Aggregate("idx:users", request);
        // expected: FT.AGGREGATE idx:users * FILTER @StatusId==1 GROUPBY 1 @CreatedDay REDUCE COUNT_DISTINCT 1 @UserId REDUCE COUNT 0 AS count DIALECT 3
        Assert.Equal("FT.AGGREGATE", buildCommand.Command);
        Assert.Equal(["idx:users", "*", "FILTER", "@StatusId==1", "GROUPBY", 1, "@CreatedDay", "REDUCE", "COUNT_DISTINCT", 1, "@UserId", "REDUCE", "COUNT", 0, "AS", "count", "DIALECT", 3
        ], buildCommand.Args);
    }

    [SkipIfRedisTheory(Comparison.LessThan, "7.3.240")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestNumericInDialect4(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();

        Schema sc = new Schema()
            .AddTextField("title", 1.0)
            .AddNumericField("version");

        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));
        Dictionary<string, object> fields4 = new()
        {
            { "title", "hello world" },
            { "version", 123 }
        };
        AddDocument(db, "qux", fields4);

        Assert.Equal(1, ft.Search(index, new("@version:[123 123]")).TotalResults);
        Assert.Equal(1, ft.Search(index, new Query("@version:[123]").Dialect(4)).TotalResults);
    }

    [SkipIfRedisTheory(Comparison.LessThan, "7.3.240")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestNumericOperatorsInDialect4(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();

        Schema sc = new Schema()
            .AddTextField("title", 1.0)
            .AddNumericField("version");

        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));
        Dictionary<string, object> fields4 = new()
        {
            { "title", "hello world" },
            { "version", 123 }
        };
        AddDocument(db, "qux", fields4);

        Assert.Equal(1, ft.Search(index, new("@version:[123 123]")).TotalResults);
        Assert.Equal(1, ft.Search(index, new Query("@version==123").Dialect(4)).TotalResults);

        Assert.Equal(1, ft.Search(index, new("@version:[122 +inf]")).TotalResults);
        Assert.Equal(1, ft.Search(index, new Query("@version>=122").Dialect(4)).TotalResults);

        Assert.Equal(1, ft.Search(index, new("@version:[-inf 124]")).TotalResults);
        Assert.Equal(1, ft.Search(index, new Query("@version<=124").Dialect(4)).TotalResults);

    }

    [SkipIfRedisTheory(Comparison.LessThan, "7.3.240")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestNumericLogicalOperatorsInDialect4(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();

        Schema sc = new Schema()
            .AddTextField("title", 1.0)
            .AddNumericField("version")
            .AddNumericField("id");

        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));
        Dictionary<string, object> fields4 = new()
        {
            { "title", "hello world" },
            { "version", 123 },
            { "id", 456 }
        };
        AddDocument(db, "qux", fields4);

        Assert.Equal(1, ft.Search(index, new Query("@version<=124").Dialect(4)).TotalResults);

        Assert.Equal(1, ft.Search(index, new("@version:[123 123]")).TotalResults);
        Assert.Equal(1, ft.Search(index, new Query("@version:[123] | @version:[124]").Dialect(4)).TotalResults);

        Assert.Equal(1, ft.Search(index, new("@version:[123 123] | @version:[7890 7890]")).TotalResults);
        Assert.Equal(1, ft.Search(index, new Query("@version==123 | @version==7890").Dialect(4)).TotalResults);

        Assert.Equal(1, ft.Search(index, new("@version:[123 123] | @id:[456 7890]")).TotalResults);
        Assert.Equal(1, ft.Search(index, new Query("@version==123 @id==456").Dialect(4)).TotalResults);
    }

    /// <summary>
    /// this test is to check if the issue 352 is fixed
    /// Load operation was failing because the document was not being dropped in search result due to this behaviour;
    /// "If a relevant key expires while a query is running, an attempt to load the key's value will return a null array. 
    /// However, the key is still counted in the total number of results."
    /// https://redis.io/docs/latest/commands/ft.search/#:~:text=If%20a%20relevant%20key%20expires,the%20total%20number%20of%20results. 
    /// </summary>
    [Fact]
    public void TestDocumentLoad_Issue352()
    {
        Document d = Document.Load("1", 0.5, null, [RedisValue.Null]);
        Assert.Empty(d.GetProperties().ToList());
    }

    /// <summary>
    /// this test is to check if the issue 352 is fixed
    /// Load operation was failing because the document was not being dropped in search result due to this behaviour;
    /// "If a relevant key expires while a query is running, an attempt to load the key's value will return a null array. 
    /// However, the key is still counted in the total number of results."
    /// https://redis.io/docs/latest/commands/ft.search/#:~:text=If%20a%20relevant%20key%20expires,the%20total%20number%20of%20results. 
    /// </summary>
    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestDocumentLoadWithDB_Issue352(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ft = db.FT();

        Schema sc = new Schema().AddTextField("firstText", 1.0).AddTextField("lastText", 1.0).AddNumericField("ageNumeric");
        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));

        Document? droppedDocument = null;
        int numberOfAttempts = 0;
        do
        {
            // try until succesfully create the key and set the TTL
            bool ttlRefreshed = false;
            do
            {
                db.HashSet("student:22222", [new("firstText", "Joe"), new("lastText", "Dod"), new("ageNumeric", 18)]);
                ttlRefreshed = db.KeyExpire("student:22222", TimeSpan.FromMilliseconds(500));
            } while (!ttlRefreshed);

            Int32 completed = 0;

            Action checker = () =>
            {
                for (int i = 0; i < 1000000; i++)
                {
                    SearchResult result = ft.Search(index, new());
                    List<Document> docs = result.Documents;

                    // check if doc is already dropped before search and load;
                    // if yes then its already late and we missed the window that 
                    // doc would show up in search result with no fields 
                    if (docs.Count == 0)
                    {
                        Interlocked.Increment(ref completed);
                        break;
                    }
                    // if we get a document with no fields then we know that the key 
                    // is going to be expired while the query is running, and we are able to catch the state
                    // but key itself might not be expired yet
                    else if (docs[0].GetProperties().Count() == 0)
                    {
                        droppedDocument = docs[0];
                    }
                }
            };

            List<Task> tasks = [];
            // try with 3 different tasks simultaneously to increase the chance of hitting it
            for (int i = 0; i < 3; i++) { tasks.Add(Task.Run(checker)); }
            Task checkTask = Task.WhenAll(tasks);
            await Task.WhenAny(checkTask, Task.Delay(1000));
            var keyTtl = db.KeyTimeToLive("student:22222");
            Assert.Equal(0, keyTtl.HasValue ? keyTtl.Value.Milliseconds : 0);
            Assert.Equal(3, completed);
        } while (droppedDocument == null && numberOfAttempts++ < 5);
        // we won't do an actual assert here since 
        // it is not guaranteed that window stays open wide enough to catch it.
        // instead we attempt 5 times.
        // Without fix for Issue352, document load in this case fails %100 with my local test runs,, and %100 success with fixed version.
        // The results in pipeline should be the same.
    }
}
