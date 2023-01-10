using Xunit;
using StackExchange.Redis;
using NRedisStack.RedisStackCommands;
using Moq;
using NRedisStack.Search.FT.CREATE;
using NRedisStack.Search;
using static NRedisStack.Search.Schema;
using NRedisStack.Search.Aggregation;
using NRedisStack.Literals.Enums;

namespace NRedisStack.Tests.Search;

public class SearchTests : AbstractNRedisStackTest, IDisposable
{
    Mock<IDatabase> _mock = new Mock<IDatabase>();
    // private readonly string key = "SEARCH_TESTS";
    private readonly string index = "TEST_INDEX";
    public SearchTests(RedisFixture redisFixture) : base(redisFixture) { }

    public void Dispose()
    {
        redisFixture.Redis.GetDatabase().KeyDelete(index);
    }

    private void AddDocument(IDatabase db, Document doc)
    {
            string key = doc.Id;
            var properties = doc.GetProperties();
            // HashEntry[] hash = new  HashEntry[properties.Count()];
            // for(int i = 0; i < properties.Count(); i++)
            // {
            //     var property = properties.ElementAt(i);
            //     hash[i] = new HashEntry(property.Key, property.Value);
            // }
            // db.HashSet(key, hash);
            var nameValue = new List<object>() { key };
            foreach (var item in properties)
            {
                nameValue.Add(item.Key);
                nameValue.Add(item.Value);
            }
            db.Execute("HSET", nameValue);
    }

    private void AddDocument(IDatabase db, string key, Dictionary<string, object> objDictionary)
    {
        Dictionary<string, string> strDictionary = new Dictionary<string, string>();
        // HashEntry[] hash = new  HashEntry[objDictionary.Count()];
        // for(int i = 0; i < objDictionary.Count(); i++)
        // {
        //     var property = objDictionary.ElementAt(i);
        //     hash[i] = new HashEntry(property.Key, property.Value.ToString());
        // }
        // db.HashSet(key, hash);
        var nameValue = new List<object>() { key };
        foreach (var item in objDictionary)
        {
            nameValue.Add(item.Key);
            nameValue.Add(item.Value);
        }
        db.Execute("HSET", nameValue);
    }

    [Fact]
    public void TestAggregationRequestVerbatim()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        Schema sc = new Schema();
        sc.AddTextField("name", 1.0, sortable: true);
        ft.Create(index, FTCreateParams.CreateParams(), sc);
        AddDocument(db, new Document("data1").Set("name", "hello kitty"));

        AggregationRequest r = new AggregationRequest("kitti");

        AggregationResult res = ft.Aggregate(index, r);
        Assert.Equal(1, res.TotalResults);

        r = new AggregationRequest("kitti")
                .Verbatim();

        res = ft.Aggregate(index, r);
        Assert.Equal(0, res.TotalResults);
    }

    [Fact]
    public async Task TestAggregationRequestVerbatimAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        Schema sc = new Schema();
        sc.AddTextField("name", 1.0, sortable: true);
        ft.Create(index, FTCreateParams.CreateParams(), sc);
        AddDocument(db, new Document("data1").Set("name", "hello kitty"));

        AggregationRequest r = new AggregationRequest("kitti");

        AggregationResult res = await ft.AggregateAsync(index, r);
        Assert.Equal(1, res.TotalResults);

        r = new AggregationRequest("kitti")
                .Verbatim();

        res = await ft.AggregateAsync(index, r);
        Assert.Equal(0, res.TotalResults);
    }

    [Fact]
    public void TestAggregationRequestTimeout()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        Schema sc = new Schema();
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

    [Fact]
    public async Task TestAggregationRequestTimeoutAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        Schema sc = new Schema();
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

    [Fact]
    public void TestAggregations()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        Schema sc = new Schema();
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
        Assert.NotNull(r1);
        Assert.Equal("def", r1.GetString("name"));
        Assert.Equal(30, r1.GetLong("sum"));
        Assert.Equal(30, r1.GetDouble("sum"), 0);

        Assert.Equal(0L, r1.GetLong("nosuchcol"));
        Assert.Equal(0.0, r1.GetDouble("nosuchcol"), 0);
        Assert.Null(r1.GetString("nosuchcol"));

        Row r2 = res.GetRow(1);
        Assert.NotNull(r2);
        Assert.Equal("abc", r2.GetString("name"));
        Assert.Equal(10, r2.GetLong("sum"));
    }

    [Fact]
    public async Task TestAggregationsAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        await db.ExecuteAsync("FLUSHALL");
        var ft = db.FT();
        Schema sc = new Schema();
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
        Assert.NotNull(r1);
        Assert.Equal("def", r1.GetString("name"));
        Assert.Equal(30, r1.GetLong("sum"));
        Assert.Equal(30, r1.GetDouble("sum"), 0);

        Assert.Equal(0L, r1.GetLong("nosuchcol"));
        Assert.Equal(0.0, r1.GetDouble("nosuchcol"), 0);
        Assert.Null(r1.GetString("nosuchcol"));

        Row r2 = res.GetRow(1);
        Assert.NotNull(r2);
        Assert.Equal("abc", r2.GetString("name"));
        Assert.Equal(10, r2.GetLong("sum"));
    }


    [Fact]
    public void TestAggregationsLoad()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        var sc = new Schema().AddTextField("t1").AddTextField("t2");
        ft.Create("idx", new FTCreateParams(), sc);

        AddDocument(db, new Document("doc1").Set("t1", "hello").Set("t2", "world"));

        // load t1
        var req = new AggregationRequest("*").Load(new FieldName("t1"));
        var res = ft.Aggregate("idx", req);
        Assert.Equal(res[0]["t1"].ToString(), "hello");

        // load t2
        req = new AggregationRequest("*").Load(new FieldName("t2"));
        res = ft.Aggregate("idx", req);
        Assert.Equal(res[0]["t2"], "world");

        // load all
        req = new AggregationRequest("*").LoadAll();
        res = ft.Aggregate("idx", req);
        Assert.Equal(res[0]["t1"].ToString(), "hello");
        Assert.Equal(res[0]["t2"], "world");
    }

    [Fact]
    public async Task TestAggregationsLoadAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        await db.ExecuteAsync("FLUSHALL");
        var ft = db.FT();
        var sc = new Schema().AddTextField("t1").AddTextField("t2");
        await ft.CreateAsync("idx", new FTCreateParams(), sc);

        AddDocument(db, new Document("doc1").Set("t1", "hello").Set("t2", "world"));

        // load t1
        var req = new AggregationRequest("*").Load(new FieldName("t1"));
        var res = await ft.AggregateAsync("idx", req);
        Assert.Equal(res[0]["t1"].ToString(), "hello");

        // load t2
        req = new AggregationRequest("*").Load(new FieldName("t2"));
        res = await ft.AggregateAsync("idx", req);
        Assert.Equal(res[0]["t2"], "world");

        // load all
        req = new AggregationRequest("*").LoadAll();
        res = await ft.AggregateAsync("idx", req);
        Assert.Equal(res[0]["t1"].ToString(), "hello");
        Assert.Equal(res[0]["t2"], "world");
    }



    [Fact]
    public void TestAggregationRequestParamsDialect()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        Schema sc = new Schema();
        sc.AddTextField("name", 1.0, sortable: true);
        sc.AddNumericField("count", sortable: true);
        ft.Create(index, FTCreateParams.CreateParams(), sc);
        AddDocument(db, new Document("data1").Set("name", "abc").Set("count", 10));
        AddDocument(db, new Document("data2").Set("name", "def").Set("count", 5));
        AddDocument(db, new Document("data3").Set("name", "def").Set("count", 25));

        Dictionary<string, object> parameters = new Dictionary<string, object>();
        parameters.Add("name", "abc");

        AggregationRequest r = new AggregationRequest("$name")
                .GroupBy("@name", Reducers.Sum("@count").As("sum"))
                .Params(parameters)
                .Dialect(2); // From documentation - To use PARAMS, DIALECT must be set to 2

        AggregationResult res = ft.Aggregate(index, r);
        Assert.Equal(1, res.TotalResults);

        Row r1 = res.GetRow(0);
        Assert.NotNull(r1);
        Assert.Equal("abc", r1.GetString("name"));
        Assert.Equal(10, r1.GetLong("sum"));
    }

    [Fact]
    public async Task TestAggregationRequestParamsDialectAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        Schema sc = new Schema();
        sc.AddTextField("name", 1.0, sortable: true);
        sc.AddNumericField("count", sortable: true);
        ft.Create(index, FTCreateParams.CreateParams(), sc);
        AddDocument(db, new Document("data1").Set("name", "abc").Set("count", 10));
        AddDocument(db, new Document("data2").Set("name", "def").Set("count", 5));
        AddDocument(db, new Document("data3").Set("name", "def").Set("count", 25));

        Dictionary<string, object> parameters = new Dictionary<string, object>();
        parameters.Add("name", "abc");

        AggregationRequest r = new AggregationRequest("$name")
                .GroupBy("@name", Reducers.Sum("@count").As("sum"))
                .Params(parameters)
                .Dialect(2); // From documentation - To use PARAMS, DIALECT must be set to 2

        AggregationResult res = await ft.AggregateAsync(index, r);
        Assert.Equal(1, res.TotalResults);

        Row r1 = res.GetRow(0);
        Assert.NotNull(r1);
        Assert.Equal("abc", r1.GetString("name"));
        Assert.Equal(10, r1.GetLong("sum"));
    }

    [Fact]
    public void TestAlias()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        Schema sc = new Schema().AddTextField("field1");

        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));

        Dictionary<string, object> doc = new Dictionary<string, object>();
        doc.Add("field1", "value");
        AddDocument(db, "doc1", doc);

        Assert.True(ft.AliasAdd("ALIAS1", index));
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

    [Fact]
    public async Task TestAliasAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        Schema sc = new Schema().AddTextField("field1");

        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));

        Dictionary<string, object> doc = new Dictionary<string, object>();
        doc.Add("field1", "value");
        AddDocument(db, "doc1", doc);

        Assert.True(await ft.AliasAddAsync("ALIAS1", index));
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

    [Fact]
    public void TestApplyAndFilterAggregations()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        Schema sc = new Schema();
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

        AggregationRequest r = new AggregationRequest().Apply("(@subj1+@subj2)/2", "attemptavg")
            .GroupBy("@name", Reducers.Avg("@attemptavg").As("avgscore"))
            .Filter("@avgscore>=50")
            .SortBy(10, SortedField.Asc("@name"));

        // actual search
        AggregationResult res = ft.Aggregate(index, r);
        Assert.Equal(3, res.TotalResults);

        Row r1 = res.GetRow(0);
        Assert.NotNull(r1);
        Assert.Equal("def", r1.GetString("name"));
        Assert.Equal(52.5, r1.GetDouble("avgscore"), 0);

        Row r2 = res.GetRow(1);
        Assert.NotNull(r2);
        Assert.Equal("ghi", r2.GetString("name"));
        Assert.Equal(67.5, r2.GetDouble("avgscore"), 0);
    }

    [Fact]
    public void TestCreate()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        var schema = new Schema().AddTextField("first").AddTextField("last").AddNumericField("age");
        var parameters = FTCreateParams.CreateParams().Filter("@age>16").Prefix("student:", "pupil:");

        Assert.True(ft.Create(index, parameters, schema));

        db.HashSet("profesor:5555", new HashEntry[] { new("first", "Albert"), new("last", "Blue"), new("age", "55") });
        db.HashSet("student:1111", new HashEntry[] { new("first", "Joe"), new("last", "Dod"), new("age", "18") });
        db.HashSet("pupil:2222", new HashEntry[] { new("first", "Jen"), new("last", "Rod"), new("age", "14") });
        db.HashSet("student:3333", new HashEntry[] { new("first", "El"), new("last", "Mark"), new("age", "17") });
        db.HashSet("pupil:4444", new HashEntry[] { new("first", "Pat"), new("last", "Shu"), new("age", "21") });
        db.HashSet("student:5555", new HashEntry[] { new("first", "Joen"), new("last", "Ko"), new("age", "20") });
        db.HashSet("teacher:6666", new HashEntry[] { new("first", "Pat"), new("last", "Rod"), new("age", "20") });

        var noFilters = ft.Search(index, new Query());
        Assert.Equal(4, noFilters.TotalResults);

        var res1 = ft.Search(index, new Query("@first:Jo*"));
        Assert.Equal(2, res1.TotalResults);

        var res2 = ft.Search(index, new Query("@first:Pat"));
        Assert.Equal(1, res2.TotalResults);

        var res3 = ft.Search(index, new Query("@last:Rod"));
        Assert.Equal(0, res3.TotalResults);
    }

    [Fact]
    public async Task TestCreateAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        var schema = new Schema().AddTextField("first").AddTextField("last").AddNumericField("age");
        var parameters = FTCreateParams.CreateParams().Filter("@age>16").Prefix("student:", "pupil:");
        Assert.True(await ft.CreateAsync(index, parameters, schema));
        db.HashSet("profesor:5555", new HashEntry[] { new("first", "Albert"), new("last", "Blue"), new("age", "55") });
        db.HashSet("student:1111", new HashEntry[] { new("first", "Joe"), new("last", "Dod"), new("age", "18") });
        db.HashSet("pupil:2222", new HashEntry[] { new("first", "Jen"), new("last", "Rod"), new("age", "14") });
        db.HashSet("student:3333", new HashEntry[] { new("first", "El"), new("last", "Mark"), new("age", "17") });
        db.HashSet("pupil:4444", new HashEntry[] { new("first", "Pat"), new("last", "Shu"), new("age", "21") });
        db.HashSet("student:5555", new HashEntry[] { new("first", "Joen"), new("last", "Ko"), new("age", "20") });
        db.HashSet("teacher:6666", new HashEntry[] { new("first", "Pat"), new("last", "Rod"), new("age", "20") });
        var noFilters = ft.Search(index, new Query());
        Assert.Equal(4, noFilters.TotalResults);
        var res1 = ft.Search(index, new Query("@first:Jo*"));
        Assert.Equal(2, res1.TotalResults);
        var res2 = ft.Search(index, new Query("@first:Pat"));
        Assert.Equal(1, res2.TotalResults);
        var res3 = ft.Search(index, new Query("@last:Rod"));
        Assert.Equal(0, res3.TotalResults);
    }

    [Fact]
    public void CreateNoParams()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();

        Schema sc = new Schema().AddTextField("first", 1.0).AddTextField("last", 1.0).AddNumericField("age");
        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));

        db.HashSet("student:1111", new HashEntry[] { new("first", "Joe"), new("last", "Dod"), new("age", 18) });
        db.HashSet("student:3333", new HashEntry[] { new("first", "El"), new("last", "Mark"), new("age", 17) });
        db.HashSet("pupil:4444", new HashEntry[] { new("first", "Pat"), new("last", "Shu"), new("age", 21) });
        db.HashSet("student:5555", new HashEntry[] { new("first", "Joen"), new("last", "Ko"), new("age", 20) });

        SearchResult noFilters = ft.Search(index, new Query());
        Assert.Equal(4, noFilters.TotalResults);

        SearchResult res1 = ft.Search(index, new Query("@first:Jo*"));
        Assert.Equal(2, res1.TotalResults);

        SearchResult res2 = ft.Search(index, new Query("@first:Pat"));
        Assert.Equal(1, res2.TotalResults);

        SearchResult res3 = ft.Search(index, new Query("@last:Rod"));
        Assert.Equal(0, res3.TotalResults);
    }

    [Fact]
    public async Task CreateNoParamsAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();

        Schema sc = new Schema().AddTextField("first", 1.0).AddTextField("last", 1.0).AddNumericField("age");
        Assert.True(await ft.CreateAsync(index, FTCreateParams.CreateParams(), sc));

        db.HashSet("student:1111", new HashEntry[] { new("first", "Joe"), new("last", "Dod"), new("age", 18) });
        db.HashSet("student:3333", new HashEntry[] { new("first", "El"), new("last", "Mark"), new("age", 17) });
        db.HashSet("pupil:4444", new HashEntry[] { new("first", "Pat"), new("last", "Shu"), new("age", 21) });
        db.HashSet("student:5555", new HashEntry[] { new("first", "Joen"), new("last", "Ko"), new("age", 20) });

        SearchResult noFilters = ft.Search(index, new Query());
        Assert.Equal(4, noFilters.TotalResults);

        SearchResult res1 = ft.Search(index, new Query("@first:Jo*"));
        Assert.Equal(2, res1.TotalResults);

        SearchResult res2 = ft.Search(index, new Query("@first:Pat"));
        Assert.Equal(1, res2.TotalResults);

        SearchResult res3 = ft.Search(index, new Query("@last:Rod"));
        Assert.Equal(0, res3.TotalResults);
    }

    [Fact]
    public void CreateWithFieldNames()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        Schema sc = new Schema().AddField(new TextField(FieldName.Of("first").As("given")))
            .AddField(new TextField(FieldName.Of("last")));

        Assert.True(ft.Create(index, FTCreateParams.CreateParams().Prefix("student:", "pupil:"), sc));

        db.HashSet("profesor:5555", new HashEntry[] { new("first", "Albert"), new("last", "Blue"), new("age", "55") });
        db.HashSet("student:1111", new HashEntry[] { new("first", "Joe"), new("last", "Dod"), new("age", "18") });
        db.HashSet("pupil:2222", new HashEntry[] { new("first", "Jen"), new("last", "Rod"), new("age", "14") });
        db.HashSet("student:3333", new HashEntry[] { new("first", "El"), new("last", "Mark"), new("age", "17") });
        db.HashSet("pupil:4444", new HashEntry[] { new("first", "Pat"), new("last", "Shu"), new("age", "21") });
        db.HashSet("student:5555", new HashEntry[] { new("first", "Joen"), new("last", "Ko"), new("age", "20") });
        db.HashSet("teacher:6666", new HashEntry[] { new("first", "Pat"), new("last", "Rod"), new("age", "20") });

        SearchResult noFilters = ft.Search(index, new Query());
        Assert.Equal(5, noFilters.TotalResults);

        SearchResult asOriginal = ft.Search(index, new Query("@first:Jo*"));
        Assert.Equal(0, asOriginal.TotalResults);

        SearchResult asAttribute = ft.Search(index, new Query("@given:Jo*"));
        Assert.Equal(2, asAttribute.TotalResults);

        SearchResult nonAttribute = ft.Search(index, new Query("@last:Rod"));
        Assert.Equal(1, nonAttribute.TotalResults);
    }

    [Fact]
    public async Task CreateWithFieldNamesAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        Schema sc = new Schema().AddField(new TextField(FieldName.Of("first").As("given")))
            .AddField(new TextField(FieldName.Of("last")));

        Assert.True(await ft.CreateAsync(index, FTCreateParams.CreateParams().Prefix("student:", "pupil:"), sc));

        db.HashSet("profesor:5555", new HashEntry[] { new("first", "Albert"), new("last", "Blue"), new("age", "55") });
        db.HashSet("student:1111", new HashEntry[] { new("first", "Joe"), new("last", "Dod"), new("age", "18") });
        db.HashSet("pupil:2222", new HashEntry[] { new("first", "Jen"), new("last", "Rod"), new("age", "14") });
        db.HashSet("student:3333", new HashEntry[] { new("first", "El"), new("last", "Mark"), new("age", "17") });
        db.HashSet("pupil:4444", new HashEntry[] { new("first", "Pat"), new("last", "Shu"), new("age", "21") });
        db.HashSet("student:5555", new HashEntry[] { new("first", "Joen"), new("last", "Ko"), new("age", "20") });
        db.HashSet("teacher:6666", new HashEntry[] { new("first", "Pat"), new("last", "Rod"), new("age", "20") });

        SearchResult noFilters = await ft.SearchAsync(index, new Query());
        Assert.Equal(5, noFilters.TotalResults);

        SearchResult asOriginal = await ft.SearchAsync(index, new Query("@first:Jo*"));
        Assert.Equal(0, asOriginal.TotalResults);

        SearchResult asAttribute = await ft.SearchAsync(index, new Query("@given:Jo*"));
        Assert.Equal(2, asAttribute.TotalResults);

        SearchResult nonAttribute = await ft.SearchAsync(index, new Query("@last:Rod"));
        Assert.Equal(1, nonAttribute.TotalResults);
    }

    [Fact]
    public void AlterAdd()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        Schema sc = new Schema().AddTextField("title", 1.0);

        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));
        var fields = new HashEntry("title", "hello world");
        //fields.("title", "hello world");
        for (int i = 0; i < 100; i++)
        {
            db.HashSet($"doc{i}", fields.Name, fields.Value);
        }
        SearchResult res = ft.Search(index, new Query("hello world"));
        Assert.Equal(100, res.TotalResults);

        Assert.True(ft.Alter(index, new Schema().AddTagField("tags").AddTextField("name", weight: 0.5)));
        for (int i = 0; i < 100; i++)
        {
            var fields2 = new HashEntry[] { new("name", "name" + i),
                                      new("tags", $"tagA,tagB,tag{i}") };
            //      assertTrue(client.updateDocument(string.format("doc%d", i), 1.0, fields2));
            db.HashSet($"doc{i}", fields2);
        }
        SearchResult res2 = ft.Search(index, new Query("@tags:{tagA}"));
        Assert.Equal(100, res2.TotalResults);

        var info = ft.Info(index);
        Assert.Equal(index, info.IndexName);
        Assert.Equal(0, info.IndexOption.Count);
        // Assert.Equal(,info.IndexDefinition);
        Assert.Equal("title", (info.Attributes[0]["identifier"]).ToString());
        Assert.Equal("TAG", (info.Attributes[1]["type"]).ToString());
        Assert.Equal("name", (info.Attributes[2]["attribute"]).ToString());
        Assert.Equal(100, info.NumDocs);
        Assert.Equal("300", info.MaxDocId);
        Assert.Equal(102, info.NumTerms);
        Assert.True(info.NumRecords == 800 || info.NumRecords == 802); // TODO: should this be 800?
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
        Assert.True(info.TotalIndexingTime > 0);
        Assert.Equal(0, info.Indexing);
        Assert.Equal(1, info.PercentIndexed);
        Assert.Equal(4, info.NumberOfUses);
        Assert.Equal(7, info.GcStats.Count);
        Assert.Equal(4, info.CursorStats.Count);
    }

    [Fact]
    public async Task AlterAddAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        Schema sc = new Schema().AddTextField("title", 1.0);

        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));
        var fields = new HashEntry("title", "hello world");
        //fields.("title", "hello world");
        for (int i = 0; i < 100; i++)
        {
            db.HashSet($"doc{i}", fields.Name, fields.Value);
        }
        SearchResult res = ft.Search(index, new Query("hello world"));
        Assert.Equal(100, res.TotalResults);

        Assert.True(await ft.AlterAsync(index, new Schema().AddTagField("tags").AddTextField("name", weight: 0.5)));
        for (int i = 0; i < 100; i++)
        {
            var fields2 = new HashEntry[] { new("name", "name" + i),
                                      new("tags", $"tagA,tagB,tag{i}") };
            //      assertTrue(client.updateDocument(string.format("doc%d", i), 1.0, fields2));
            db.HashSet($"doc{i}", fields2);
        }
        SearchResult res2 = ft.Search(index, new Query("@tags:{tagA}"));
        Assert.Equal(100, res2.TotalResults);

        var info = await ft.InfoAsync(index);
        Assert.Equal(index, info.IndexName);
        Assert.Equal("title", (info.Attributes[0]["identifier"]).ToString());
        Assert.Equal("TAG", (info.Attributes[1]["type"]).ToString());
        Assert.Equal("name", (info.Attributes[2]["attribute"]).ToString());
        Assert.Equal(100, info.NumDocs);
        Assert.Equal("300", info.MaxDocId);
        Assert.Equal(102, info.NumTerms);
        Assert.True(info.NumRecords == 800 || info.NumRecords == 802); // TODO: should this be 800?
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
        Assert.True(info.TotalIndexingTime > 0);
        Assert.Equal(0, info.Indexing);
        Assert.Equal(1, info.PercentIndexed);
        Assert.Equal(4, info.NumberOfUses);
        Assert.Equal(7, info.GcStats.Count);
        Assert.Equal(4, info.CursorStats.Count);
    }

    [Fact]
    public void TestConfig()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        Assert.True(ft.ConfigSet("TIMEOUT", "100"));
        Dictionary<string, string> configMap = ft.ConfigGet("*");
        Assert.Equal("100", configMap["TIMEOUT"].ToString());
    }

    [Fact]
    public async Task TestConfigAsnyc()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        Assert.True(await ft.ConfigSetAsync("TIMEOUT", "100"));
        Dictionary<string, string> configMap = await ft.ConfigGetAsync("*");
        Assert.Equal("100", configMap["TIMEOUT"].ToString());
    }

    [Fact]
    public void configOnTimeout()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        Assert.True(ft.ConfigSet("ON_TIMEOUT", "fail"));
        Assert.Equal("fail", ft.ConfigGet("ON_TIMEOUT")["ON_TIMEOUT"]);

        try { ft.ConfigSet("ON_TIMEOUT", "null"); } catch (RedisServerException) { }
    }

    [Fact]
    public async Task configOnTimeoutAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        Assert.True(await ft.ConfigSetAsync("ON_TIMEOUT", "fail"));
        Assert.Equal("fail", (await ft.ConfigGetAsync("ON_TIMEOUT"))["ON_TIMEOUT"]);

        try { ft.ConfigSet("ON_TIMEOUT", "null"); } catch (RedisServerException) { }
    }

    [Fact]
    public void TestDialectConfig()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
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

    [Fact]
    public async Task TestDialectConfigAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
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

    [Fact]
    public async Task TestCursor()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        Schema sc = new Schema();
        sc.AddTextField("name", 1.0, sortable: true);
        sc.AddNumericField("count", sortable: true);
        ft.Create(index, FTCreateParams.CreateParams(), sc);
        AddDocument(db, new Document("data1").Set("name", "abc").Set("count", 10));
        AddDocument(db, new Document("data2").Set("name", "def").Set("count", 5));
        AddDocument(db, new Document("data3").Set("name", "def").Set("count", 25));

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

        res = ft.CursorRead(index, res.CursorId, 1);
        Row? row2 = res.GetRow(0);

        Assert.NotNull(row2);
        Assert.Equal("abc", row2.Value.GetString("name"));
        Assert.Equal(10, row2.Value.GetLong("sum"));

        Assert.True(ft.CursorDel(index, res.CursorId));

        try
        {
            ft.CursorRead(index, res.CursorId, 1);
            Assert.True(false);
        }
        catch (RedisException) { }

        _ = new AggregationRequest()
            .GroupBy("@name", Reducers.Sum("@count").As("sum"))
            .SortBy(10, SortedField.Desc("@sum"))
            .Cursor(1, 1000);

        await Task.Delay(1000).ConfigureAwait(false);

        try
        {
            ft.CursorRead(index, res.CursorId, 1);
            Assert.True(false);
        }
        catch (RedisException) { }
    }

    [Fact]
    public async Task TestCursorAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        Schema sc = new Schema();
        sc.AddTextField("name", 1.0, sortable: true);
        sc.AddNumericField("count", sortable: true);
        ft.Create(index, FTCreateParams.CreateParams(), sc);
        AddDocument(db, new Document("data1").Set("name", "abc").Set("count", 10));
        AddDocument(db, new Document("data2").Set("name", "def").Set("count", 5));
        AddDocument(db, new Document("data3").Set("name", "def").Set("count", 25));

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

        res = await ft.CursorReadAsync(index, res.CursorId, 1);
        Row? row2 = res.GetRow(0);

        Assert.NotNull(row2);
        Assert.Equal("abc", row2.Value.GetString("name"));
        Assert.Equal(10, row2.Value.GetLong("sum"));

        Assert.True(await ft.CursorDelAsync(index, res.CursorId));

        try
        {
            await ft.CursorReadAsync(index, res.CursorId, 1);
            Assert.True(false);
        }
        catch (RedisException) { }

        _ = new AggregationRequest()
            .GroupBy("@name", Reducers.Sum("@count").As("sum"))
            .SortBy(10, SortedField.Desc("@sum"))
            .Cursor(1, 1000);

        await Task.Delay(1000).ConfigureAwait(false);

        try
        {
            await ft.CursorReadAsync(index, res.CursorId, 1);
            Assert.True(false);
        }
        catch (RedisException) { }
    }

    [Fact]
    public void TestAggregationGroupBy()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();

        // Creating the index definition and schema
        ft.Create("idx", new FTCreateParams(), new Schema().AddNumericField("random_num")
                                                           .AddTextField("title")
                                                           .AddTextField("body")
                                                           .AddTextField("parent"));

        // Indexing a document
        AddDocument(db, "search", new Dictionary<string, object>(){
        { "title", "RediSearch" },
        { "body", "Redisearch impements a search engine on top of redis" },
        { "parent", "redis" },
        { "random_num", 10 }});

        AddDocument(db, "ai", new Dictionary<string, object>
        {
        { "title", "RedisAI" },
        { "body", "RedisAI executes Deep Learning/Machine Learning models and managing their data." },
        { "parent", "redis" },
        { "random_num", 3 }});

        AddDocument(db, "json", new Dictionary<string, object>
        {
        { "title", "RedisJson" },
        { "body", "RedisJSON implements ECMA-404 The JSON Data Interchange Standard as a native data type." },
        { "parent", "redis" },
        { "random_num", 8 }});

        var req = new AggregationRequest("redis").GroupBy("@parent", Reducers.Count());
        var res = ft.Aggregate("idx", req).GetRow(0);
        Assert.True(res.ContainsKey("parent"));
        Assert.Equal(res["parent"], "redis");
        Assert.Equal(res["__generated_aliascount"], "3");

        req = new AggregationRequest("redis").GroupBy("@parent", Reducers.CountDistinct("@title"));
        res = ft.Aggregate("idx", req).GetRow(0);
        Assert.Equal(res["parent"], "redis");
        Assert.Equal(res.GetLong("__generated_aliascount_distincttitle"), 3);

        req = new AggregationRequest("redis").GroupBy("@parent", Reducers.CountDistinctish("@title"));
        res = ft.Aggregate("idx", req).GetRow(0);
        Assert.Equal(res["parent"], "redis");
        Assert.Equal(res.GetLong("__generated_aliascount_distinctishtitle"), 3);

        req = new AggregationRequest("redis").GroupBy("@parent", Reducers.Sum("@random_num"));
        res = ft.Aggregate("idx", req).GetRow(0);
        Assert.Equal(res["parent"], "redis");
        Assert.Equal(res.GetLong("__generated_aliassumrandom_num"), 21); // 10+8+3

        req = new AggregationRequest("redis").GroupBy("@parent", Reducers.Min("@random_num"));
        res = ft.Aggregate("idx", req).GetRow(0);
        Assert.Equal(res["parent"], "redis");
        Assert.Equal(res.GetLong("__generated_aliasminrandom_num"), 3); // min(10,8,3)

        req = new AggregationRequest("redis").GroupBy("@parent", Reducers.Max("@random_num"));
        res = ft.Aggregate("idx", req).GetRow(0);
        Assert.Equal(res["parent"], "redis");
        Assert.Equal(res.GetLong("__generated_aliasmaxrandom_num"), 10); // max(10,8,3)

        req = new AggregationRequest("redis").GroupBy("@parent", Reducers.Avg("@random_num"));
        res = ft.Aggregate("idx", req).GetRow(0);
        Assert.Equal(res["parent"], "redis");
        Assert.Equal(res.GetLong("__generated_aliasavgrandom_num"), 7); // (10+3+8)/3

        req = new AggregationRequest("redis").GroupBy("@parent", Reducers.StdDev("@random_num"));
        res = ft.Aggregate("idx", req).GetRow(0);
        Assert.Equal(res["parent"], "redis");
        Assert.Equal(res.GetDouble("__generated_aliasstddevrandom_num"), 3.60555127546);

        req = new AggregationRequest("redis").GroupBy(
            "@parent", Reducers.Quantile("@random_num", 0.5));
        res = ft.Aggregate("idx", req).GetRow(0);
        Assert.Equal(res["parent"], "redis");
        Assert.Equal(res.GetLong("__generated_aliasquantilerandom_num,0.5"), 8);  // median of 3,8,10

        req = new AggregationRequest("redis").GroupBy(
            "@parent", Reducers.ToList("@title"));
        var rawRes = ft.Aggregate("idx", req);
        res = rawRes.GetRow(0);
        Assert.Equal(res["parent"], "redis");
        // TODO: complete this assert after handling multi bulk reply
        //Assert.Equal((RedisValue[])res["__generated_aliastolisttitle"], { "RediSearch", "RedisAI", "RedisJson"});

        req = new AggregationRequest("redis").GroupBy(
            "@parent", Reducers.FirstValue("@title").As("first"));
        res = ft.Aggregate("idx", req).GetRow(0);
        Assert.Equal(res["parent"], "redis");
        Assert.Equal(res["first"], "RediSearch");

        req = new AggregationRequest("redis").GroupBy(
            "@parent", Reducers.RandomSample("@title", 2).As("random"));
        res = ft.Aggregate("idx", req).GetRow(0);
        Assert.Equal(res["parent"], "redis");
        // TODO: complete this assert after handling multi bulk reply
        // Assert.Equal(res[2], "random");
        // Assert.Equal(len(res[3]), 2);
        // Assert.Equal(res[3][0] in ["RediSearch", "RedisAI", "RedisJson"]);
        // req = new AggregationRequest("redis").GroupBy("@parent", redu

    }


    [Fact]
    public void TestDictionary()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();

        Assert.Equal(3L, ft.DictAdd("dict", "bar", "foo", "hello world"));

        var dumResult = ft.DictDump("dict");
        int i = 0;
        Assert.Equal("bar", dumResult[i++].ToString());
        Assert.Equal("foo", dumResult[i++].ToString());
        Assert.Equal("hello world", dumResult[i].ToString());

        Assert.Equal(3L, ft.DictDel("dict", "foo", "bar", "hello world"));
        Assert.Equal(ft.DictDump("dict").Length, 0);
    }

    [Fact]
    public void TestDropIndex()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        Schema sc = new Schema().AddTextField("title", 1.0);
        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));

        Dictionary<string, object> fields = new Dictionary<string, object>();
        fields.Add("title", "hello world");
        for (int i = 0; i < 100; i++)
        {
            AddDocument(db, $"doc{i}", fields);
        }

        SearchResult res = ft.Search(index, new Query("hello world"));
        Assert.Equal(100, res.TotalResults);

        Assert.True(ft.DropIndex(index));

        try
        {
            ft.Search(index, new Query("hello world"));
            //fail("Index should not exist.");
        }
        catch (RedisServerException ex)
        {
            Assert.True(ex.Message.Contains("no such index"));
        }
        Assert.Equal("100", db.Execute("DBSIZE").ToString());
    }

    [Fact]
    public async Task TestDropIndexAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        Schema sc = new Schema().AddTextField("title", 1.0);
        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));

        Dictionary<string, object> fields = new Dictionary<string, object>();
        fields.Add("title", "hello world");
        for (int i = 0; i < 100; i++)
        {
            AddDocument(db, $"doc{i}", fields);
        }

        SearchResult res = ft.Search(index, new Query("hello world"));
        Assert.Equal(100, res.TotalResults);

        Assert.True(await ft.DropIndexAsync(index));

        try
        {
            ft.Search(index, new Query("hello world"));
            //fail("Index should not exist.");
        }
        catch (RedisServerException ex)
        {
            Assert.True(ex.Message.Contains("no such index"));
        }
        Assert.Equal("100", db.Execute("DBSIZE").ToString());
    }

    [Fact]
    public void dropIndexDD()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        Schema sc = new Schema().AddTextField("title", 1.0);
        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));

        Dictionary<string, object> fields = new Dictionary<string, object>();
        fields.Add("title", "hello world");
        for (int i = 0; i < 100; i++)
        {
            AddDocument(db, $"doc{i}", fields);
        }

        SearchResult res = ft.Search(index, new Query("hello world"));
        Assert.Equal(100, res.TotalResults);

        Assert.True(ft.DropIndex(index, true));

        RedisResult[] keys = (RedisResult[])db.Execute("KEYS", "*");
        Assert.True(keys.Length == 0);
        Assert.Equal("0", db.Execute("DBSIZE").ToString());
    }

    [Fact]
    public async Task dropIndexDDAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        Schema sc = new Schema().AddTextField("title", 1.0);
        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));

        Dictionary<string, object> fields = new Dictionary<string, object>();
        fields.Add("title", "hello world");
        for (int i = 0; i < 100; i++)
        {
            AddDocument(db, $"doc{i}", fields);
        }

        SearchResult res = ft.Search(index, new Query("hello world"));
        Assert.Equal(100, res.TotalResults);

        Assert.True(await ft.DropIndexAsync(index, true));

        RedisResult[] keys = (RedisResult[])db.Execute("KEYS", "*");
        Assert.True(keys.Length == 0);
        Assert.Equal("0", db.Execute("DBSIZE").ToString());
    }

    [Fact]
    public async Task TestDictionaryAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();

        Assert.Equal(3L, await ft.DictAddAsync("dict", "bar", "foo", "hello world"));

        var dumResult = await ft.DictDumpAsync("dict");
        int i = 0;
        Assert.Equal("bar", dumResult[i++].ToString());
        Assert.Equal("foo", dumResult[i++].ToString());
        Assert.Equal("hello world", dumResult[i].ToString());

        Assert.Equal(3L, await ft.DictDelAsync("dict", "foo", "bar", "hello world"));
        Assert.Equal((await ft.DictDumpAsync("dict")).Length, 0);
    }

    [Fact]
    public void TestExplain()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        Schema sc = new Schema()
            .AddTextField("f1", 1.0)
            .AddTextField("f2", 1.0)
            .AddTextField("f3", 1.0);
        ft.Create(index, FTCreateParams.CreateParams(), sc);

        String res = ft.Explain(index, new Query("@f3:f3_val @f2:f2_val @f1:f1_val"));
        Assert.NotNull(res);
        Assert.False(res.Length == 0);
    }

    [Fact]
    public async Task TestExplainAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        Schema sc = new Schema()
            .AddTextField("f1", 1.0)
            .AddTextField("f2", 1.0)
            .AddTextField("f3", 1.0);
        ft.Create(index, FTCreateParams.CreateParams(), sc);

        String res = await ft.ExplainAsync(index, new Query("@f3:f3_val @f2:f2_val @f1:f1_val"));
        Assert.NotNull(res);
        Assert.False(res.Length == 0);
    }

    [Fact]
    public void TestSynonym()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
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

        Dictionary<string, List<string>> expected = new Dictionary<string, List<string>>();
        expected.Add("girl", new List<string>() { group1_str });
        expected.Add("baby", new List<string>() { group1_str });
        expected.Add("child", new List<string>() { group1_str, group2_str });
        Assert.Equal(expected, dump);
    }

    [Fact]
    public async Task TestSynonymAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
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

        Dictionary<string, List<string>> expected = new Dictionary<string, List<string>>();
        expected.Add("girl", new List<string>() { group1_str });
        expected.Add("baby", new List<string>() { group1_str });
        expected.Add("child", new List<string>() { group1_str, group2_str });
        Assert.Equal(expected, dump);
    }

    [Fact]
    public void TestModulePrefixs()
    {
        IDatabase db1 = redisFixture.Redis.GetDatabase();
        IDatabase db2 = redisFixture.Redis.GetDatabase();

        var ft1 = db1.FT();
        var ft2 = db2.FT();

        Assert.NotEqual(ft1.GetHashCode(), ft2.GetHashCode());
    }

    [Fact]
    public async Task GetTagFieldSyncAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        Schema sc = new Schema()
            .AddTextField("title", 1.0)
            .AddTagField("category");

        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));
        Dictionary<string, object> fields1 = new Dictionary<string, object>();
        fields1.Add("title", "hello world");
        fields1.Add("category", "red");
        //    assertTrue(client.AddDocument(db, "foo", fields1));
        AddDocument(db, "foo", fields1);
        Dictionary<string, object> fields2 = new Dictionary<string, object>();
        fields2.Add("title", "hello world");
        fields2.Add("category", "blue");
        //    assertTrue(client.AddDocument(db, "bar", fields2));
        AddDocument(db, "bar", fields2);
        Dictionary<string, object> fields3 = new Dictionary<string, object>();
        fields3.Add("title", "hello world");
        fields3.Add("category", "green,yellow");
        //    assertTrue(client.AddDocument(db, "baz", fields3));
        AddDocument(db, "baz", fields3);
        Dictionary<string, object> fields4 = new Dictionary<string, object>();
        fields4.Add("title", "hello world");
        fields4.Add("category", "orange;purple");
        //    assertTrue(client.AddDocument(db, "qux", fields4));
        AddDocument(db, "qux", fields4);

        Assert.Equal(1, ft.Search(index, new Query("@category:{red}")).TotalResults);
        Assert.Equal(1, ft.Search(index, new Query("@category:{blue}")).TotalResults);
        Assert.Equal(1, ft.Search(index, new Query("hello @category:{red}")).TotalResults);
        Assert.Equal(1, ft.Search(index, new Query("hello @category:{blue}")).TotalResults);
        Assert.Equal(1, ft.Search(index, new Query("@category:{yellow}")).TotalResults);
        Assert.Equal(0, ft.Search(index, new Query("@category:{purple}")).TotalResults);
        Assert.Equal(1, ft.Search(index, new Query("@category:{orange\\;purple}")).TotalResults);
        Assert.Equal(4, ft.Search(index, new Query("hello")).TotalResults);

        var SyncRes = ft.TagVals(index, "category");
        int i = 0;
        Assert.Equal(SyncRes[i++].ToString(), "blue");
        Assert.Equal(SyncRes[i++].ToString(), "green");
        Assert.Equal(SyncRes[i++].ToString(), "orange;purple");
        Assert.Equal(SyncRes[i++].ToString(), "red");
        Assert.Equal(SyncRes[i++].ToString(), "yellow");

        var AsyncRes = await ft.TagValsAsync(index, "category");
        i = 0;
        Assert.Equal(SyncRes[i++].ToString(), "blue");
        Assert.Equal(SyncRes[i++].ToString(), "green");
        Assert.Equal(SyncRes[i++].ToString(), "orange;purple");
        Assert.Equal(SyncRes[i++].ToString(), "red");
        Assert.Equal(SyncRes[i++].ToString(), "yellow");
    }

    [Fact]
    public async Task TestGetTagFieldWithNonDefaultSeparatorSyncAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        Schema sc = new Schema()
            .AddTextField("title", 1.0)
            .AddTagField("category", separator: ";");

        Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));
        Dictionary<string, object> fields1 = new Dictionary<string, object>();
        fields1.Add("title", "hello world");
        fields1.Add("category", "red");
        //    assertTrue(client.AddDocument(db, "foo", fields1));
        AddDocument(db, "foo", fields1);
        Dictionary<string, object> fields2 = new Dictionary<string, object>();
        fields2.Add("title", "hello world");
        fields2.Add("category", "blue");
        //    assertTrue(client.AddDocument(db, "bar", fields2));
        AddDocument(db, "bar", fields2);
        Dictionary<string, object> fields3 = new Dictionary<string, object>();
        fields3.Add("title", "hello world");
        fields3.Add("category", "green;yellow");
        AddDocument(db, "baz", fields3);
        //    assertTrue(client.AddDocument(db, "baz", fields3));
        Dictionary<string, object> fields4 = new Dictionary<string, object>();
        fields4.Add("title", "hello world");
        fields4.Add("category", "orange,purple");
        //    assertTrue(client.AddDocument(db, "qux", fields4));
        AddDocument(db, "qux", fields4);

        Assert.Equal(1, ft.Search(index, new Query("@category:{red}")).TotalResults);
        Assert.Equal(1, ft.Search(index, new Query("@category:{blue}")).TotalResults);
        Assert.Equal(1, ft.Search(index, new Query("hello @category:{red}")).TotalResults);
        Assert.Equal(1, ft.Search(index, new Query("hello @category:{blue}")).TotalResults);
        Assert.Equal(1, ft.Search(index, new Query("hello @category:{yellow}")).TotalResults);
        Assert.Equal(0, ft.Search(index, new Query("@category:{purple}")).TotalResults);
        Assert.Equal(1, ft.Search(index, new Query("@category:{orange\\,purple}")).TotalResults);
        Assert.Equal(4, ft.Search(index, new Query("hello")).TotalResults);

        var SyncRes = ft.TagVals(index, "category");
        int i = 0;
        Assert.Equal(SyncRes[i++].ToString(), "blue");
        Assert.Equal(SyncRes[i++].ToString(), "green");
        Assert.Equal(SyncRes[i++].ToString(), "orange,purple");
        Assert.Equal(SyncRes[i++].ToString(), "red");
        Assert.Equal(SyncRes[i++].ToString(), "yellow");

        var AsyncRes = await ft.TagValsAsync(index, "category");
        i = 0;
        Assert.Equal(SyncRes[i++].ToString(), "blue");
        Assert.Equal(SyncRes[i++].ToString(), "green");
        Assert.Equal(SyncRes[i++].ToString(), "orange,purple");
        Assert.Equal(SyncRes[i++].ToString(), "red");
        Assert.Equal(SyncRes[i++].ToString(), "yellow");
    }


    [Fact]
    public void TestFTCreateParamsCommandBuilder()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
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
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
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

    [Fact]
    public void TestFilters()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        // Create the index with the same fields as in the original test
        var sc = new Schema()
            .AddTextField("txt")
            .AddNumericField("num")
            .AddGeoField("loc");
        ft.Create("idx", new FTCreateParams(), sc);

        // Add the two documents to the index
        AddDocument(db, "doc1", new Dictionary<string, object> {
                { "txt", "foo bar" },
                { "num", "3.141" },
                { "loc", "-0.441,51.458" }
            });
        AddDocument(db, "doc2", new Dictionary<string, object> {
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

    [Fact]
    public async Task TestFiltersAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        // Create the index with the same fields as in the original test
        var sc = new Schema()
            .AddTextField("txt")
            .AddNumericField("num")
            .AddGeoField("loc");
        await ft.CreateAsync("idx", new FTCreateParams(), sc);

        // Add the two documents to the index
        AddDocument(db, "doc1", new Dictionary<string, object> {
                { "txt", "foo bar" },
                { "num", "3.141" },
                { "loc", "-0.441,51.458" }
            });
        AddDocument(db, "doc2", new Dictionary<string, object> {
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
        var db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        ft.Create("idx", new FTCreateParams(), new Schema().AddTextField("txt"));
        var res = ft.Search("idx", testQuery);
        Assert.Equal(0, res.Documents.Count());
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

        for (int i = 0; i < buildCommand.Args.Count(); i++)
        {
            Assert.Equal(expectedArgs[i].ToString(), buildCommand.Args[i].ToString());
        }
        Assert.Equal("FT.SEARCH", buildCommand.Command);

        // test that the command not throw an exception:
        var db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        ft.Create("idx", new FTCreateParams(), new Schema().AddTextField("txt"));
        var res = ft.Search("idx", testQuery);
        Assert.Equal(0, res.Documents.Count());
    }

    [Fact]
    public void TestQueryCommandBuilderScore()
    {
        // TODO: write better test for scores and payloads
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();

        db.Execute("JSON.SET",  "doc:1",  "$",  "[{\"arr\": [1, 2, 3]}, {\"val\": \"hello\"}, {\"val\": \"world\"}]");
        db.Execute("FT.CREATE", "idx", "ON", "JSON", "PREFIX", "1", "doc:", "SCHEMA", "$..arr", "AS", "arr", "NUMERIC", "$..val", "AS", "val", "TEXT");
        var res = ft.Search("idx", new Query("*").ReturnFields("arr", "val").SetWithScores().SetPayload("arr"));
        Assert.Equal(1, res.TotalResults);
    }

    [Fact]
    public void TestFieldsCommandBuilder()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        // Create the index with the same fields as in the original test
        var sc = new Schema()
            .AddTextField(FieldName.Of("txt"), 1.0, true, true, true, "dm:en", true, true)
            .AddNumericField(FieldName.Of("num"), true, true)
            .AddGeoField(FieldName.Of("loc"), true, true)
            .AddTagField(FieldName.Of("tag"), true, true, true, ";", true, true)
            .AddVectorField("vec", VectorField.VectorAlgo.FLAT, new Dictionary<string, object> { { "dim", 10 } });
        var buildCommand = SearchCommandBuilder.Create("idx", new FTCreateParams(), sc);
        var expectedArgs = new List<object> {
            "idx",
            "SCHEMA",
            "txt",
            "TEXT",
            "SORTABLE",
            "UNF",
            "NOSTEM",
            "NOINDEX",
            "PHONETIC",
            "dm:en",
            "WITHSUFFIXTRIE",
            "num",
            "NUMERIC",
            "SORTABLE",
            "NOINDEX",
            "loc",
            "GEO",
            "SORTABLE",
            "NOINDEX",
            "tag",
            "TAG",
            "SORTABLE",
            "UNF",
            "NOINDEX",
            "WITHSUFFIXTRIE",
            "SEPARATOR",
            ";",
            "CASESENSITIVE",
            "vec",
            "VECTOR",
            "FLAT",
            "1",
            "dim",
            "10"
        };

        Assert.Equal("FT.CREATE", buildCommand.Command);
        for (int i = 0; i < expectedArgs.Count; i++)
        {
            Assert.Equal(expectedArgs[i], buildCommand.Args[i]);
        }
    }

    [Fact]
    public void TestLimit()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();

        ft.Create("idx", new FTCreateParams(), new Schema().AddTextField("t1").AddTextField("t2"));
        Document doc1 = new Document("doc1", new Dictionary<string, RedisValue> {{"t1", "a"}, {"t2", "b"}});
        Document doc2 = new Document("doc2", new Dictionary<string, RedisValue> {{"t1", "b"}, {"t2", "a"}});
        AddDocument(db, doc1);
        AddDocument(db, doc2);

        var req = new AggregationRequest("*").SortBy("@t1").Limit(1, 1);
        var res = ft.Aggregate("idx", req);

        Assert.Equal( res.GetResults().Count, 1);
        Assert.Equal( res.GetResults()[0]["t1"].ToString(), "b");
    }

    [Fact]
    public async Task TestLimitAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();

        ft.Create("idx", new FTCreateParams(), new Schema().AddTextField("t1").AddTextField("t2"));
        Document doc1 = new Document("doc1", new Dictionary<string, RedisValue> {{"t1", "a"}, {"t2", "b"}});
        Document doc2 = new Document("doc2", new Dictionary<string, RedisValue> {{"t1", "b"}, {"t2", "a"}});
        AddDocument(db, doc1);
        AddDocument(db, doc2);

        var req = new AggregationRequest("*").SortBy("@t1").Limit(1, 1);
        var res = await ft.AggregateAsync("idx", req);

        Assert.Equal( res.GetResults().Count, 1);
        Assert.Equal( res.GetResults()[0]["t1"].ToString(), "b");
    }

    [Fact]
    public void Test_List()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        Assert.Equal(ft._List(), new RedisResult[] { });
    }

    [Fact]
    public async Task Test_ListAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();
        Assert.Equal(await ft._ListAsync(), new RedisResult[] { });
    }

    [Fact]
    public void TestModulePrefixs1()
    {
        {
            var conn = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = conn.GetDatabase();

            var ft = db.FT();
            // ...
            conn.Dispose();
        }

        {
            var conn = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = conn.GetDatabase();

            var ft = db.FT();
            // ...
            conn.Dispose();
        }
    }
}