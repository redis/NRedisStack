using Xunit;
using StackExchange.Redis;
using NRedisStack.RedisStackCommands;
using Moq;
using NRedisStack.Search.FT.CREATE;
using NRedisStack.Search;
using static NRedisStack.Search.Schema;
using NRedisStack.Search.Aggregation;

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
        AddDocument(db, new Document("data1").Set("name", "abc").Set("count", 10));
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
        AddDocument(db, new Document("data1").Set("name", "abc").Set("count", 10));
        AddDocument(db, new Document("data2").Set("name", "def").Set("count", 5));
        AddDocument(db, new Document("data3").Set("name", "def").Set("count", 25));

        AggregationRequest r = new AggregationRequest()
                .GroupBy("@name", Reducers.Sum("@count").As("sum"))
                .Timeout(5000);

        AggregationResult res = await ft.AggregateAsync(index, r);
        Assert.Equal(2, res.TotalResults);
    }

    // // TODO: underastant why its not working
    // [Fact]
    // public void TestAggregations()
    // {
    //     IDatabase db = redisFixture.Redis.GetDatabase();
    //     db.Execute("FLUSHALL");
    //     var ft = db.FT();
    //     Schema sc = new Schema();
    //     sc.AddTextField("name", 1.0, true);
    //     sc.AddNumericField("count", true);
    //     ft.Create(index, FTCreateParams.CreateParams(), sc);
    //     //    client.AddDocument(new Document("data1").Set("name", "abc").Set("count", 10));
    //     //    client.AddDocument(new Document("data2").Set("name", "def").Set("count", 5));
    //     //    client.AddDocument(new Document("data3").Set("name", "def").Set("count", 25));
    //     AddDocument(db, new Document("data1").Set("name", "abc").Set("count", 10));
    //     AddDocument(db, new Document("data2").Set("name", "def").Set("count", 5));
    //     AddDocument(db, new Document("data3").Set("name", "def").Set("count", 25));

    //     AggregationRequest r = new AggregationRequest()
    //         .GroupBy("@name", Reducers.Sum("@count").As ("sum"))
    //     .SortBy(10, SortedField.Desc("@sum"));

    //     // actual search
    //     // var resBefore = db.Execute("FT.AGGREGATE", index, "*", "GROUPBY", "1", "@name", "REDUCE", "SUM", "1", "@count", "AS", "sum", "SORTBY", "2", "@sum", "DESC", "MAX", "10");
    //     //var res = new AggregationResult(resBefore);

    //     var res = ft.Aggregate(index, r);
    //     Assert.Equal(2, res.TotalResults);

    //     Row r1 = res.GetRow(0);
    //     Assert.NotNull(r1);
    //     Assert.Equal("def", r1.GetString("name"));
    //     Assert.Equal(30, r1.GetLong("sum"));
    //     Assert.Equal(30, r1.GetDouble("sum"), 0);

    //     Assert.Equal(0L, r1.GetLong("nosuchcol"));
    //     Assert.Equal(0.0, r1.GetDouble("nosuchcol"), 0);
    //     Assert.Equal("", r1.GetString("nosuchcol"));

    //     Row r2 = res.GetRow(1);
    //     Assert.NotNull(r2);
    //     Assert.Equal("abc", r2.GetString("name"));
    //     Assert.Equal(10, r2.GetLong("sum"));
    // }

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

    // // TODO: underastant why its not working
    // [Fact]
    // public void TestApplyAndFilterAggregations()
    // {
    //     IDatabase db = redisFixture.Redis.GetDatabase();
    //     db.Execute("FLUSHALL");
    //     var ft = db.FT();
    //     Schema sc = new Schema();
    //     sc.AddTextField("name", 1.0, sortable: true);
    //     sc.AddNumericField("subj1", sortable: true);
    //     sc.AddNumericField("subj2", sortable: true);
    //     ft.Create(index, FTCreateParams.CreateParams(), sc);
    //     //    client.AddDocument(db, new Document("data1").Set("name", "abc").Set("subj1", 20).Set("subj2", 70));
    //     //    client.AddDocument(db, new Document("data2").Set("name", "def").Set("subj1", 60).Set("subj2", 40));
    //     //    client.AddDocument(db, new Document("data3").Set("name", "ghi").Set("subj1", 50).Set("subj2", 80));
    //     //    client.AddDocument(db, new Document("data4").Set("name", "abc").Set("subj1", 30).Set("subj2", 20));
    //     //    client.AddDocument(db, new Document("data5").Set("name", "def").Set("subj1", 65).Set("subj2", 45));
    //     //    client.AddDocument(db, new Document("data6").Set("name", "ghi").Set("subj1", 70).Set("subj2", 70));
    //     AddDocument(db, new Document("data1").Set("name", "abc").Set("subj1", 20).Set("subj2", 70));
    //     AddDocument(db, new Document("data2").Set("name", "def").Set("subj1", 60).Set("subj2", 40));
    //     AddDocument(db, new Document("data3").Set("name", "ghi").Set("subj1", 50).Set("subj2", 80));
    //     AddDocument(db, new Document("data4").Set("name", "abc").Set("subj1", 30).Set("subj2", 20));
    //     AddDocument(db, new Document("data5").Set("name", "def").Set("subj1", 65).Set("subj2", 45));
    //     AddDocument(db, new Document("data6").Set("name", "ghi").Set("subj1", 70).Set("subj2", 70));

    //     AggregationRequest r = new AggregationRequest().Apply("(@subj1+@subj2)/2", "attemptavg")
    //         .GroupBy("@name", Reducers.Avg("@attemptavg").As("avgscore"))
    //         .Filter("@avgscore>=50")
    //         .SortBy(10, SortedField.Asc("@name"));

    //     // actual search
    //     AggregationResult res = ft.Aggregate(index, r);
    //     Assert.Equal(3, res.TotalResults);

    //     Row r1 = res.GetRow(0);
    //     Assert.NotNull(r1);
    //     Assert.Equal("def", r1.GetString("name"));
    //     Assert.Equal(52.5, r1.GetDouble("avgscore"), 0);

    //     Row r2 = res.GetRow(1);
    //     Assert.NotNull(r2);
    //     Assert.Equal("ghi", r2.GetString("name"));
    //     Assert.Equal(67.5, r2.GetDouble("avgscore"), 0);
    // }

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
        db.HashSet("student:1111",  new HashEntry[] { new("first", "Joe"), new("last", "Dod"), new("age", "18") });
        db.HashSet("pupil:2222",    new HashEntry[] { new("first", "Jen"), new("last", "Rod"), new("age", "14") });
        db.HashSet("student:3333",  new HashEntry[] { new("first", "El"), new("last", "Mark"), new("age", "17") });
        db.HashSet("pupil:4444",    new HashEntry[] { new("first", "Pat"), new("last", "Shu"), new("age", "21") });
        db.HashSet("student:5555",  new HashEntry[] { new("first", "Joen"), new("last", "Ko"), new("age", "20") });
        db.HashSet("teacher:6666",  new HashEntry[] { new("first", "Pat"), new("last", "Rod"), new("age", "20") });

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
        db.HashSet("student:1111",  new HashEntry[] { new("first", "Joe"), new("last", "Dod"), new("age", "18") });
        db.HashSet("pupil:2222",    new HashEntry[] { new("first", "Jen"), new("last", "Rod"), new("age", "14") });
        db.HashSet("student:3333",  new HashEntry[] { new("first", "El"), new("last", "Mark"), new("age", "17") });
        db.HashSet("pupil:4444",    new HashEntry[] { new("first", "Pat"), new("last", "Shu"), new("age", "21") });
        db.HashSet("student:5555",  new HashEntry[] { new("first", "Joen"), new("last", "Ko"), new("age", "20") });
        db.HashSet("teacher:6666",  new HashEntry[] { new("first", "Pat"), new("last", "Rod"), new("age", "20") });
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
        db.HashSet("pupil:4444",   new HashEntry[] { new("first", "Pat"), new("last", "Shu"), new("age", 21) });
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
        db.HashSet("pupil:4444",   new HashEntry[] { new("first", "Pat"), new("last", "Shu"), new("age", 21) });
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

        db.HashSet("profesor:5555", new HashEntry[] { new("first", "Albert"), new("last", "Blue"),  new("age", "55") });
        db.HashSet("student:1111",  new HashEntry[] { new("first", "Joe"),    new("last", "Dod"),   new("age", "18") });
        db.HashSet("pupil:2222",    new HashEntry[] { new("first", "Jen"),    new("last", "Rod"),   new("age", "14") });
        db.HashSet("student:3333",  new HashEntry[] { new("first", "El"),     new("last", "Mark"),  new("age", "17") });
        db.HashSet("pupil:4444",    new HashEntry[] { new("first", "Pat"),    new("last", "Shu"),   new("age", "21") });
        db.HashSet("student:5555",  new HashEntry[] { new("first", "Joen"),   new("last", "Ko"),    new("age", "20") });
        db.HashSet("teacher:6666",  new HashEntry[] { new("first", "Pat"),    new("last", "Rod"),   new("age", "20") });

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

        db.HashSet("profesor:5555", new HashEntry[] { new("first", "Albert"), new("last", "Blue"),  new("age", "55") });
        db.HashSet("student:1111",  new HashEntry[] { new("first", "Joe"),    new("last", "Dod"),   new("age", "18") });
        db.HashSet("pupil:2222",    new HashEntry[] { new("first", "Jen"),    new("last", "Rod"),   new("age", "14") });
        db.HashSet("student:3333",  new HashEntry[] { new("first", "El"),     new("last", "Mark"),  new("age", "17") });
        db.HashSet("pupil:4444",    new HashEntry[] { new("first", "Pat"),    new("last", "Shu"),   new("age", "21") });
        db.HashSet("student:5555",  new HashEntry[] { new("first", "Joen"),   new("last", "Ko"),    new("age", "20") });
        db.HashSet("teacher:6666",  new HashEntry[] { new("first", "Pat"),    new("last", "Rod"),   new("age", "20") });

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
        Assert.Equal("title", (info.Attributes[0]["identifier"]).ToString());
        Assert.Equal("TAG", (info.Attributes[1]["type"]).ToString());
        Assert.Equal("name", (info.Attributes[2]["attribute"]).ToString());
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
        try { ft.ConfigSet("DEFAULT_DIALECT", "0"); } catch (RedisServerException) { }
        try { ft.ConfigSet("DEFAULT_DIALECT", "3"); } catch (RedisServerException) { }

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
        try { await ft.ConfigSetAsync("DEFAULT_DIALECT", "0"); } catch (RedisServerException) { }
        try { await ft.ConfigSetAsync("DEFAULT_DIALECT", "3"); } catch (RedisServerException) { }

        // Assert.Throws<RedisServerException>(() => ft.ConfigSet("DEFAULT_DIALECT", "0"));
        // Assert.Throws<RedisServerException>(() => ft.ConfigSet("DEFAULT_DIALECT", "3"));

        // Restore to default
        Assert.True(ft.ConfigSet("DEFAULT_DIALECT", "1"));
    }

    [Fact]
    public async Task TestCursor() // TODO: finish this test and understand whats the problem with aggregate (maybe related to the dispose?)
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
    public void TestDictionary()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();

        Assert.Equal(3L, ft.DictAdd("dict", "bar", "foo", "hello world"));

        var dumResult = ft.DictDump("dict");
        int i = 0;
        Assert.Equal("bar",dumResult[i++].ToString());
        Assert.Equal("foo",dumResult[i++].ToString());
        Assert.Equal("hello world",dumResult[i].ToString());

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

        RedisResult[] keys = (RedisResult[]) db.Execute("KEYS", "*");
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

        RedisResult[] keys = (RedisResult[]) db.Execute("KEYS", "*");
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
        Assert.Equal("bar",dumResult[i++].ToString());
        Assert.Equal("foo",dumResult[i++].ToString());
        Assert.Equal("hello world",dumResult[i].ToString());

        Assert.Equal(3L, await ft.DictDelAsync("dict", "foo", "bar", "hello world"));
        Assert.Equal((await ft.DictDumpAsync("dict")).Length, 0);
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