using Xunit;
using StackExchange.Redis;
using NRedisStack.RedisStackCommands;
using Moq;
using NRedisStack.Search.FT.CREATE;
using NRedisStack.Search;
using static NRedisStack.Search.Schema;

namespace NRedisStack.Tests.Search;

public class SearchTests : AbstractNRedisStackTest, IDisposable
{
    Mock<IDatabase> _mock = new Mock<IDatabase>();
    private readonly string key = "SEARCH_TESTS";
    private readonly string index = "TEST_INDEX";
    public SearchTests(RedisFixture redisFixture) : base(redisFixture) { }

    public void Dispose()
    {
        redisFixture.Redis.GetDatabase().KeyDelete(key);
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

        //TODO: complete this test when I finish the command FT.INFO
        // var info = ft.Info(index);
        // Assert.Equal(index, info.get("index_name"));
        // Assert.Equal("identifier", ((List)((List)info.get("attributes")).get(1)).get(0));
        // Assert.Equal("attribute", ((List)((List)info.get("attributes")).get(1)).get(2));
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

        //TODO: complete this test when I finish the command FT.INFO
        // var info = ft.Info(index);
        // Assert.Equal(index, info.get("index_name"));
        // Assert.Equal("identifier", ((List)((List)info.get("attributes")).get(1)).get(0));
        // Assert.Equal("attribute", ((List)((List)info.get("attributes")).get(1)).get(2));
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