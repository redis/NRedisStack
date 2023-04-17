using System.Text.Json;
using System.Text.Json.Nodes;
using Moq;
using Newtonsoft.Json.Linq;
using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using NRedisStack.Search;
using NRedisStack.Search.Literals.Enums;
using StackExchange.Redis;
using Xunit;

namespace NRedisStack.Tests;

public class ExaplesTests : AbstractNRedisStackTest, IDisposable
{
    Mock<IDatabase> _mock = new Mock<IDatabase>();
    private readonly string key = "EXAMPLES_TESTS";
    public ExaplesTests(RedisFixture redisFixture) : base(redisFixture) { }

    public void Dispose()
    {
        redisFixture.Redis.GetDatabase().KeyDelete(key);
    }

    [Fact]
    public void HSETandSearch()
    {
        // Connect to the Redis server
        var redis = ConnectionMultiplexer.Connect("localhost");

        // Get a reference to the database and for search commands:
        var db = redis.GetDatabase();
        db.Execute("FLUSHALL");
        var ft = db.FT();

        // Use HSET to add a field-value pair to a hash
        db.HashSet("profesor:5555", new HashEntry[] { new("first", "Albert"), new("last", "Blue"), new("age", "55") });
        db.HashSet("student:1111", new HashEntry[] { new("first", "Joe"), new("last", "Dod"), new("age", "18") });
        db.HashSet("pupil:2222", new HashEntry[] { new("first", "Jen"), new("last", "Rod"), new("age", "14") });
        db.HashSet("student:3333", new HashEntry[] { new("first", "El"), new("last", "Mark"), new("age", "17") });
        db.HashSet("pupil:4444", new HashEntry[] { new("first", "Pat"), new("last", "Shu"), new("age", "21") });
        db.HashSet("student:5555", new HashEntry[] { new("first", "Joen"), new("last", "Ko"), new("age", "20") });
        db.HashSet("teacher:6666", new HashEntry[] { new("first", "Pat"), new("last", "Rod"), new("age", "20") });

        // Create the schema to index first and last as text fields, and age as a numeric field
        var schema = new Schema().AddTextField("first").AddTextField("last").AddNumericField("age");
        // Filter the index to only include hashes with an age greater than 16, and prefix of student: or pupil:
        var parameters = FTCreateParams.CreateParams().Filter("@age>16").Prefix("student:", "pupil:");
        // Create the index
        ft.Create("example_index", parameters, schema);

        //sleep:
        System.Threading.Thread.Sleep(2000);

        // Search all hashes in the index
        var noFilters = ft.Search("example_index", new Query());
        // noFilters now contains: student:1111, student:5555, pupil:4444, student:3333

        // Search for hashes with a first name starting with Jo
        var startWithJo = ft.Search("example_index", new Query("@first:Jo*"));
        // startWithJo now contains: student:1111 (Joe), student:5555 (Joen)

        // Search for hashes with first name of Pat
        var namedPat = ft.Search("example_index", new Query("@first:Pat"));
        // namedPat now contains pupil:4444 (Pat). teacher:6666 (Pat) is not included because it does not have a prefix of student: or pupil:

        // Search for hashes with last name of Rod
        var lastNameRod = ft.Search("example_index", new Query("@last:Rod"));
        // lastNameRod is empty because there are no hashes with a last name of Rod that match the index definition
        Assert.Equal(4, noFilters.TotalResults);
        Assert.Equal(2, startWithJo.TotalResults);
        Assert.Equal(1, namedPat.TotalResults);
        Assert.Equal(0, lastNameRod.TotalResults);
    }

    [Fact]
    public async Task AsyncExample()
    {
        // Connect to the Redis server
        var redis = await ConnectionMultiplexer.ConnectAsync("localhost");
        var db = redis.GetDatabase();
        db.Execute("FLUSHALL");
        var json = db.JSON();

        // call async version of JSON.SET/GET
        await json.SetAsync("key", "$", new { name = "John", age = 30, city = "New York" });
        var john = await json.GetAsync("key");
    }

    [Fact]
    public void PipelineExample()
    {
        // Pipeline can get IDatabase for pipeline
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var pipeline = new Pipeline(db);

        // Add JsonSet to pipeline
        pipeline.Json.SetAsync("person", "$", new { name = "John", age = 30, city = "New York", nicknames = new[] { "John", "Johny", "Jo" } });

        // Increase age by 2
        pipeline.Json.NumIncrbyAsync("person", "$.age", 2);

        // Clear the nicknames from the Json
        pipeline.Json.ClearAsync("person", "$.nicknames");

        // Del the nicknames
        pipeline.Json.DelAsync("person", "$.nicknames");

        // Get the Json response
        var getResponse = pipeline.Json.GetAsync("person");

        // Execute the pipeline
        pipeline.Execute();

        // Get the result back JSON
        var result = getResponse.Result;

        // Assert the result
        var expected = "{\"name\":\"John\",\"age\":32,\"city\":\"New York\"}";
        Assert.Equal(expected, result.ToString());
    }

    [Fact]
    public async Task JsonWithSearchPipeline()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        //Setup pipeline connection
        var pipeline = new Pipeline(db);

        // Add JsonSet to pipeline
        _ = pipeline.Json.SetAsync("person:01", "$", new { name = "John", age = 30, city = "New York" });
        _ = pipeline.Json.SetAsync("person:02", "$", new { name = "Joy", age = 25, city = "Los Angeles" });
        _ = pipeline.Json.SetAsync("person:03", "$", new { name = "Mark", age = 21, city = "Chicago" });
        _ = pipeline.Json.SetAsync("person:04", "$", new { name = "Steve", age = 24, city = "Phoenix" });
        _ = pipeline.Json.SetAsync("person:05", "$", new { name = "Michael", age = 55, city = "San Antonio" });

        // Create the schema to index name as text field, age as a numeric field and city as tag field.
        var schema = new Schema().AddTextField("name").AddNumericField("age", true).AddTagField("city");

        // Filter the index to only include Jsons with prefix of person:
        var parameters = FTCreateParams.CreateParams().On(IndexDataType.JSON).Prefix("person:");

        // Create the index via pipeline
        var create = pipeline.Ft.CreateAsync("person-idx", parameters, schema);

        // execute the pipeline
        pipeline.Execute();

        // Search for all indexed person records
        Task.Delay(2000).Wait();
        var getAllPersons = await db.FT().SearchAsync("person-idx", new Query());


        // Get the total count of people records that indexed.
        var count = getAllPersons.TotalResults;

        // Gets the first person form the result.
        var firstPerson = getAllPersons.Documents.FirstOrDefault();
        // first person is John here.

        Assert.True(create.Result);
        Assert.Equal(5, count);
        //Assert.Equal("person:01", firstPerson?.Id);
    }

    [Fact]
    public async Task PipelineWithAsync()
    {
        // Connect to the Redis server
        var redis = ConnectionMultiplexer.Connect("localhost");

        // Get a reference to the database
        var db = redis.GetDatabase();
        db.Execute("FLUSHALL");
        // Setup pipeline connection

        var pipeline = new Pipeline(db);

        // Create metedata lables for time-series.
        TimeSeriesLabel label1 = new TimeSeriesLabel("temp", "TLV");
        TimeSeriesLabel label2 = new TimeSeriesLabel("temp", "JLM");
        var labels1 = new List<TimeSeriesLabel> { label1 };
        var labels2 = new List<TimeSeriesLabel> { label2 };

        // Create a new time-series.
        pipeline.Ts.CreateAsync("temp:TLV", labels: labels1);
        pipeline.Ts.CreateAsync("temp:JLM", labels: labels2);

        // Adding multiple sequenece of time-series data.
        List<(string, TimeStamp, double)> sequence1 = new List<(string, TimeStamp, double)>()
        {
            ("temp:TLV",1000,30),
            ("temp:TLV", 1010 ,35),
            ("temp:TLV", 1020, 9999),
            ("temp:TLV", 1030, 40)
        };
        List<(string, TimeStamp, double)> sequence2 = new List<(string, TimeStamp, double)>()
        {
            ("temp:JLM",1005,30),
            ("temp:JLM", 1015 ,35),
            ("temp:JLM", 1025, 9999),
            ("temp:JLM", 1035, 40)
        };

        // Adding mutiple samples to mutiple series.
        pipeline.Ts.MAddAsync(sequence1);
        pipeline.Ts.MAddAsync(sequence2);

        // Execute the pipeline
        pipeline.Execute();

        // Get a reference to the database and for time-series commands
        var ts = db.TS();

        // Get only the location label for each last sample, use SELECTED_LABELS.
        var respons = await ts.MGetAsync(new List<string> { "temp=JLM" }, selectedLabels: new List<string> { "location" });

        // Assert the respons
        Assert.Equal(1, respons.Count);
        Assert.Equal("temp:JLM", respons[0].key);
    }

    [Fact]
    public async Task TransactionExample()
    {
        // Connect to the Redis server
        var redis = ConnectionMultiplexer.Connect("localhost");

        // Get a reference to the database
        var db = redis.GetDatabase();
        db.Execute("FLUSHALL");

        // Setup transaction with IDatabase
        var tran = new Transaction(db);

        // Add account details with Json.Set to transaction
        tran.Json.SetAsync("accdetails:Jeeva", "$", new { name = "Jeeva", totalAmount = 1000, bankName = "City" });
        tran.Json.SetAsync("accdetails:Shachar", "$", new { name = "Shachar", totalAmount = 1000, bankName = "City" });

        // Get the Json response
        var getShachar = tran.Json.GetAsync("accdetails:Shachar");
        var getJeeva = tran.Json.GetAsync("accdetails:Jeeva");

        // Debit 200 from Jeeva
        tran.Json.NumIncrbyAsync("accdetails:Jeeva", "$.totalAmount", -200);

        // Credit 200 from Shachar
        tran.Json.NumIncrbyAsync("accdetails:Shachar", "$.totalAmount", 200);

        // Get total amount for both Jeeva = 800 & Shachar = 1200
        var totalAmtOfJeeva = tran.Json.GetAsync("accdetails:Jeeva", path: "$.totalAmount");
        var totalAmtOfShachar = tran.Json.GetAsync("accdetails:Shachar", path: "$.totalAmount");

        // Execute the transaction
        var condition = tran.ExecuteAsync();

        // Assert
        Assert.True(condition.Result);
        Assert.NotEmpty(getJeeva.Result.ToString());
        Assert.NotEmpty(getShachar.Result.ToString());
        Assert.Equal("[800]", totalAmtOfJeeva.Result.ToString());
        Assert.Equal("[1200]", totalAmtOfShachar.Result.ToString());
    }

    [Fact]
    public void TestJsonConvert()
    {
        ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
        IDatabase db = redis.GetDatabase();
        db.Execute("FLUSHALL");
        ISearchCommands ft = db.FT();
        IJsonCommands json = db.JSON();

        ft.Create("test", new FTCreateParams().On(IndexDataType.JSON).Prefix("doc:"),
            new Schema().AddTagField(new FieldName("$.name", "name")));
        for (int i = 0; i < 10; i++)
        {
            json.Set("doc:" + i, "$", "{\"name\":\"foo\"}");
        }
        var res = ft.Search("test", new Query("@name:{foo}"));

        var docs = res.ToJson();

        Assert.Equal(10, docs.Count());
    }

    [Fact]
    public void BasicJsonExamplesTest()
    {
        ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
        IDatabase db = redis.GetDatabase();
        db.Execute("FLUSHALL");
        IJsonCommands json = db.JSON();

        // Insert a simple KVP as a JSON object:
        Assert.True(json.Set("ex1:1", "$", "\"val\""));
        // Insert a single-property JSON object:
        Assert.True(json.Set("ex1:2", "$", new { field1 = "val1" }));
        // Insert a JSON object with multiple properties:
        Assert.True(json.Set("ex1:3", "$", new
        {
            field1 = "val1",
            field2 = "val2"
        }));

        // Insert a JSON object with multiple properties of different data types:
        Assert.True(json.Set("ex1:4", "$", new
        {
            field1 = "val1",
            field2 = "val2",
            field3 = true,
            field4 = (string?)null
        }));

        // Insert a JSON object that contains an array:
        Assert.True(json.Set("ex1:5", "$", new
        {
            arr1 = new[] { "val1", "val2", "val3" }
        }));

        // Insert a JSON object that contains a nested object:
        Assert.True(json.Set("ex1:6", "$", new
        {
            obj1 = new
            {
                str1 = "val1",
                num2 = 2
            }
        }));

        // Insert a JSON object with a mixture of property data types:
        Assert.True(json.Set("ex1:7", "$", new
        {
            str1 = "val1",
            str2 = "val2",
            arr1 = new[] { 1, 2, 3, 4 },
            obj1 = new
            {
                num1 = 1,
                arr2 = new[] { "val1", "val2", "val3" }
            }
        }));

        // Set and Fetch a simple JSON KVP:
        json.Set("ex2:1", "$", "\"val\"");
        var res = json.Get(key: "ex2:1",
            path: "$",
            indent: "\t",
            newLine: "\n"
        );
        Assert.Equal("[\n\t\"val\"\n]", res.ToString());

        // Set and Fetch a single property from a JSON object:
        json.Set("ex2:2", "$", new
        {
            field1 = "val1"
        });
        res = json.Get(key: "ex2:2",
             path: "$.field1",
             indent: "\t",
             newLine: "\n"
         );
        Assert.Equal("[\n\t\"val1\"\n]", res.ToString());

        // Fetch multiple properties:
        json.Set("ex2:3", "$", new
        {
            field1 = "val1",
            field2 = "val2"
        });
        // sleep
        Thread.Sleep(500);
        res = json.Get(key: "ex2:3",
            paths: new[] { "$.field1", "$.field2" },
            indent: "\t",
            newLine: "\n"
        );

        var actualJson = res.ToString();
        var expectedJson1 = "{\n\t\"$.field1\":[\n\t\t\"val1\"\n\t],\n\t\"$.field2\":[\n\t\t\"val2\"\n\t]\n}";
        var expectedJson2 = "{\n\t\"$.field2\":[\n\t\t\"val2\"\n\t],\n\t\"$.field1\":[\n\t\t\"val1\"\n\t]\n}";

        Assert.True(actualJson == expectedJson1 || actualJson == expectedJson2);

        // Fetch a property nested in another JSON object:
        json.Set("ex2:4", "$", new
        {
            obj1 = new
            {
                str1 = "val1",
                num2 = 2
            }
        });
        res = json.Get(key: "ex2:4",
            path: "$.obj1.num2",
            indent: "\t",
            newLine: "\n"
        );
        Assert.Equal("[\n\t2\n]", res.ToString());

        // Fetch properties within an array and utilize array subscripting:
        json.Set("ex2:5", "$", new
        {
            str1 = "val1",
            str2 = "val2",
            arr1 = new[] { 1, 2, 3, 4 },
            obj1 = new
            {
                num1 = 1,
                arr2 = new[] { "val1", "val2", "val3" }
            }
        });
        res = json.Get(key: "ex2:5",
            path: "$.obj1.arr2",
            indent: "\t",
            newLine: "\n"
        );
        Assert.Equal("[\n\t[\n\t\t\"val1\",\n\t\t\"val2\",\n\t\t\"val3\"\n\t]\n]", res.ToString());
        res = json.Get(key: "ex2:5",
            path: "$.arr1[1]",
            indent: "\t",
            newLine: "\n"
        );
        Assert.Equal("[\n\t2\n]", res.ToString());
        res = json.Get(key: "ex2:5",
            path: "$.obj1.arr2[0:2]",
            indent: "\t",
            newLine: "\n"
        );
        Assert.Equal("[\n\t\"val1\",\n\t\"val2\"\n]", res.ToString());
        res = json.Get(key: "ex2:5",
            path: "$.arr1[-2:]",
            indent: "\t",
            newLine: "\n"
        );
        Assert.Equal("[\n\t3,\n\t4\n]", res.ToString());

        // Update an entire JSON object:
        json.Set("ex3:1", "$", new { field1 = "val1" });
        json.Set("ex3:1", "$", new { foo = "bar" });
        res = json.Get(key: "ex3:1",
            indent: "\t",
            newLine: "\n"
        );
        Assert.Equal("{\n\t\"foo\":\"bar\"\n}", res.ToString());

        // Update a single property within an object:
        json.Set("ex3:2", "$", new
        {
            field1 = "val1",
            field2 = "val2"
        });
        json.Set("ex3:2", "$.field1", "\"foo\"");
        res = json.Get(key: "ex3:2",
            indent: "\t",
            newLine: "\n"
        );
        Assert.Equal("{\n\t\"field1\":\"foo\",\n\t\"field2\":\"val2\"\n}", res.ToString());

        // Update a property in an embedded JSON object:
        json.Set("ex3:3", "$", new
        {
            obj1 = new
            {
                str1 = "val1",
                num2 = 2
            }
        });
        json.Set("ex3:3", "$.obj1.num2", 3);
        res = json.Get(key: "ex3:3",
            indent: "\t",
            newLine: "\n"
        );
        Assert.Equal("{\n\t\"obj1\":{\n\t\t\"str1\":\"val1\",\n\t\t\"num2\":3\n\t}\n}", res.ToString());

        // Update an item in an array via index:
        json.Set("ex3:4", "$", new
        {
            arr1 = new[] { "val1", "val2", "val3" }
        });
        json.Set("ex3:4", "$.arr1[0]", "\"foo\"");
        res = json.Get(key: "ex3:4",
            indent: "\t",
            newLine: "\n"
        );
        Assert.Equal("{\n\t\"arr1\":[\n\t\t\"foo\",\n\t\t\"val2\",\n\t\t\"val3\"\n\t]\n}", res.ToString());

        // Delete entire object/key:
        json.Set("ex4:1", "$", new { field1 = "val1" });
        json.Del("ex4:1");
        res = json.Get(key: "ex4:1",
            indent: "\t",
            newLine: "\n"
        );
        Assert.Equal("", res.ToString());

        // Delete a single property from an object:
        json.Set("ex4:2", "$", new
        {
            field1 = "val1",
            field2 = "val2"
        });
        json.Del("ex4:2", "$.field1");
        res = json.Get(key: "ex4:2",
            indent: "\t",
            newLine: "\n"
        );
        Assert.Equal("{\n\t\"field2\":\"val2\"\n}", res.ToString());

        // Delete a property from an embedded object:
        json.Set("ex4:3", "$", new
        {
            obj1 = new
            {
                str1 = "val1",
                num2 = 2
            }
        });
        json.Del("ex4:3", "$.obj1.num2");
        res = json.Get(key: "ex4:3",
            indent: "\t",
            newLine: "\n"
        );
        Assert.Equal("{\n\t\"obj1\":{\n\t\t\"str1\":\"val1\"\n\t}\n}", res.ToString());

        // Delete a single item from an array:
        json.Set("ex4:4", "$", new
        {
            arr1 = new[] { "val1", "val2", "val3" }
        });
        json.Del("ex4:4", "$.arr1[0]");
        res = json.Get(key: "ex4:4",
            indent: "\t",
            newLine: "\n"
        );
        Assert.Equal("{\n\t\"arr1\":[\n\t\t\"val2\",\n\t\t\"val3\"\n\t]\n}", res.ToString());
    }

    [Fact]
    public void AdvancedJsonExamplesTest()
    {
        ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
        IDatabase db = redis.GetDatabase();
        db.Execute("FLUSHALL");
        IJsonCommands json = db.JSON();
        
        json.Set("warehouse:1", "$", new
        {
            city = "Boston",
            location = "42.361145, -71.057083",
            inventory = new[] {
                    new {
                        id = 15970,
                        gender = "Men",
                        season = new[] {"Fall", "Winter"},
                        description = "Turtle Check Men Navy Blue Shirt",
                        price = 34.95
                    },
                    new {
                        id = 59263,
                        gender = "Women",
                        season = new[] {"Fall", "Winter", "Spring", "Summer"},
                        description = "Titan Women Silver Watch",
                        price = 129.99
                    },
                    new {
                        id = 46885,
                        gender = "Boys",
                        season = new[] {"Fall"},
                        description =  "Ben 10 Boys Navy Blue Slippers",
                        price = 45.99
                    }
                }
        });

        // Fetch all properties of an array:
        var res = json.Get(key: "warehouse:1",
                path: "$.inventory[*]",
                indent: "\t",
                newLine: "\n"
            );
        var expected = "[\n\t{\n\t\t\"id\":15970,\n\t\t\"gender\":\"Men\",\n\t\t\"season\":[\n\t\t\t\"Fall\",\n\t\t\t\"Winter\"\n\t\t],\n\t\t\"description\":\"Turtle Check Men Navy Blue Shirt\",\n\t\t\"price\":34.95\n\t},\n\t{\n\t\t\"id\":59263,\n\t\t\"gender\":\"Women\",\n\t\t\"season\":[\n\t\t\t\"Fall\",\n\t\t\t\"Winter\",\n\t\t\t\"Spring\",\n\t\t\t\"Summer\"\n\t\t],\n\t\t\"description\":\"Titan Women Silver Watch\",\n\t\t\"price\":129.99\n\t},\n\t{\n\t\t\"id\":46885,\n\t\t\"gender\":\"Boys\",\n\t\t\"season\":[\n\t\t\t\"Fall\"\n\t\t],\n\t\t\"description\":\"Ben 10 Boys Navy Blue Slippers\",\n\t\t\"price\":45.99\n\t}\n]";
        Assert.Equal(expected, res.ToString()); // TODO: fine nicer way to compare the two JSON strings


        // Fetch all values of a field within an array:
        res = json.Get(
                    key: "warehouse:1",
                    path: "$.inventory[*].price",
                    indent: "\t",
                    newLine: "\n"
        );
        expected = "[\n\t34.95,\n\t129.99,\n\t45.99\n]";
        Assert.Equal(expected, res.ToString());

        // Fetch all items within an array where a text field matches a given value:
        res = json.Get(
                    key: "warehouse:1",
                    path: "$.inventory[?(@.description==\"Turtle Check Men Navy Blue Shirt\")]",
                    indent: "\t",
                    newLine: "\n"
        );

        expected = "[\n\t{\n\t\t\"id\":15970,\n\t\t\"gender\":\"Men\",\n\t\t\"season\":[\n\t\t\t\"Fall\",\n\t\t\t\"Winter\"\n\t\t],\n\t\t\"description\":\"Turtle Check Men Navy Blue Shirt\",\n\t\t\"price\":34.95\n\t}\n]";
        Assert.Equal(expected, res.ToString());

        // Fetch all items within an array where a numeric field is less than a given value:
        res = json.Get(key: "warehouse:1",
                    path: "$.inventory[?(@.price<100)]",
                    indent: "\t",
                    newLine: "\n"
                );
        expected = "[\n\t{\n\t\t\"id\":15970,\n\t\t\"gender\":\"Men\",\n\t\t\"season\":[\n\t\t\t\"Fall\",\n\t\t\t\"Winter\"\n\t\t],\n\t\t\"description\":\"Turtle Check Men Navy Blue Shirt\",\n\t\t\"price\":34.95\n\t},\n\t{\n\t\t\"id\":46885,\n\t\t\"gender\":\"Boys\",\n\t\t\"season\":[\n\t\t\t\"Fall\"\n\t\t],\n\t\t\"description\":\"Ben 10 Boys Navy Blue Slippers\",\n\t\t\"price\":45.99\n\t}\n]";
        Assert.Equal(expected, res.ToString());

        // Fetch all items within an array where a numeric field is less than a given value:
        res = json.Get(key: "warehouse:1",
                    path: "$.inventory[?(@.id>=20000)]",
                    indent: "\t",
                    newLine: "\n"
                );
        expected = "[\n\t{\n\t\t\"id\":59263,\n\t\t\"gender\":\"Women\",\n\t\t\"season\":[\n\t\t\t\"Fall\",\n\t\t\t\"Winter\",\n\t\t\t\"Spring\",\n\t\t\t\"Summer\"\n\t\t],\n\t\t\"description\":\"Titan Women Silver Watch\",\n\t\t\"price\":129.99\n\t},\n\t{\n\t\t\"id\":46885,\n\t\t\"gender\":\"Boys\",\n\t\t\"season\":[\n\t\t\t\"Fall\"\n\t\t],\n\t\t\"description\":\"Ben 10 Boys Navy Blue Slippers\",\n\t\t\"price\":45.99\n\t}\n]";
        //Assert.Equal(expected, res.ToString());

        // Fetch all items within an array where a numeric field is less than a given value:
        res = json.Get(key: "warehouse:1",
                    path: "$.inventory[?(@.gender==\"Men\"&&@.price>20)]",
                    indent: "\t",
                    newLine: "\n"
                );
        expected = "[\n\t{\n\t\t\"id\":15970,\n\t\t\"gender\":\"Men\",\n\t\t\"season\":[\n\t\t\t\"Fall\",\n\t\t\t\"Winter\"\n\t\t],\n\t\t\"description\":\"Turtle Check Men Navy Blue Shirt\",\n\t\t\"price\":34.95\n\t}\n]";
        Assert.Equal(expected, res.ToString());

        // Fetch all items within an array that meet at least one relational operation.
        // In this case, return only the ids of those items:
        res = json.Get(key: "warehouse:1",
                    path: "$.inventory[?(@.price<100||@.gender==\"Women\")].id",
                    indent: "\t",
                    newLine: "\n"
                );
        expected = "[\n\t15970,\n\t59263,\n\t46885\n]";
        Assert.Equal(expected, res.ToString());

        // Fetch all items within an array that match a given regex pattern.
        res = json.Get(key: "warehouse:1",
                    path: "$.inventory[?(@.description =~ \"Blue\")]",
                    indent: "\t",
                    newLine: "\n"
                );
        expected = "[\n\t{\n\t\t\"id\":15970,\n\t\t\"gender\":\"Men\",\n\t\t\"season\":[\n\t\t\t\"Fall\",\n\t\t\t\"Winter\"\n\t\t],\n\t\t\"description\":\"Turtle Check Men Navy Blue Shirt\",\n\t\t\"price\":34.95\n\t},\n\t{\n\t\t\"id\":46885,\n\t\t\"gender\":\"Boys\",\n\t\t\"season\":[\n\t\t\t\"Fall\"\n\t\t],\n\t\t\"description\":\"Ben 10 Boys Navy Blue Slippers\",\n\t\t\"price\":45.99\n\t}\n]";
        Assert.Equal(expected, res.ToString());

        // Fetch all items within an array where a field contains a term, case insensitive
        res = json.Get(key: "warehouse:1",
                    path: "$.inventory[?(@.description =~ \"(?i)watch\")]",
                    indent: "\t",
                    newLine: "\n"
                );
        expected = "[\n\t{\n\t\t\"id\":59263,\n\t\t\"gender\":\"Women\",\n\t\t\"season\":[\n\t\t\t\"Fall\",\n\t\t\t\"Winter\",\n\t\t\t\"Spring\",\n\t\t\t\"Summer\"\n\t\t],\n\t\t\"description\":\"Titan Women Silver Watch\",\n\t\t\"price\":129.99\n\t}\n]";
        Assert.Equal(expected, res.ToString());

        // Fetch all items within an array where a field begins with a given expression
        res = json.Get(key: "warehouse:1",
                    path: "$.inventory[?(@.description =~ \"^T\")]",
                    indent: "\t",
                    newLine: "\n"
                );
        expected = "[\n\t{\n\t\t\"id\":15970,\n\t\t\"gender\":\"Men\",\n\t\t\"season\":[\n\t\t\t\"Fall\",\n\t\t\t\"Winter\"\n\t\t],\n\t\t\"description\":\"Turtle Check Men Navy Blue Shirt\",\n\t\t\"price\":34.95\n\t},\n\t{\n\t\t\"id\":59263,\n\t\t\"gender\":\"Women\",\n\t\t\"season\":[\n\t\t\t\"Fall\",\n\t\t\t\"Winter\",\n\t\t\t\"Spring\",\n\t\t\t\"Summer\"\n\t\t],\n\t\t\"description\":\"Titan Women Silver Watch\",\n\t\t\"price\":129.99\n\t}\n]";
        Assert.Equal(expected, res.ToString());
    }
}