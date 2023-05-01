using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Moq;
using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using NRedisStack.Search;
using NRedisStack.Search.Aggregation;
using NRedisStack.Search.Literals.Enums;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Math;
using Org.BouncyCastle.OpenSsl;
using StackExchange.Redis;
using Xunit;
using static NRedisStack.Search.Schema;

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
        // var redis = ConnectionMultiplexer.Connect("localhost");

        // Get a reference to the database and for search commands:
        // var db = redis.GetDatabase();
        var db =  redisFixture.Redis.GetDatabase();
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
        // var redis = await ConnectionMultiplexer.ConnectAsync("localhost");
        // var db = redis.GetDatabase();
        var db = redisFixture.Redis.GetDatabase();
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
        // Assert.Equal("person:01", firstPerson?.Id);
    }

    [Fact]
    public async Task PipelineWithAsync()
    {
        // Connect to the Redis server
        // var redis = ConnectionMultiplexer.Connect("localhost");

        // Get a reference to the database
        // var db = redis.GetDatabase();
        var db =  redisFixture.Redis.GetDatabase();
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
        // var redis = ConnectionMultiplexer.Connect("localhost");

        // Get a reference to the database
        // var db = redis.GetDatabase();

        var db = redisFixture.Redis.GetDatabase();
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
        // ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
        // IDatabase db = redis.GetDatabase();

        var db =  redisFixture.Redis.GetDatabase();
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

#if NET481
    [Fact]
    public void TestRedisCloudConnection()
    {
        var root = Path.GetFullPath(Directory.GetCurrentDirectory());
        var redisCaPath = Path.GetFullPath(Path.Combine(root, "redis_ca.pem"));
        var redisUserCrtPath  = Path.GetFullPath(Path.Combine(root, "redis_user.crt"));
        var redisUserPrivateKeyPath  = Path.GetFullPath(Path.Combine(root, "redis_user_private.key"));

        var password = Environment.GetEnvironmentVariable("PASSWORD") ?? throw new Exception("PASSWORD is not set.");
        var endpoint = Environment.GetEnvironmentVariable("ENDPOINT") ?? throw new Exception("ENDPOINT is not set.");

        // Load the Redis credentials
        var redisUserCertificate = new X509Certificate2(File.ReadAllBytes(redisUserCrtPath));
        var redisCaCertificate = new X509Certificate2(File.ReadAllBytes(redisCaPath));

        var rsa = RSA.Create();

        var redisUserPrivateKeyText = File.ReadAllText(redisUserPrivateKeyPath);
        var pemFileData = File.ReadAllLines(redisUserPrivateKeyPath).Where(x => !x.StartsWith("-"));
        var binaryEncoding = Convert.FromBase64String(string.Join(null, pemFileData));

        rsa.ImportParameters(ImportPrivateKey(File.ReadAllText(redisUserPrivateKeyPath)));
        redisUserCertificate.CopyWithPrivateKey(rsa);
        rsa.ImportParameters(ImportPrivateKey(File.ReadAllText(redisUserPrivateKeyText)));
        var clientCert = redisUserCertificate.CopyWithPrivateKey(rsa);

        // Connect to Redis Cloud
        var redisConfiguration = new ConfigurationOptions
        {
            EndPoints = { endpoint },
            Ssl = true,
            Password = password
        };

        redisConfiguration.CertificateSelection += (_, _, _, _, _) => clientCert;

        redisConfiguration.CertificateValidation += (_, cert, _, errors) =>
        {
            if (errors == SslPolicyErrors.None)
            {
                return true;
            }

            var privateChain = new X509Chain();
            privateChain.ChainPolicy = new X509ChainPolicy { RevocationMode = X509RevocationMode.NoCheck };
            X509Certificate2 cert2 = new X509Certificate2(cert!);
            privateChain.ChainPolicy.ExtraStore.Add(redisCaCertificate);
            privateChain.Build(cert2);

            bool isValid = true;

            // we're establishing the trust chain so if the only complaint is that that the root CA is untrusted, and the root CA root
            // matches our certificate, we know it's ok
            foreach (X509ChainStatus chainStatus in privateChain.ChainStatus.Where(x =>
                         x.Status != X509ChainStatusFlags.UntrustedRoot))
            {
                if (chainStatus.Status != X509ChainStatusFlags.NoError)
                {
                    isValid = false;
                    break;
                }
            }

            return isValid;
        };


        var redis = ConnectionMultiplexer.Connect(redisConfiguration);
        var db = redis.GetDatabase();
        db.Ping();
    }

    public static RSAParameters ImportPrivateKey(string pem)
    {
        PemReader pr = new PemReader(new StringReader(pem));
        RsaPrivateCrtKeyParameters privKey = (RsaPrivateCrtKeyParameters)pr.ReadObject();
        RSAParameters rp = new RSAParameters();
        rp.Modulus = privKey.Modulus.ToByteArrayUnsigned();
        rp.Exponent = privKey.PublicExponent.ToByteArrayUnsigned();
        rp.P = privKey.P.ToByteArrayUnsigned();
        rp.Q = privKey.Q.ToByteArrayUnsigned();
        rp.D = ConvertRSAParametersField(privKey.Exponent, rp.Modulus.Length);
        rp.DP = ConvertRSAParametersField(privKey.DP, rp.P.Length);
        rp.DQ = ConvertRSAParametersField(privKey.DQ, rp.Q.Length);
        rp.InverseQ = ConvertRSAParametersField(privKey.QInv, rp.Q.Length);

        return rp;
    }

    private static byte[] ConvertRSAParametersField(BigInteger n, int size)
    {
        byte[] bs = n.ToByteArrayUnsigned();
        if (bs.Length == size)
            return bs;
        if (bs.Length > size)
            throw new ArgumentException("Specified size too small", "size");
        byte[] padded = new byte[size];
        Array.Copy(bs, 0, padded, size - bs.Length, bs.Length);
        return padded;
    }
#endif

#if NET6_0_OR_GREATER
    [Fact]
    public void TestRedisCloudConnection()
    {
        var root = Path.GetFullPath(Directory.GetCurrentDirectory());
        var redisCaPath = Path.GetFullPath(Path.Combine(root, "redis_ca.pem"));
        var redisUserCrtPath  = Path.GetFullPath(Path.Combine(root, "redis_user.crt"));
        var redisUserPrivateKeyPath  = Path.GetFullPath(Path.Combine(root, "redis_user_private.key"));

        var password = Environment.GetEnvironmentVariable("PASSWORD") ?? throw new Exception("PASSWORD is not set.");
        var endpoint = Environment.GetEnvironmentVariable("ENDPOINT") ?? throw new Exception("ENDPOINT is not set.");

        // Load the Redis credentials
        var redisUserCertificate = new X509Certificate2(File.ReadAllBytes(redisUserCrtPath));
        var redisCaCertificate = new X509Certificate2(File.ReadAllBytes(redisCaPath));

        var rsa = RSA.Create();

        var redisUserPrivateKeyText = File.ReadAllText(redisUserPrivateKeyPath);
        var pemFileData = File.ReadAllLines(redisUserPrivateKeyPath).Where(x => !x.StartsWith("-"));
        var binaryEncoding = Convert.FromBase64String(string.Join(null, pemFileData));

        rsa.ImportRSAPrivateKey(binaryEncoding, out _);
        redisUserCertificate.CopyWithPrivateKey(rsa);
        rsa.ImportFromPem(redisUserPrivateKeyText.ToCharArray());
        var clientCert = redisUserCertificate.CopyWithPrivateKey(rsa);

        // Connect to Redis Cloud
        var redisConfiguration = new ConfigurationOptions
        {
            EndPoints = { endpoint },
            Ssl = true,
            Password = password
        };

        redisConfiguration.CertificateSelection += (_, _, _, _, _) => clientCert;

        redisConfiguration.CertificateValidation += (_, cert, _, errors) =>
        {
            if (errors == SslPolicyErrors.None)
            {
                return true;
            }

            var privateChain = new X509Chain();
            privateChain.ChainPolicy = new X509ChainPolicy { RevocationMode = X509RevocationMode.NoCheck };
            X509Certificate2 cert2 = new X509Certificate2(cert!);
            privateChain.ChainPolicy.ExtraStore.Add(redisCaCertificate);
            privateChain.Build(cert2);

            bool isValid = true;

            // we're establishing the trust chain so if the only complaint is that that the root CA is untrusted, and the root CA root
            // matches our certificate, we know it's ok
            foreach (X509ChainStatus chainStatus in privateChain.ChainStatus.Where(x =>
                         x.Status != X509ChainStatusFlags.UntrustedRoot))
            {
                if (chainStatus.Status != X509ChainStatusFlags.NoError)
                {
                    isValid = false;
                    break;
                }
            }

            return isValid;
        };


        var redis = ConnectionMultiplexer.Connect(redisConfiguration);
        var db = redis.GetDatabase();
        db.Ping();
    }

    [Fact]
    public void TestRedisCloudConnection_DotnetCore3()
    {
        // Replace this with your own Redis Cloud credentials
        var root = Path.GetFullPath(Directory.GetCurrentDirectory());
        var redisCaPath = Path.GetFullPath(Path.Combine(root, "redis_ca.pem"));
        var redisUserCrtPath  = Path.GetFullPath(Path.Combine(root, "redis_user.crt"));
        var redisUserPrivateKeyPath  = Path.GetFullPath(Path.Combine(root, "redis_user_private.key"));

        var password = Environment.GetEnvironmentVariable("PASSWORD") ?? throw new Exception("PASSWORD is not set.");
        var endpoint = Environment.GetEnvironmentVariable("ENDPOINT") ?? throw new Exception("ENDPOINT is not set.");

        // Load the Redis credentials
        var redisUserCertificate = new X509Certificate2(File.ReadAllBytes(redisUserCrtPath));
        var redisCaCertificate = new X509Certificate2(File.ReadAllBytes(redisCaPath));

        var rsa = RSA.Create();

        var redisUserPrivateKeyText = File.ReadAllText(redisUserPrivateKeyPath);
        var pemFileData = File.ReadAllLines(redisUserPrivateKeyPath).Where(x => !x.StartsWith("-"));
        var binaryEncoding = Convert.FromBase64String(string.Join(null, pemFileData));

        rsa.ImportRSAPrivateKey(binaryEncoding, out _);
        redisUserCertificate.CopyWithPrivateKey(rsa);
        rsa.ImportFromPem(redisUserPrivateKeyText.ToCharArray());
        var clientCert = redisUserCertificate.CopyWithPrivateKey(rsa);

        var sslOptions = new SslClientAuthenticationOptions
        {
            CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
            LocalCertificateSelectionCallback = (_, _, _, _, _) => clientCert,
            RemoteCertificateValidationCallback = (_, cert, _, errors) =>
            {
                if (errors == SslPolicyErrors.None)
                {
                    return true;
                }

                var privateChain = new X509Chain();
                privateChain.ChainPolicy = new X509ChainPolicy { RevocationMode = X509RevocationMode.NoCheck };
                X509Certificate2 cert2 = new X509Certificate2(cert!);
                privateChain.ChainPolicy.ExtraStore.Add(redisCaCertificate);
                privateChain.Build(cert2);

                bool isValid = true;

                // we're establishing the trust chain so if the only complaint is that that the root CA is untrusted, and the root CA root
                // matches our certificate, we know it's ok
                foreach (X509ChainStatus chainStatus in privateChain.ChainStatus.Where(x=>x.Status != X509ChainStatusFlags.UntrustedRoot))
                {
                    if (chainStatus.Status != X509ChainStatusFlags.NoError)
                    {
                        isValid = false;
                        break;
                    }
                }

                return isValid;
            },
            TargetHost = endpoint
        };
        // Connect to Redis Cloud
        var redisConfiguration = new ConfigurationOptions
        {
            EndPoints = { endpoint },
            Ssl = true,
            SslHost = sslOptions.TargetHost,
            SslClientAuthenticationOptions = host => sslOptions,
            Password = password
        };


        var redis = ConnectionMultiplexer.Connect(redisConfiguration);
        var db = redis.GetDatabase();
        db.Ping();

        db.StringSet("testKey", "testValue");
        var value = db.StringGet("testKey");
        Assert.Equal("testValue", value);
    }
#endif

    [Fact]
    public void BasicJsonExamplesTest()
    {
        // ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
        // IDatabase db = redis.GetDatabase();
        var db =  redisFixture.Redis.GetDatabase();
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

        // Set and fetch a simple JSON KVP:
        json.Set("ex2:1", "$", "\"val\"");
        var res = json.Get(key: "ex2:1",
            path: "$",
            indent: "\t",
            newLine: "\n"
        );
        Assert.Equal("[\n\t\"val\"\n]", res.ToString());

        // Set and fetch a single property from a JSON object:
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
        Thread.Sleep(2000);

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
        // ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
        // IDatabase db = redis.GetDatabase();
        var db =  redisFixture.Redis.GetDatabase();
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
        Assert.Equal(expected, res.ToString());

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

    [Fact]
    public void BasicQueryOperationsTest()
    {
        // ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
        // IDatabase db = redis.GetDatabase();
        var db =  redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        IJsonCommands json = db.JSON();
        ISearchCommands ft = db.FT();

        json.Set("product:15970", "$", new
        {
            id = 15970,
            gender = "Men",
            season = new[] { "Fall", "Winter" },
            description = "Turtle Check Men Navy Blue Shirt",
            price = 34.95,
            city = "Boston",
            coords = "-71.057083, 42.361145"
        });
        json.Set("product:59263", "$", new
        {
            id = 59263,
            gender = "Women",
            season = new[] { "Fall", "Winter", "Spring", "Summer" },
            description = "Titan Women Silver Watch",
            price = 129.99,
            city = "Dallas",
            coords = "-96.808891, 32.779167"
        });
        json.Set("product:46885", "$", new
        {
            id = 46885,
            gender = "Boys",
            season = new[] { "Fall" },
            description = "Ben 10 Boys Navy Blue Slippers",
            price = 45.99,
            city = "Denver",
            coords = "-104.991531, 39.742043"
        });

        try { ft.DropIndex("idx1"); } catch { };
        ft.Create("idx1", new FTCreateParams().On(IndexDataType.JSON)
                                              .Prefix("product:"),
                                        new Schema().AddNumericField(new FieldName("$.id", "id"))
                                                    .AddTagField(new FieldName("$.gender", "gender"))
                                                    .AddTagField(new FieldName("$.season.*", "season"))
                                                    .AddTextField(new FieldName("$.description", "description"))
                                                    .AddNumericField(new FieldName("$.price", "price"))
                                                    .AddTextField(new FieldName("$.city", "city"))
                                                    .AddGeoField(new FieldName("$.coords", "coords")));

        // sleep:
        Thread.Sleep(2000);

        // Find all documents for a given index:
        var res = ft.Search("idx1", new Query("*")).ToJson();

        Assert.NotNull(res);
        // Assert.Equal(3, res!.Count);
        var expectedList = new List<string>()
        {
            "{\"id\":59263,\"gender\":\"Women\",\"season\":[\"Fall\",\"Winter\",\"Spring\",\"Summer\"],\"description\":\"Titan Women Silver Watch\",\"price\":129.99,\"city\":\"Dallas\",\"coords\":\"-96.808891, 32.779167\"}",
            "{\"id\":15970,\"gender\":\"Men\",\"season\":[\"Fall\",\"Winter\"],\"description\":\"Turtle Check Men Navy Blue Shirt\",\"price\":34.95,\"city\":\"Boston\",\"coords\":\"-71.057083, 42.361145\"}",
            "{\"id\":46885,\"gender\":\"Boys\",\"season\":[\"Fall\"],\"description\":\"Ben 10 Boys Navy Blue Slippers\",\"price\":45.99,\"city\":\"Denver\",\"coords\":\"-104.991531, 39.742043\"}"
        };

        SortAndCompare(expectedList, res);



        // Find all documents with a given word in a text field:
        res = ft.Search("idx1", new Query("@description:Slippers")).ToJson();
        var expected = "{\"id\":46885,\"gender\":\"Boys\",\"season\":[\"Fall\"],\"description\":\"Ben 10 Boys Navy Blue Slippers\",\"price\":45.99,\"city\":\"Denver\",\"coords\":\"-104.991531, 39.742043\"}";
        Assert.Equal(expected, res![0].ToString());


        // Find all documents with a given phrase in a text field:
        res = ft.Search("idx1", new Query("@description:(\"Blue Shirt\")")).ToJson();
        expected = "{\"id\":15970,\"gender\":\"Men\",\"season\":[\"Fall\",\"Winter\"],\"description\":\"Turtle Check Men Navy Blue Shirt\",\"price\":34.95,\"city\":\"Boston\",\"coords\":\"-71.057083, 42.361145\"}";
        Assert.Equal(expected, res![0].ToString());

        // Find all documents with a numeric field in a given range:
        res = ft.Search("idx1", new Query("@price:[40,130]")).ToJson();

        expectedList = new()
        {
            "{\"id\":59263,\"gender\":\"Women\",\"season\":[\"Fall\",\"Winter\",\"Spring\",\"Summer\"],\"description\":\"Titan Women Silver Watch\",\"price\":129.99,\"city\":\"Dallas\",\"coords\":\"-96.808891, 32.779167\"}",
            "{\"id\":46885,\"gender\":\"Boys\",\"season\":[\"Fall\"],\"description\":\"Ben 10 Boys Navy Blue Slippers\",\"price\":45.99,\"city\":\"Denver\",\"coords\":\"-104.991531, 39.742043\"}"
        };

        SortAndCompare(expectedList, res);



        // Find all documents that contain a given value in an array field (tag):
        res = ft.Search("idx1", new Query("@season:{Spring}")).ToJson();
        expected = "{\"id\":59263,\"gender\":\"Women\",\"season\":[\"Fall\",\"Winter\",\"Spring\",\"Summer\"],\"description\":\"Titan Women Silver Watch\",\"price\":129.99,\"city\":\"Dallas\",\"coords\":\"-96.808891, 32.779167\"}";
        Assert.Equal(expected, res[0].ToString());

        // Find all documents contain both a numeric field in a range and a word in a text field:
        res = ft.Search("idx1", new Query("@price:[40, 100] @description:Blue")).ToJson();
        expected = "{\"id\":46885,\"gender\":\"Boys\",\"season\":[\"Fall\"],\"description\":\"Ben 10 Boys Navy Blue Slippers\",\"price\":45.99,\"city\":\"Denver\",\"coords\":\"-104.991531, 39.742043\"}";
        Assert.Equal(expected, res[0].ToString());

        // Find all documents that either match tag value or text value:
        res = ft.Search("idx1", new Query("(@gender:{Women})|(@city:Boston)")).ToJson();
        expectedList = new()
        {
            "{\"id\":59263,\"gender\":\"Women\",\"season\":[\"Fall\",\"Winter\",\"Spring\",\"Summer\"],\"description\":\"Titan Women Silver Watch\",\"price\":129.99,\"city\":\"Dallas\",\"coords\":\"-96.808891, 32.779167\"}",
            "{\"id\":15970,\"gender\":\"Men\",\"season\":[\"Fall\",\"Winter\"],\"description\":\"Turtle Check Men Navy Blue Shirt\",\"price\":34.95,\"city\":\"Boston\",\"coords\":\"-71.057083, 42.361145\"}"
        };

        SortAndCompare(expectedList, res);

        // Find all documents that do not contain a given word in a text field:
        res = ft.Search("idx1", new Query("-(@description:Shirt)")).ToJson();

        expectedList = new()
        {
            "{\"id\":59263,\"gender\":\"Women\",\"season\":[\"Fall\",\"Winter\",\"Spring\",\"Summer\"],\"description\":\"Titan Women Silver Watch\",\"price\":129.99,\"city\":\"Dallas\",\"coords\":\"-96.808891, 32.779167\"}",
            "{\"id\":46885,\"gender\":\"Boys\",\"season\":[\"Fall\"],\"description\":\"Ben 10 Boys Navy Blue Slippers\",\"price\":45.99,\"city\":\"Denver\",\"coords\":\"-104.991531, 39.742043\"}"
        };
        SortAndCompare(expectedList, res);

        // Find all documents that have a word that begins with a given prefix value:
        res = ft.Search("idx1", new Query("@description:Nav*")).ToJson();

        expectedList = new()
        {
            "{\"id\":15970,\"gender\":\"Men\",\"season\":[\"Fall\",\"Winter\"],\"description\":\"Turtle Check Men Navy Blue Shirt\",\"price\":34.95,\"city\":\"Boston\",\"coords\":\"-71.057083, 42.361145\"}",
            "{\"id\":46885,\"gender\":\"Boys\",\"season\":[\"Fall\"],\"description\":\"Ben 10 Boys Navy Blue Slippers\",\"price\":45.99,\"city\":\"Denver\",\"coords\":\"-104.991531, 39.742043\"}"
        };
        SortAndCompare(expectedList, res);

        // Find all documents that contain a word that ends with a given suffix value:
        res = ft.Search("idx1", new Query("@description:*Watch")).ToJson();

        expected = "{\"id\":59263,\"gender\":\"Women\",\"season\":[\"Fall\",\"Winter\",\"Spring\",\"Summer\"],\"description\":\"Titan Women Silver Watch\",\"price\":129.99,\"city\":\"Dallas\",\"coords\":\"-96.808891, 32.779167\"}";
        Assert.Equal(expected, res[0].ToString());

        // Find all documents that contain a word that is within 1 Levenshtein distance of a given word:
        res = ft.Search("idx1", new Query("@description:%wavy%")).ToJson();


        expectedList = new()
        {
            "{\"id\":15970,\"gender\":\"Men\",\"season\":[\"Fall\",\"Winter\"],\"description\":\"Turtle Check Men Navy Blue Shirt\",\"price\":34.95,\"city\":\"Boston\",\"coords\":\"-71.057083, 42.361145\"}",
            "{\"id\":46885,\"gender\":\"Boys\",\"season\":[\"Fall\"],\"description\":\"Ben 10 Boys Navy Blue Slippers\",\"price\":45.99,\"city\":\"Denver\",\"coords\":\"-104.991531, 39.742043\"}"
        };
        SortAndCompare(expectedList, res);

        // Find all documents that have geographic coordinates within a given range of a given coordinate.
        // Colorado Springs coords(long, lat) = -104.800644, 38.846127:
        res = ft.Search("idx1", new Query("@coords:[-104.800644 38.846127 100 mi]")).ToJson();

        expected = "{\"id\":46885,\"gender\":\"Boys\",\"season\":[\"Fall\"],\"description\":\"Ben 10 Boys Navy Blue Slippers\",\"price\":45.99,\"city\":\"Denver\",\"coords\":\"-104.991531, 39.742043\"}";
        Assert.Equal(expected, res[0].ToString());
    }

    [Fact]
    public void AdvancedQueryOperationsTest()
    {
        // ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
        // IDatabase db = redis.GetDatabase();
        var db =  redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        IJsonCommands json = db.JSON();
        ISearchCommands ft = db.FT();

        // Vector Similarity Search (VSS)
        // Data load:
        db.HashSet("vec:1", "vector", (new float[] { 1f, 1f, 1f, 1f }).SelectMany(BitConverter.GetBytes).ToArray());
        db.HashSet("vec:2", "vector", (new float[] { 2f, 2f, 2f, 2f }).SelectMany(BitConverter.GetBytes).ToArray());
        db.HashSet("vec:3", "vector", (new float[] { 3f, 3f, 3f, 3f }).SelectMany(BitConverter.GetBytes).ToArray());
        db.HashSet("vec:5", "vector", (new float[] { 4f, 4f, 4f, 4f }).SelectMany(BitConverter.GetBytes).ToArray());

        // Index creation:
        try { ft.DropIndex("vss_idx"); } catch { };
        Assert.True(ft.Create("vss_idx", new FTCreateParams().On(IndexDataType.HASH).Prefix("vec:"),
            new Schema()
            .AddVectorField("vector", VectorField.VectorAlgo.FLAT,
                new Dictionary<string, object>()
                {
                    ["TYPE"] = "FLOAT32",
                    ["DIM"] = "4",
                    ["DISTANCE_METRIC"] = "L2"
                }
        )));

        // Sleep:
        Thread.Sleep(2000);

        // Search:
        float[] vec = new[] { 2f, 2f, 3f, 3f };
        var res = ft.Search("vss_idx",
                    new Query("*=>[KNN 2 @vector $query_vec]")
                    .AddParam("query_vec", vec.SelectMany(BitConverter.GetBytes).ToArray())
                    .SetSortBy("__vector_score")
                    .Dialect(2));
        HashSet<string> resSet = new HashSet<string>();
        foreach (var doc in res.Documents)
        {
            foreach (var item in doc.GetProperties())
            {
                if (item.Key == "__vector_score")
                {
                    resSet.Add($"id: {doc.Id}, score: {item.Value}");
                }
            }
        }

        HashSet<string> expectedResSet = new HashSet<string>()
        {
            "id: vec:2, score: 2",
            "id: vec:3, score: 2",
        };

        Assert.Equal(expectedResSet, resSet);

        //Advanced Search Queries:
        // data load:
        json.Set("warehouse:1", "$", new
        {
            city = "Boston",
            location = "-71.057083, 42.361145",
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
                    description = "Ben 10 Boys Navy Blue Slippers",
                    price = 45.99
                }
            }
        });
        json.Set("warehouse:2", "$", new
        {
            city = "Dallas",
            location = "-96.808891, 32.779167",
            inventory = new[] {
                new {
                    id = 51919,
                    gender = "Women",
                    season = new[] {"Summer"},
                    description = "Nyk Black Horado Handbag",
                    price = 52.49
                },
                new {
                    id = 4602,
                    gender = "Unisex",
                    season = new[] {"Fall", "Winter"},
                    description = "Wildcraft Red Trailblazer Backpack",
                    price = 50.99
                },
                new {
                    id = 37561,
                    gender = "Girls",
                    season = new[] {"Spring", "Summer"},
                    description = "Madagascar3 Infant Pink Snapsuit Romper",
                    price = 23.95
                }
            }
        });

        // Index creation:
        try { ft.DropIndex("wh_idx"); } catch { };
        Assert.True(ft.Create("wh_idx", new FTCreateParams()
                                .On(IndexDataType.JSON)
                                .Prefix("warehouse:"),
                                new Schema().AddTextField(new FieldName("$.city", "city"))));

        // Sleep:
        Thread.Sleep(2000);

        // Find all inventory ids from all the Boston warehouse that have a price > $50:
        res = ft.Search("wh_idx",
                        new Query("@city:Boston")
                            .ReturnFields(new FieldName("$.inventory[?(@.price>50)].id", "result"))
                            .Dialect(3));

        Assert.Equal("[59263]", res.Documents[0]["result"].ToString());

        // Find all inventory items in Dallas that are for Women or Girls:
        res = ft.Search("wh_idx",
                        new Query("@city:(Dallas)")
                            .ReturnFields(new FieldName("$.inventory[?(@.gender==\"Women\" || @.gender==\"Girls\")]", "result"))
                            .Dialect(3));
        var expected = "[{\"id\":51919,\"gender\":\"Women\",\"season\":[\"Summer\"],\"description\":\"Nyk Black Horado Handbag\",\"price\":52.49},{\"id\":37561,\"gender\":\"Girls\",\"season\":[\"Spring\",\"Summer\"],\"description\":\"Madagascar3 Infant Pink Snapsuit Romper\",\"price\":23.95}]";
        Assert.Equal(expected, res.Documents[0]["result"].ToString());

        // Aggregation
        // Data load:
        json.Set("book:1", "$", new
        {
            title = "System Design Interview",
            year = 2020,
            price = 35.99
        });
        json.Set("book:2", "$", new
        {
            title = "The Age of AI: And Our Human Future",
            year = 2021,
            price = 13.99
        });
        json.Set("book:3", "$", new
        {
            title = "The Art of Doing Science and Engineering: Learning to Learn",
            year = 2020,
            price = 20.99
        });
        json.Set("book:4", "$", new
        {
            title = "Superintelligence: Path, Dangers, Stategies",
            year = 2016,
            price = 14.36
        });

        Assert.True(ft.Create("book_idx", new FTCreateParams()
                        .On(IndexDataType.JSON)
                        .Prefix("book:"),
                        new Schema().AddTextField(new FieldName("$.title", "title"))
                            .AddNumericField(new FieldName("$.year", "year"))
                            .AddNumericField(new FieldName("$.price", "price"))));
        // sleep:
        Thread.Sleep(2000);

        // Find the total number of books per year:
        var request = new AggregationRequest("*").GroupBy("@year", Reducers.Count().As("count"));
        var result = ft.Aggregate("book_idx", request);

        resSet.Clear();
        for (var i = 0; i < result.TotalResults; i++)
        {
            var row = result.GetRow(i);
            resSet.Add($"{row["year"]}: {row["count"]}");
        }
        expectedResSet.Clear();
        expectedResSet.Add("2016: 1");
        expectedResSet.Add("2020: 2");
        expectedResSet.Add("2021: 1");

        Assert.Equal(expectedResSet, resSet);

        // Sum of inventory dollar value by year:
        request = new AggregationRequest("*").GroupBy("@year", Reducers.Sum("@price").As("sum"));
        result = ft.Aggregate("book_idx", request);

        resSet.Clear();
        for (var i = 0; i < result.TotalResults; i++)
        {
            var row = result.GetRow(i);
            resSet.Add($"{row["year"]}: {row["sum"]}");
        }
        expectedResSet.Clear();
        expectedResSet.Add("2016: 14.36");
        expectedResSet.Add("2020: 56.98");
        expectedResSet.Add("2021: 13.99");

        Assert.Equal(expectedResSet, resSet);
    }

    private static void SortAndCompare(List<string> expectedList, List<string> res)
    {
        res.Sort();
        expectedList.Sort();

        for (int i = 0; i < res.Count; i++)
        {
            Assert.Equal(expectedList[i], res[i].ToString());
        }
    }
}