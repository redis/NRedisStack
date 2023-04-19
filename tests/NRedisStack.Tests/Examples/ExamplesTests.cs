using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Moq;
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

}