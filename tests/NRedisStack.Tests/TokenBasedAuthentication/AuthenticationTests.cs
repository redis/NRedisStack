using Xunit;
using StackExchange.Redis;
using Azure.Identity;
using NRedisStack.RedisStackCommands;
using NRedisStack.Search;

namespace NRedisStack.Tests.TokenBasedAuthentication
{
    public class AuthenticationTests 
    {
        static readonly string key = "myKey";
        static readonly string value = "myValue";
        static readonly string index = "myIndex";
        static readonly string field = "myField";
        static readonly string alias = "myAlias";

        string env0Host = "localhost:6379";

        [Fact]
        public void TestTokenBasedAuthentication()
        {

            // NOTE: ConnectionMultiplexer instances should be as long-lived as possible. Ideally a single ConnectionMultiplexer per cache is reused over the lifetime of the client application process.
            ConnectionMultiplexer? connectionMultiplexer = null;
            StringWriter connectionLog = new();

            var configurationOptions = ConfigurationOptions.Parse($"{env0Host}").ConfigureForAzureWithTokenCredentialAsync(new DefaultAzureCredential()).Result!;
            configurationOptions.Ssl = false;
            configurationOptions.AbortOnConnectFail = true; // Fail fast for the purposes of this sample. In production code, this should remain false to retry connections on startup

            connectionMultiplexer = ConnectionMultiplexer.ConnectAsync(configurationOptions, connectionLog).Result;

            IDatabase db = connectionMultiplexer.GetDatabase();

            db.KeyDelete(key);
            try
            {
                db.FT().DropIndex(index);
            }
            catch { }

            db.StringSet(key, value);
            string result = db.StringGet(key);
            Assert.Equal(value, result);

            var ft = db.FT();
            Schema sc = new Schema().AddTextField(field);
            Assert.True(ft.Create(index, FTCreateParams.CreateParams(), sc));

            db.HashSet(index, new HashEntry[] { new HashEntry(field, value) });

            Assert.True(ft.AliasAdd(alias, index));
            SearchResult res1 = ft.Search(alias, new Query("*").ReturnFields(field));
            Assert.Equal(1, res1.TotalResults);
            Assert.Equal(value, res1.Documents[0][field]);

        }
    }
}