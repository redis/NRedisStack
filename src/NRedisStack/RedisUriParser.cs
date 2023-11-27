using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using StackExchange.Redis;

[assembly: InternalsVisibleTo("NRedisStack.Tests")]

namespace NRedisStack
{
    /// <summary>
    /// URI parsing utility.
    /// </summary>
    internal static class RedisUriParser
    {
        /// <summary>
        /// Parses a Config options for StackExchange Redis from the URI.
        /// </summary>
        /// <param name="redisUri">Redis Uri string</param>
        /// <returns>A configuration options result for SE.Redis.</returns>
        internal static ConfigurationOptions FromUri(string redisUri)
        {
            var options = new ConfigurationOptions();

            if (string.IsNullOrEmpty(redisUri))
            {
                options.EndPoints.Add("localhost:6379");
                return options;
            }

            var uri = new Uri(redisUri);
            ParseHost(options, uri);
            ParseUserInfo(options, uri);
            ParseQueryArguments(options, uri);
            ParseDefaultDatabase(options, uri);
            options.Ssl = uri.Scheme == "rediss";
            options.AbortOnConnectFail = false;
            return options;
        }

        private static void ParseDefaultDatabase(ConfigurationOptions options, Uri uri)
        {
            if (string.IsNullOrEmpty(uri.AbsolutePath))
            {
                return;
            }

            var dbNumStr = Regex.Match(uri.AbsolutePath, "[0-9]+").Value;
            int dbNum;
            if (int.TryParse(dbNumStr, out dbNum))
            {
                options.DefaultDatabase = dbNum;
            }
        }

        private static IList<KeyValuePair<string, string>> ParseQuery(string query) =>
            query.Split('&').Select(x =>
                new KeyValuePair<string, string>(x.Split('=').First(), x.Split('=').Last())).ToList();

        private static void ParseUserInfo(ConfigurationOptions options, Uri uri)
        {
            if (!string.IsNullOrEmpty(uri.UserInfo))
            {
                var userInfo = uri.UserInfo.Split(':');
                if (userInfo.Length > 1)
                {
                    options.User = Uri.UnescapeDataString(userInfo[0]);
                    options.Password = Uri.UnescapeDataString(userInfo[1]);
                }
                else
                {
                    throw new FormatException(
                        "Username and password must be in the form username:password - if there is no username use the format :password");
                }
            }
        }

        private static void ParseHost(ConfigurationOptions options, Uri uri)
        {
            var port = uri.Port >= 0 ? uri.Port : 6379;
            var host = !string.IsNullOrEmpty(uri.Host) ? uri.Host : "localhost";
            options.EndPoints.Add($"{host}:{port}");
        }

        private static void ParseQueryArguments(ConfigurationOptions options, Uri uri)
        {
            if (string.IsNullOrEmpty(uri.Query))
            {
                return;
            }

            var queryArgs = ParseQuery(uri.Query.Substring(1));

            var actions = new Dictionary<string, Action<string>>(StringComparer.OrdinalIgnoreCase)
            {
                { "timeout", value => SetTimeoutOptions(options, value) },
                { "clientname", value => options.ClientName = value },
                { "sentinel_primary_name", value => options.ServiceName = value },
                { "endpoint", value => options.EndPoints.Add(value) },
                { "allowadmin", value => options.AllowAdmin = bool.Parse(value) },
                { "abortConnect", value => options.AbortOnConnectFail = bool.Parse(value) },
                { "asynctimeout", value => options.AsyncTimeout = int.Parse(value) },
                { "retry", value => options.ConnectRetry = int.Parse(value)}
                // TODO: add more options
            };

            foreach (var arg in queryArgs.Where(arg => actions.ContainsKey(arg.Key)))
            {
                actions[arg.Key](arg.Value);
            }
        }

        private static void SetTimeoutOptions(ConfigurationOptions options, string value)
        {
            var timeout = int.Parse(value);
            options.AsyncTimeout = timeout;
            options.SyncTimeout = timeout;
            options.ConnectTimeout = timeout;
        }

    }
}
