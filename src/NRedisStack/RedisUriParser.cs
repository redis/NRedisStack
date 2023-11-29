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
        internal static string defaultHost = "localhost";
        internal static int defaultPort = 6379;

        // The options:
        internal const string
            Timeout = "timeout",
            ClientName = "clientname",
            Sentinel_primary_name = "sentinel_primary_name",
            Endpoint = "endpoint",
            AllowAdmin = "allowadmin",
            AbortConnect = "abortconnect",
            AsyncTimeout = "asynctimeout",
            Retry = "retry",
            Protocol = "protocol";

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
                options.EndPoints.Add($"{defaultHost}:{defaultPort}");
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
            if (string.IsNullOrEmpty(uri.UserInfo))
            {
                return;
            }

            var userInfo = uri.UserInfo.Split(':');

            if (userInfo.Length > 1)
            {
                options.User = Uri.UnescapeDataString(userInfo[0]);
                options.Password = Uri.UnescapeDataString(userInfo[1]);
            }

            else
            {
                throw new FormatException("Username and password must be in the form username:password - if there is no username use the format :password");
            }
        }


        private static void ParseHost(ConfigurationOptions options, Uri uri)
        {
            var port = uri.Port >= 0 ? uri.Port : defaultPort;
            var host = !string.IsNullOrEmpty(uri.Host) ? uri.Host : defaultHost;
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
                { Timeout, value => SetTimeoutOptions(options, value) },
                { ClientName, value => options.ClientName = value },
                { Sentinel_primary_name, value => options.ServiceName = value },
                { Endpoint, value => options.EndPoints.Add(value) },
                { AllowAdmin, value => options.AllowAdmin = bool.Parse(value) },
                { AbortConnect, value => options.AbortOnConnectFail = bool.Parse(value) },
                { AsyncTimeout, value => options.AsyncTimeout = int.Parse(value) },
                { Retry, value => options.ConnectRetry = int.Parse(value) },
                { Protocol, value => ParseRedisProtocol(options, value) }
                // TODO: add more options
            };

            foreach (var arg in queryArgs.Where(arg => actions.ContainsKey(arg.Key)))
            {
                actions[arg.Key.ToLower()](arg.Value);
            }
        }

        private static void ParseRedisProtocol(ConfigurationOptions options, string value)
        {
            switch (value)
            {
                case "2":
                    options.Protocol = RedisProtocol.Resp2;
                    break;
                case "3":
                    options.Protocol = RedisProtocol.Resp3;;
                    break;
                default:
                    throw new FormatException("Invalid protocol specified");
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
