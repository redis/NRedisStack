[![license](https://img.shields.io/github/license/redis/NRedisStack.svg)](https://raw.githubusercontent.com/redis/NRedisStack/master/LICENSE)
[![.github/workflows/integration.yml](https://github.com/redis/NRedisStack/actions/workflows/integration.yml/badge.svg)](https://github.com/redis/NRedisStack/actions/workflows/integration.yml)
[![codecov](https://codecov.io/gh/redis/NRedisStack/branch/master/graph/badge.svg?token=4B0KCNK12D)](https://codecov.io/gh/redis/NRedisStack)
[![NRedisStack NuGet release](https://img.shields.io/nuget/v/NRedisStack.svg?label=nuget&logo=nuget)](https://www.nuget.org/packages/NRedisStack)

# NRedisStack

NRedisStack is the .NET client for the Redis module commands: [Search](https://redis.io/commands/?group=search),
[JSON](https://redis.io/commands/?group=json), [TimeSeries](https://redis.io/commands/?group=timeseries),
[Bloom filter](https://redis.io/commands/?group=bf), [Cuckoo filter](https://redis.io/commands/?group=cf),
[T-Digest](https://redis.io/commands/?group=tdigest), [Count-min Sketch](https://redis.io/commands/?group=cms) and
[Top-K](https://redis.io/commands/?group=topk). It builds on
[StackExchange.Redis](https://github.com/seredis/StackExchange.Redis), which provides the connection and the core
Redis commands; NRedisStack adds the module commands on top. It is developed and maintained by
[Redis Inc](https://redis.io).

## Installation

```text
dotnet add package NRedisStack
```

This also brings in StackExchange.Redis. You need a Redis server that has the modules; Redis 8 includes them:

```sh
docker run -p 6379:6379 --name redis redis:latest
```

## Quick start

Connecting is StackExchange.Redis' job, so every kind of connection it supports works here, and the module
commands hang off the `IDatabase` it gives you.

### Standalone

```csharp
using NRedisStack;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;

ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
IDatabase db = redis.GetDatabase();

JsonCommands json = db.JSON();
json.Set("user:1", "$", new { Age = 35, Name = "Alice" });
```

Each module has an accessor:

```csharp
BloomCommands bf = db.BF();
CuckooCommands cf = db.CF();
CmsCommands cms = db.CMS();
TopKCommands topk = db.TOPK();
TdigestCommands tdigest = db.TDIGEST();
SearchCommands ft = db.FT();
JsonCommands json = db.JSON();
TimeSeriesCommands ts = db.TS();
```

Every command has an async twin on the same object, for example `await json.SetAsync(...)`. Core Redis commands
are used exactly as in StackExchange.Redis, for example `db.StringSet("key", "value")`.

### Cluster

List one or more cluster nodes; the rest are discovered, and commands are routed by key:

```csharp
ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("127.0.0.1:16379,127.0.0.1:16380");
IDatabase db = redis.GetDatabase();
SearchCommands ft = db.FT();
```

### Sentinel

Name the sentinel endpoints and the service; the connection follows the current primary:

```csharp
ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost:26379,serviceName=mymaster");
IDatabase db = redis.GetDatabase();
```

### Pub/sub

NRedisStack adds nothing to pub/sub. Use the StackExchange.Redis subscriber on the same multiplexer:

```csharp
ISubscriber sub = redis.GetSubscriber();
sub.Subscribe(RedisChannel.Literal("news"), (channel, message) => Console.WriteLine(message));
```

### Pipelines and transactions

`Pipeline` and `Transaction` expose the async module commands over a StackExchange.Redis batch or transaction:

```csharp
var pipeline = new Pipeline(db);
_ = pipeline.Json.SetAsync("user:2", "$", new { Age = 28, Name = "Bob" });
_ = pipeline.Json.SetAsync("user:3", "$", new { Age = 41, Name = "Carol" });
pipeline.Execute();
```

## Index and search

To search for documents, create an index and run a query:

```csharp
using NRedisStack.Search;
using NRedisStack.Search.Literals.Enums;

SearchCommands ft = db.FT();

// FT.CREATE myIndex ON HASH PREFIX 1 doc: SCHEMA title TEXT WEIGHT 5.0 body TEXT url TEXT
ft.Create("myIndex", new FTCreateParams().On(IndexDataType.HASH)
                                         .Prefix("doc:"),
                     new Schema().AddTextField("title", 5.0)
                                 .AddTextField("body")
                                 .AddTextField("url"));

// Documents with the doc: prefix are indexed automatically when created or modified.
// HSET doc:1 title "hello world" body "lorem ipsum" url "http://redis.io"
db.HashSet("doc:1", new HashEntry[] { new("title", "hello world"),
                                      new("body", "lorem ipsum"),
                                      new("url", "http://redis.io") });

// FT.SEARCH myIndex "hello world" LIMIT 0 10
ft.Search("myIndex", new Query("hello world").Limit(0, 10));

// FT.DROPINDEX myIndex
ft.DropIndex("myIndex");
```

## Search dialect

By default the client sends `DIALECT 2` with commands such as `FT.SEARCH` and `FT.AGGREGATE`, overriding the
[server-side default dialect](https://redis.io/docs/latest/develop/interact/search-and-query/advanced-concepts/dialects/).
The dialect can change the results a query returns. To use a different one, pass it to the accessor, for example
`db.FT(3)`.

## More examples

The [Examples folder](https://github.com/redis/NRedisStack/tree/master/Examples) has longer walk-throughs: JSON,
query operations, pipelines, transactions and combining modules.

## Documentation

- [NRedisStack guide on redis.io](https://redis.io/docs/latest/develop/clients/dotnet/)
- [Redis command reference](https://redis.io/commands/)
- [StackExchange.Redis documentation](https://seredis.dev/), for connections, configuration, cluster, sentinel
  and pub/sub
- There is no hosted API reference; the XML documentation ships in the package and shows in your IDE.

New to Redis? [Redis University](https://university.redis.io/academy/) is free, the
[developer tutorials](https://redis.io/learn/) cover common use cases, and
[Redis Cloud](https://redis.io/try-free/) has a free tier.

## Getting help and contributing

- Questions and ideas: [GitHub Discussions](https://github.com/redis/NRedisStack/discussions), or the
  [Redis Discord](https://discord.gg/redis). Discussions is the support channel maintainers watch.
- Bugs and feature requests: [GitHub Issues](https://github.com/redis/NRedisStack/issues).
- Security vulnerabilities: see the [security policy](https://github.com/redis/NRedisStack/blob/master/SECURITY.md).
  Do not open a public issue.
- To contribute a change, read [CONTRIBUTING.md](https://github.com/redis/NRedisStack/blob/master/CONTRIBUTING.md).

## Supported versions

- **Redis.** Each release is tested against the Redis versions in the
  [CI matrix](https://github.com/redis/NRedisStack/blob/master/.github/workflows/integration.yml): the current
  stable release and the long-term-support releases on every pull request, and a wider set nightly. A command
  that an older server does not have fails with that server's error.
- **.NET.** The package targets .NET Standard 2.0, so it runs on .NET Framework and on every modern .NET. It
  also has builds for the .NET versions listed in
  [NRedisStack.csproj](https://github.com/redis/NRedisStack/blob/master/src/NRedisStack/NRedisStack.csproj).

## License

NRedisStack is released under the [MIT license](https://github.com/redis/NRedisStack/blob/master/LICENSE).
