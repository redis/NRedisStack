[![license](https://img.shields.io/github/license/redis/NRedisStack.svg)](https://raw.githubusercontent.com/redis/NRedisStack/master/LICENSE)
[![.github/workflows/integration.yml](https://github.com/redis/NRedisStack/actions/workflows/integration.yml/badge.svg)](https://github.com/redis/NRedisStack/actions/workflows/integration.yml)
[![pre-release](https://img.shields.io/github/v/release/redis/nredisstack?include_prereleases&label=prerelease)](https://github.com/redis/nredisstack/releases)
[![codecov](https://codecov.io/gh/redis/NRedisStack/branch/master/graph/badge.svg?token=4B0KCNK12D)](https://codecov.io/gh/redis/NRedisStack)
[![NRedisStack NuGet release](https://img.shields.io/nuget/v/NRedisStack.svg?label=nuget&logo=nuget)](https://www.nuget.org/packages/NRedisStack)



# NRedisStack

.NET Client for Redis

## Note

This project builds on [StackExchange.Redis](https://github.com/StackExchange/StackExchange.Redis), and seeks to bring native support for Redis Stack commands to the C# ecosystem.

## API

The complete documentation for Redis  module commands can be found at the [Redis commands website](https://redis.io/commands/).

### Redis OSS commands
You can use Redis OSS commands in the same way as you use them in [StackExchange.Redis](https://github.com/StackExchange/StackExchange.Redis).

### Stack commands
Each module has a command class with its own commands.
The supported modules are [Search](https://redis.io/commands/?group=search), [JSON](https://redis.io/commands/?group=json), [Graph](https://redis.io/commands/?group=graph), [TimeSeries](https://redis.io/commands/?group=timeseries), [Bloom Filter](https://redis.io/commands/?group=bf), [Cuckoo Filter](https://redis.io/commands/?group=cf), [T-Digest](https://redis.io/commands/?group=tdigest), [Count-min Sketch](https://redis.io/commands/?group=cms), and [Top-K](https://redis.io/commands/?group=topk).

# Usage

## 💻 Installation

Using the dotnet cli, run:

```text
dotnet add package NRedisStack
```

## 🏁 Getting started

### Starting Redis

Before writing any code, you'll need a Redis instance with the appropriate Redis modules. The quickest way to get this is with Docker:

```sh
docker run -p 6379:6379 --name redis-stack redis/redis-stack:latest
```

This launches [Redis Stack](https://redis.io/docs/stack/), an extension of Redis that adds modern data structures to Redis.

Now, you need to connect to Redis, exactly the same way you do it in [StackExchange.Redis](https://github.com/StackExchange/StackExchange.Redis):
```csharp
using NRedisStack;
...
ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
```
Now you can create a variable from any type of module in the following way:
```csharp
IBloomCommands bf = db.BF();
ICuckooCommands cf = db.CF();
ICmsCommands cms = db.CMS();
IGraphCommands graph = db.GRAPH();
ITopKCommands topk = db.TOPK();
ITdigestCommands tdigest = db.TDIGEST();
ISearchCommands ft = db.FT();
IJsonCommands json = db.JSON();
ITimeSeriesCommands ts = db.TS();
```
Then, that variable will allow you to call all the commands of that module.
## Examples
### Set JSON object to Redis
Set a json object to Redis:
```csharp
ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
IDatabase db = redis.GetDatabase();

IJsonCommands json = db.JSON();
var key = "myKey";
json.Set(key, "$", new Person() { Age = 35, Name = "Alice" });
```
### Index and search
We will see an example that shows how you can create an index, add a document to it and search it using NRedisStack.

Setup:
```csharp
using NRedisStack;
...
IDatabase db = redisFixture.Redis.GetDatabase();
ISearchCommands ft = db.FT();
IJsonCommands json = db.JSON();
```
Create an index with fields and weights:
```csharp
// FT.CREATE myIdx ON HASH PREFIX 1 doc: SCHEMA title TEXT WEIGHT 5.0 body TEXT url TEXT
ft.Create("myIndex", new FTCreateParams().On(IndexDataType.Hash)
                                         .Prefix("doc:"),
                     new Schema().AddTextField("title", 5.0)
                                 .AddTextField("body")
                                 .AddTextField("url"));
```

After you create the index, any new hash documents with the doc: prefix are automatically indexed upon creation.


To create a new hash document and add it to the index, use the HSET command:
```csharp
// HSET doc:1 title "hello world" body "lorem ipsum" url "http://redis.io"
db.HashSet("doc:1", new HashEntry[] { new("title", "hello world"),
                                      new("body", "lorem ipsum"),
                                      new("url", "http://redis.io") });
```
Search the index for documents that contain "hello world":
```csharp
// FT.SEARCH myIndex "hello world" LIMIT 0 10
ft.Search("myIndex", new Query("hello world").Limit(0, 10));
```
Drop the index:
```csharp
// FT.DROPINDEX myIndex
ft.DropIndex("myIndex");
```
------

### Author

NRedisStack is developed and maintained by [Redis Inc](https://redis.com). It can be found [here](
https://github.com/redis/NRedisStack), or downloaded from [NuGet](https://www.nuget.org/packages/NRedisStack).

---

[![Redis](./docs/logo-redis.png)](https://www.redis.com)
