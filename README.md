[![license](https://img.shields.io/github/license/redis/NRedisStack.svg)](https://raw.githubusercontent.com/redis/NRedisStack/master/LICENSE)
[![.github/workflows/integration.yml](https://github.com/redis/NRedisStack/actions/workflows/integration.yml/badge.svg)](https://github.com/redis/NRedisStack/actions/workflows/integration.yml)
[![codecov](https://codecov.io/gh/redis/NRedisStack/branch/master/graph/badge.svg?token=4B0KCNK12D)](https://codecov.io/gh/redis/NRedisStack)
[![NRedisStack NuGet release](https://img.shields.io/nuget/v/NRedisStack.svg?label=nuget&logo=nuget)](https://www.nuget.org/packages/NRedisStack)


# NRedisStack

.NET Client for Redis

## Note

This project builds on [StackExchange.Redis](https://github.com/StackExchange/StackExchange.Redis), and seeks to bring native support for Redis Stack commands to the C# ecosystem.

## How do I Redis?

[Learn for free at Redis University](https://university.redis.com/)

[Build faster with the Redis Launchpad](https://launchpad.redis.com/)

[Try the Redis Cloud](https://redis.com/try-free/)

[Dive in developer tutorials](https://developer.redis.com/)

[Join the Redis community](https://redis.com/community/)

[Work at Redis](https://redis.com/company/careers/jobs/)

## API

The complete documentation for Redis  module commands can be found at the [Redis commands website](https://redis.io/commands/).

### Redis OSS commands

You can use Redis OSS commands in the same way as you use them in [StackExchange.Redis](https://github.com/StackExchange/StackExchange.Redis).

### Stack commands

Each module has a command class with its own commands.

The supported modules are [Search](https://redis.io/commands/?group=search), [JSON](https://redis.io/commands/?group=json), [TimeSeries](https://redis.io/commands/?group=timeseries), [Bloom Filter](https://redis.io/commands/?group=bf), [Cuckoo Filter](https://redis.io/commands/?group=cf), [T-Digest](https://redis.io/commands/?group=tdigest), [Count-min Sketch](https://redis.io/commands/?group=cms), and [Top-K](https://redis.io/commands/?group=topk).

**Note:** RedisGraph support has been deprecated starting from Redis Stack version 7.2. For more information, please refer to [this blog post](https://redis.com/blog/redisgraph-eol/).

# Usage

## 💻 Installation

Using the dotnet cli, run:

```text
dotnet add package NRedisStack
```

## 🏁 Getting started

### Supported Redis versions

The most recent version of this library supports Redis version [6.2](https://github.com/redis/redis/blob/6.2/00-RELEASENOTES), [7.2](https://github.com/redis/redis/blob/7.2/00-RELEASENOTES).

### Starting Redis

Before writing any code, you'll need a Redis instance with the appropriate Redis modules. The quickest way to get this is with Docker:

```sh
docker run -p 6379:6379 --name redis-stack redis/redis-stack:latest
```

This launches [Redis Stack](https://redis.io/docs/stack/), an extension of Redis that adds modern data structures to Redis.

Now, you need to connect to Redis, exactly the same way you do it in [StackExchange.Redis](https://github.com/StackExchange/StackExchange.Redis):
```csharp
using NRedisStack;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
//...
ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
IDatabase db = redis.GetDatabase();
```
Now you can create a variable from any type of module in the following way:
```csharp
BloomCommands bf = db.BF();
CuckooCommands cf = db.CF();
CmsCommands cms = db.CMS();
TopKCommands topk = db.TOPK();
TdigestCommands tdigest = db.TDIGEST();
SearchCommands ft = db.FT();
JsonCommands json = db.JSON();
TimeSeriesCommands ts = db.TS();
GraphCommands graph = db.GRAPH(); // If Redis version is less than 7.2
```
Then, that variable will allow you to call all the commands of that module.

## Examples

### Store a JSON object in Redis

To store a json object in Redis:

```csharp
ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
IDatabase db = redis.GetDatabase();

JsonCommands json = db.JSON();
var key = "myKey";
json.Set(key, "$", new { Age = 35, Name = "Alice" });
```

### Index and search
Now, to execute a search  for objects, we need to index them on the server, and run a query:

Setup:

```csharp
using NRedisStack.Search;
using NRedisStack.Search.Literals.Enums;
//...
ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
IDatabase db = redis.GetDatabase();

SearchCommands ft = db.FT();
JsonCommands json = db.JSON();
```

Create an index with fields and weights:
```csharp
// FT.CREATE myIdx ON HASH PREFIX 1 doc: SCHEMA title TEXT WEIGHT 5.0 body TEXT url TEXT
ft.Create("myIndex", new FTCreateParams().On(IndexDataType.HASH)
                                         .Prefix("doc:"),
                     new Schema().AddTextField("title", 5.0)
                                 .AddTextField("body")
                                 .AddTextField("url"));
```

After creating the index, future documents with the ```doc:``` prefix will be automatically indexed when created or modified.

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

More examples can be found in the [examples folder](Examples).

------

### Author

NRedisStack is developed and maintained by [Redis Inc](https://redis.com). It can be found [here](
https://github.com/redis/NRedisStack), or downloaded from [NuGet](https://www.nuget.org/packages/NRedisStack).

---

[![Redis](./docs/logo-redis.png)](https://www.redis.com)
