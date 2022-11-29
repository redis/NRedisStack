[![license](https://img.shields.io/github/license/redis/NRedisStack.svg)](https://raw.githubusercontent.com/redis/NRedisStack/master/LICENSE)
[![.github/workflows/integration.yml](https://github.com/redis/NRedisStack/actions/workflows/integration.yml/badge.svg)](https://github.com/redis/NRedisStack/actions/workflows/integration.yml)
[![pre-release](https://img.shields.io/github/v/release/redis/nredisstack?include_prereleases&label=prerelease)](https://github.com/redis/nredisstack/releases)


# NRedisStack

.NET Client for Redis

## Note

This project builds on [StackExchange.Redis](https://github.com/StackExchange/StackExchange.Redis), and seeks to bring native support for Redis Stack commands to the C# ecosystem.

## API

The complete documentation for Redis  module commands can be found at the [Redis commands website](https://redis.io/commands/).

### Redis OSS commands
You can use Redis OSS commands in the same way as you use them in [StackExchange.Redis](https://github.com/StackExchange/StackExchange.Redis).

### Stack Commands
Each module has a command class with its own commands.
The supported modules are: [Search](https://redis.io/commands/?group=search), [Json](https://redis.io/commands/?group=json), [Graph](https://redis.io/commands/?group=graph), [TimeSeries](https://redis.io/commands/?group=timeseries), [Bloom Filter](https://redis.io/commands/?group=bf), [Cuckoo Filter](https://redis.io/commands/?group=cf), [T-Digest](https://redis.io/commands/?group=tdigest), [Count-min Sketch](https://redis.io/commands/?group=cms) and [Top-K](https://redis.io/commands/?group=topk).

# Usage

First, you need to connect to Redis, exactly the same way you do it in [StackExchange.Redis](https://github.com/StackExchange/StackExchange.Redis):
```csharp
using StackExchange.Redis;
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
## Basic Example
Set a json object to Redis:
```csharp
ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
IDatabase db = redis.GetDatabase();

IJsonCommands json = db.JSON();
var key = "myKey";
json.Set(key, "$", new Person() { Age = 35, Name = "Alice" });
```

------

### Author

NRedisStack is developed and maintained by [Redis Inc](https://redis.com). It can be found [here](
https://github.com/redis/NRedisStack), or downloaded from [NuGet](https://www.nuget.org/packages/NRedisStack).

[![Redis](./docs/logo-redis.png)](https://www.redis.com)

