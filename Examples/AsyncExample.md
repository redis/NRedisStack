
# Async Example

## All methods have sync and async implementations. The async methods end with the suffix Async(...), and are fully await-able. See the example below:

Connect to a Redis server, and retrieve an instance that can run JSON commands:

```csharp
var redis = await ConnectionMultiplexer.ConnectAsync("localhost");
var db = redis.GetDatabase();
var json = db.JSON();
```

Store and retrieve data, async:

```csharp
await json.SetAsync("key", "$", new { name = "John", age = 30, city = "New York" });
var john = await json.GetAsync("key");
```
