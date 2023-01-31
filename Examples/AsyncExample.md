
# Async Example

## All methods have synchronous & asynchronous implementation. the asynchronous methods all end ...Async(...), and are fully await-able. here is an example of using the async methods:

Connect to the Redis server and get a reference to the database and for JSON commands:

```csharp
var redis = await ConnectionMultiplexer.ConnectAsync("localhost");
var db = redis.GetDatabase();
var json = db.JSON();
```

call async version of JSON.SET/GET

```csharp
await json.SetAsync("key", "$", new { name = "John", age = 30, city = "New York" });
var john = await json.GetAsync("key");
```
