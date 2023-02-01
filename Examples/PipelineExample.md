# Pipeline 
## An example of pipelines Redis Stack Redis commands (JSON.SET & JSON.CLEAR & JSON.GET)

Connect to the Redis server and Setup 2 Pipelines

Pipeline can get IDatabase for pipeline1
```csharp
IDatabase db = redisFixture.Redis.GetDatabase();
var pipeline1 = new Pipeline(db);
```

Pipeline can get IConnectionMultiplexer for pipeline2
```csharp
var redis = ConnectionMultiplexer.Connect("localhost");
var pipeline2 = new Pipeline(redis);
```

Add JsonSet to pipeline
```csharp
pipeline1.Json.SetAsync("person", "$", new { name = "John", age = 30, city = "New York", nicknames = new[] { "John", "Johny", "Jo" } });
```

Inc age by 2 
```csharp
pipeline1.Json.NumIncrbyAsync("person", "$.age", 2);
```

Execute the pipeline1
```csharp
pipeline1.Execute();
```

Clear the nicknames from the Json
```csharp
pipeline2.Json.ClearAsync("person", "$.nicknames");
```

Del the nicknames
```csharp
pipeline2.Json.DelAsync("person", "$.nicknames");
```

Get the Json response
```csharp
var getResponse = pipeline2.Json.GetAsync("person");
```

Execute the pipeline2
```csharp
pipeline2.Execute();
```

get the result back JSON
```csharp
var result = getResponse.Result;
```