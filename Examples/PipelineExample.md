# Pipeline
## An example of pipelines Redis Stack Redis commands (JSON.SET & JSON.CLEAR & JSON.GET)

Connect to the Redis server and Setup new Pipeline
```csharp
IDatabase db = redisFixture.Redis.GetDatabase();
var pipeline = new Pipeline(db);
```


Add JSON data to pipeline
```csharp
pipeline.Json.SetAsync("person", "$", new { name = "John", age = 30, city = "New York", nicknames = new[] { "John", "Johny", "Jo" } });
```

Increase age by 2
```csharp
pipeline.Json.NumIncrbyAsync("person", "$.age", 2);
```

Remove the ```nicknames``` field from the JSON object
```csharp
pipeline.Json.ClearAsync("person", "$.nicknames");
```

Delete the nicknames
```csharp
pipeline.Json.DelAsync("person", "$.nicknames");
```

Retrieve the JSON response
```csharp
var getResponse = pipeline.Json.GetAsync("person");
```

Execute pipeline
```csharp
pipeline.Execute();
```

Access the result of the JSON response
```csharp
var result = getResponse.Result;
```
now result is:
```json
{
  "name": "John",
  "age": 32,
  "city": "New York"
}
```
