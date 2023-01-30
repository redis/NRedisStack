# Pipeline 
## An example of pipelines Redis Stack Redis commands (JSON.SET & JSON.CLEAR & JSON.GET)

### Connect to the Redis server:
```csharp
var redis = ConnectionMultiplexer.Connect("localhost");
```

### Setup pipeline connection
```csharp
var pipeline = new Pipeline(ConnectionMultiplexer.Connect("localhost"));
```

### Add JsonSet to pipeline
```csharp
pipeline.Json.SetAsync("person", "$", new { name = "John", age = 30, city = "New York", nicknames = new[] { "John", "Johny", "Jo" } });
```

### Inc age by 2 
```csharp
pipeline.Json.NumIncrbyAsync("person", "$.age", 2);
```

### Clear the nicknames from the Json
```csharp
pipeline.Json.ClearAsync("person", "$.nicknames");
```

### Del the nicknames
```csharp
pipeline.Json.DelAsync("person", "$.nicknames");
```

### Get the Json response
```csharp
var getResponse = pipeline.Json.GetAsync("person");
```

### Execute the pipeline
```csharp
pipeline.Execute();
```