# Combination modules Pipeline 
## An example of pipelines mixing a pipeline with a combination of module commands with JSON & Search

Connect to the Redis server:
```csharp
var redis = ConnectionMultiplexer.Connect("localhost");
```

Setup pipeline connection
```csharp
var pipeline = new Pipeline(ConnectionMultiplexer.Connect("localhost"));
```

## JSON
Add JsonSet to pipeline
```csharp
pipeline.Json.SetAsync("person:01", "$", new { name = "John", age = 30, city = "New York" });
pipeline.Json.SetAsync("person:02", "$", new { name = "Joy", age = 25, city = "Los Angeles" });
pipeline.Json.SetAsync("person:03", "$", new { name = "Mark", age = 21, city = "Chicago" });
pipeline.Json.SetAsync("person:04", "$", new { name = "Steve", age = 24, city = "Phoenix" });
pipeline.Json.SetAsync("person:05", "$", new { name = "Michael", age = 55, city = "San Antonio" });
```

## Search
Create the schema to index name as text field, age as a numeric field and city as tag field.
```csharp
var schema = new Schema().AddTextField("name").AddNumericField("age", true).AddTagField("city");
```

Filter the index to only include Jsons with prefix of person:
```csharp
var parameters = FTCreateParams.CreateParams().On(Literals.Enums.IndexDataType.JSON).Prefix("person:");
```

Create the index via pipeline
```csharp
pipeline.Ft.CreateAsync("person-idx", parameters, schema);
```

Search for all indexed person records
```csharp
var getAllPersons = pipeline.Ft.SearchAsync("person-idx", new Query());
```

Execute the pipeline
```csharp
pipeline.Execute();
```