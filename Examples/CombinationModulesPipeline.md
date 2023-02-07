# Combination modules Pipeline

## An example of pipelines mixing a pipeline with a combination of module commands with JSON & Search

Connect to the Redis server:

```csharp
var redis = ConnectionMultiplexer.Connect("localhost");
```

Setup pipeline connection

```csharp
var db = redis.GetDatabase();
var pipeline = new Pipeline(db);
```

## JSON

Add JSON data to the pipeline.

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

Create a search index, that only retrieves JSON objects from keys prefixed *person*.

```csharp
var parameters = FTCreateParams.CreateParams().On(IndexDataType.JSON).Prefix("person:");
```

Create a search index, on our stored data:

```csharp
pipeline.Ft.CreateAsync("person-idx", parameters, schema);
```

Execute the pipeline

```csharp
pipeline.Execute();
```

Search for all indexed person records

```csharp
var getAllPersons = db.FT().SearchAsync("person-idx", new Query());
```
