# Converting Search Result to JSON

## This example shows how to convert Redis search results to JSON format using NRedisStack

Connect to the Redis server:

```csharp
var redis = ConnectionMultiplexer.Connect("localhost");
```

Get a reference to the database and for search and json commands:

```csharp
var db = redis.GetDatabase();
var ft = db.FT();
var json = db.JSON();
```

Create a search index with a JSON field:

```csharp
ft.Create("test", new FTCreateParams().On(IndexDataType.JSON).Prefix("doc:"),
            new Schema().AddTagField(new FieldName("$.name", "name")));
```

Insert 10 JSON documents into the index:

```csharp
for (int i = 0; i < 10; i++)
{
    json.Set("doc:" + i, "$", "{\"name\":\"foo\"}");
}
```

Execute a search query and convert the results to JSON:

```csharp
var res = ft.Search("test", new Query("@name:{foo}"));
var docs = res.ToJson();
```

Now the `docs` variable contains a JSON list (IEnumerable) of the search results.
