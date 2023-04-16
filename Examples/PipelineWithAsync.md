# Pipeline With Async
## An example of pipelines Redis Stack Redis commands (JSON.SET & JSON.CLEAR & JSON.GET)

Connect to the Redis server
```csharp
var redis = ConnectionMultiplexer.Connect("localhost");
```

Get a reference to the database
```csharp
var db = redis.GetDatabase();
```

Setup pipeline connection
```csharp
var pipeline = new Pipeline(db);
```

Create metadata labels for a TimeSeries object:

```csharp
TimeSeriesLabel label1 = new TimeSeriesLabel("temp", "TLV");
TimeSeriesLabel label2 = new TimeSeriesLabel("temp", "JLM");
var labels1 = new List<TimeSeriesLabel> { label1 };
var labels2 = new List<TimeSeriesLabel> { label2 };
```

Create a new time-series object:

```csharp
pipeline.Ts.CreateAsync("temp:TLV", labels: labels1);
pipeline.Ts.CreateAsync("temp:JLM", labels: labels2);
```

Create the TimeSeries objects, and store them in Redis:

```csharp
List<(string, TimeStamp, double)> sequence1 = new List<(string, TimeStamp, double)>()
{
   ("temp:TLV",1000,30),
   ("temp:TLV", 1010 ,35),
   ("temp:TLV", 1020, 9999),
   ("temp:TLV", 1030, 40)
};
List<(string, TimeStamp, double)> sequence2 = new List<(string, TimeStamp, double)>()
{
   ("temp:JLM",1005,30),
   ("temp:JLM", 1015 ,35),
   ("temp:JLM", 1025, 9999),
   ("temp:JLM", 1035, 40)
};

pipeline.Ts.MAddAsync(sequence1);
pipeline.Ts.MAddAsync(sequence2);

```

Execute the pipeline:

```csharp
pipeline.Execute();
```

Get a reference to the database and for TimeSeries commands:
```csharp
var ts = db.TS();
```

Get only the location label for each last sample, use SELECTED_LABELS.
```csharp
var respons = await ts.MGetAsync(new List<string> { "temp=JLM" }, selectedLabels: new List<string> { "location" });
```
