# GeoShape Fields Usage In RediSearch

NRedisStack now supports GEOSHAPE field querying.

Any object that serializes the [well-known text (WKT)](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry) as a `string` can be used with NRedisStack.

Using GeoShape fields in searches with the [NetTopologySuite](https://github.com/NetTopologySuite/NetTopologySuite) library.

## Example

### Modules Needed

```c#
using StackExchange.Redis;
using NRedisStack.RedisStackCommands;
using NRedisStack.Search;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
```

### Setup

```csharp
// Connect to the Redis server:
var redis = ConnectionMultiplexer.Connect("localhost");
var db = redis.GetDatabase();
// Get a reference to the database and for search commands:
var ft = db.FT();

// Create WTKReader and GeometryFactory objects:
WKTReader reader = new WKTReader();
GeometryFactory factory = new GeometryFactory();

```

### Create the index

```csharp
ft.Create(index, new Schema().AddGeoShapeField("geom", GeoShapeField.CoordinateSystem.FLAT));
```

### Prepare the data

```csharp
Polygon small = factory.CreatePolygon(new Coordinate[]{new Coordinate(1, 1),
new Coordinate(1, 100), new Coordinate(100, 100), new Coordinate(100, 1), new Coordinate(1, 1)});
db.HashSet("small", "geom", small.ToString());

Polygon large = factory.CreatePolygon(new Coordinate[]{new Coordinate(1, 1),
new Coordinate(1, 200), new Coordinate(200, 200), new Coordinate(200, 1), new Coordinate(1, 1)});
db.HashSet("large", "geom", large.ToString());
```

## Polygon type

### Querying within condition

```csharp
Polygon within = factory.CreatePolygon(new Coordinate[]{new Coordinate(0, 0),
new Coordinate(0, 150), new Coordinate(150, 150), new Coordinate(150, 0), new Coordinate(0, 0)});

SearchResult res = ft.Search(index, new Query("@geom:[within $poly]")
   .AddParam("poly", within.ToString()) // Note serializing the argument to string
   .Dialect(3)); // DIALECT 3 is required for this query
```

The search result from redis is:

```bash
1) (integer) 1
2) "small"
3) 1) "geom"
   2) "POLYGON ((1 1, 1 100, 100 100, 100 1, 1 1))"
```

Use the reader to get the polygon:

```csharp
reader.Read(res.Documents[0]["geom"].ToString());
```

### Querying contains condition

```csharp
Polygon contains = factory.CreatePolygon(new Coordinate[]{new Coordinate(2, 2),
new Coordinate(2, 50), new Coordinate(50, 50), new Coordinate(50, 2), new Coordinate(2, 2)});

res = ft.Search(index, new Query("@geom:[contains $poly]")
    .AddParam("poly", contains.ToString()) // Note serializing the argument to string
    .Dialect(3)); // DIALECT 3 is required for this query

```

Our search result:

```bash
1) (integer) 2
2) "small"
3) 1) "geom"
   2) "POLYGON ((1 1, 1 100, 100 100, 100 1, 1 1))"
4) "large"
5) 1) "geom"
   2) "POLYGON ((1 1, 1 200, 200 200, 200 1, 1 1))"
```

### Searching with Coordinates

```csharp
Point point = factory.CreatePoint(new Coordinate(10, 10));
db.HashSet("point", "geom", point.ToString());

res = ft.Search(index, new Query("@geom:[within $poly]")
   .AddParam("poly", within.ToString()) // Note serializing the argument to string
   .Dialect(3)); // DIALECT 3 is required for this query

```

Our search result:

```bash
1) (integer) 2
2) "small"
3) 1) "geom"
   2) "POLYGON ((1 1, 1 100, 100 100, 100 1, 1 1))"
4) "point"
5) 1) "geom"
   2) "POINT (10 10)"
```
