# Basic Query Operations
Examples of simple query operations with RediSearch
## Contents
1.  [Business Value Statement](#value)
2.  [Modules Needed](#modules)
3.  [Data Set](#dataset)
4.  [Data Loading](#loading)
5.  [Index Creation](#index_creation)
6.  [Search Examples](#search_examples)
    1.  [Retrieve All](#retrieve_all)
    2.  [Single Term Text](#single_term)
    3.  [Exact Phrase Text](#exact_phrase)
    4.  [Numeric Range](#numeric_range)
    5.  [Tag Array](#tag_array)
    6.  [Logical AND](#logical_and)
    7.  [Logical OR](#logical_or)
    8.  [Negation](#negation)
    9.  [Prefix](#prefix)
    10.  [Suffix](#suffix)
    11.  [Fuzzy](#fuzzy)
    12.  [Geo](#geo)

## Business Value Statement <a name="value"></a>
Search is an essential function to derive the value of data.  Redis provides inherent, high-speed search capabilities for JSON and Hash Set data.
## Modules Needed <a name="modules"></a>
```c#
using StackExchange.Redis;
using NRedisStack;
using NRedisStack.RedisStackCommands;
using NRedisStack.Search;
using NRedisStack.Search.Literals.Enums;
```

## Data Set <a name="dataset"></a>
```JSON
[
    {
        "id": 15970,
        "gender": "Men",
        "season":["Fall", "Winter"],
        "description": "Turtle Check Men Navy Blue Shirt",
        "price": 34.95,
        "city": "Boston",
        "location": "42.361145, -71.057083"
    },
    {
        "id": 59263,
        "gender": "Women",
        "season": ["Fall", "Winter", "Spring", "Summer"],
        "description": "Titan Women Silver Watch",
        "price": 129.99,
        "city": "Dallas",
        "location": "32.779167, -96.808891"
    },
    {
        "id": 46885,
        "gender": "Boys",
        "season": ["Fall"],
        "description": "Ben 10 Boys Navy Blue Slippers",
        "price": 45.99,
        "city": "Denver",
        "location": "39.742043, -104.991531"
    }
]
```
## Data Loading <a name="loading"></a>
```c#
IJsonCommands json = db.JSON();
json.Set("product:15970", "$", new {
    id = 15970,
    gender = "Men",
    season = new[] {"Fall", "Winter"},
    description = "Turtle Check Men Navy Blue Shirt",
    price = 34.95,
    city = "Boston",
    coords = "-71.057083, 42.361145"
});
json.Set("product:59263", "$", new {
    id = 59263,
    gender = "Women",
    season = new[] {"Fall", "Winter", "Spring", "Summer"},
    description = "Titan Women Silver Watch",
    price = 129.99,
    city = "Dallas",
    coords = "-96.808891, 32.779167"
});
json.Set("product:46885", "$", new {
    id = 46885,
    gender = "Boys",
    season = new[] {"Fall"},
    description = "Ben 10 Boys Navy Blue Slippers",
    price = 45.99,
    city = "Denver",
    coords = "-104.991531, 39.742043"
});
```
## Index Creation <a name="index_creation"></a>
### Syntax
[FT.CREATE](https://redis.io/commands/ft.create/)

#### Command
```c#
ISearchCommands ft = db.FT();
try {ft.DropIndex("idx1");} catch {};
ft.Create("idx1",   new FTCreateParams().On(IndexDataType.JSON)
                                        .Prefix("product:"),
                                new Schema().AddNumericField(new FieldName("$.id", "id"))
                                            .AddTagField(new FieldName("$.gender", "gender"))
                                            .AddTagField(new FieldName("$.season.*", "season"))
                                            .AddTextField(new FieldName("$.description", "description"))
                                            .AddNumericField(new FieldName("$.price", "price"))
                                            .AddTextField(new FieldName("$.city", "city"))
                                            .AddGeoField(new FieldName("$.coords", "coords")));
```

## Search Examples <a name="search_examples"></a>
### Syntax
[FT.SEARCH](https://redis.io/commands/ft.search/)

### Retrieve All <a name="retrieve_all"></a>
Find all documents for a given index.
#### Command
```c#
foreach (var doc in ft.Search("idx1", new Query("*")).ToJson())
{
    Console.WriteLine(doc);
}
```
#### Result
```json
{"id":15970,"gender":"Men","season":["Fall","Winter"],"description":"Turtle Check Men Navy Blue Shirt","price":34.95,"city":"Boston","coords":"-71.057083, 42.361145"}
{"id":46885,"gender":"Boys","season":["Fall"],"description":"Ben 10 Boys Navy Blue Slippers","price":45.99,"city":"Denver","coords":"-104.991531, 39.742043"}
{"id":59263,"gender":"Women","season":["Fall","Winter","Spring","Summer"],"description":"Titan Women Silver Watch","price":129.99,"city":"Dallas","coords":"-96.808891, 32.779167"}
```

### Single Term Text <a name="single_term"></a>
Find all documents with a given word in a text field.
#### Command
```c#
foreach (var doc in ft.Search("idx1", new Query("@description:Slippers"))
                    .ToJson())
{
    Console.WriteLine(doc);
}
```
#### Result
```json
{"id":46885,"gender":"Boys","season":["Fall"],"description":"Ben 10 Boys Navy Blue Slippers","price":45.99,"city":"Denver","coords":"-104.991531, 39.742043"}
```

### Exact Phrase Text <a name="exact_phrase"></a>
Find all documents with a given phrase in a text field.
#### Command
```c#
foreach (var doc in ft.Search("idx1", new Query("@description:(\"Blue Shirt\")"))
                    .ToJson())
{
    Console.WriteLine(doc);
}
```
#### Result
```json
{"id":15970,"gender":"Men","season":["Fall","Winter"],"description":"Turtle Check Men Navy Blue Shirt","price":34.95,"city":"Boston","coords":"-71.057083, 42.361145"}
```

### Numeric Range <a name="numeric_range"></a>
Find all documents with a numeric field in a given range.
#### Command
```c#
foreach (var doc in ft.Search("idx1", new Query("@price:[40,130]"))
                .ToJson())
{
    Console.WriteLine(doc);
}
```
#### Result
```json
{"id":46885,"gender":"Boys","season":["Fall"],"description":"Ben 10 Boys Navy Blue Slippers","price":45.99,"city":"Denver","coords":"-104.991531, 39.742043"}
{"id":59263,"gender":"Women","season":["Fall","Winter","Spring","Summer"],"description":"Titan Women Silver Watch","price":129.99,"city":"Dallas","coords":"-96.808891, 32.779167"}
```

### Tag Array <a name="tag_array"></a>
Find all documents that contain a given value in an array field (tag).
#### Command
```c#
foreach (var doc in ft.Search("idx1", new Query("@season:{Spring}"))
                    .ToJson())
{
    Console.WriteLine(doc);
}
```
#### Result
```json
{"id":59263,"gender":"Women","season":["Fall","Winter","Spring","Summer"],"description":"Titan Women Silver Watch","price":129.99,"city":"Dallas","coords":"-96.808891, 32.779167"}
```

### Logical AND <a name="logical_and"></a>
Find all documents contain both a numeric field in a range and a word in a text field.
#### Command
```c#
foreach (var doc in ft.Search("idx1", new Query("@price:[40, 100] @description:Blue"))
                    .ToJson())
{
    Console.WriteLine(doc);
}
```
#### Result
```json
{"id":46885,"gender":"Boys","season":["Fall"],"description":"Ben 10 Boys Navy Blue Slippers","price":45.99,"city":"Denver","coords":"-104.991531, 39.742043"}
```

### Logical OR <a name="logical_or"></a>
Find all documents that either match tag value or text value.
#### Command
```c#
foreach (var doc in ft.Search("idx1", new Query("(@gender:{Women})|(@city:Boston)"))
                    .ToJson())
{
    Console.WriteLine(doc);
}
```
#### Result
```json
{"id":15970,"gender":"Men","season":["Fall","Winter"],"description":"Turtle Check Men Navy Blue Shirt","price":34.95,"city":"Boston","coords":"-71.057083, 42.361145"}
{"id":59263,"gender":"Women","season":["Fall","Winter","Spring","Summer"],"description":"Titan Women Silver Watch","price":129.99,"city":"Dallas","coords":"-96.808891, 32.779167"}
```

### Negation <a name="negation"></a>
Find all documents that do not contain a given word in a text field.
#### Command
```c#
foreach (var doc in ft.Search("idx1", new Query("-(@description:Shirt)"))
                    .ToJson())
{
    Console.WriteLine(doc);
}
```
#### Result
```json
{"id":46885,"gender":"Boys","season":["Fall"],"description":"Ben 10 Boys Navy Blue Slippers","price":45.99,"city":"Denver","coords":"-104.991531, 39.742043"}
{"id":59263,"gender":"Women","season":["Fall","Winter","Spring","Summer"],"description":"Titan Women Silver Watch","price":129.99,"city":"Dallas","coords":"-96.808891, 32.779167"}
```

### Prefix <a name="prefix"></a>
Find all documents that have a word that begins with a given prefix value.
#### Command
```c#
foreach (var doc in ft.Search("idx1", new Query("@description:Nav*"))
                    .ToJson())
{
    Console.WriteLine(doc);
}
```
#### Result
```json
{"id":15970,"gender":"Men","season":["Fall","Winter"],"description":"Turtle Check Men Navy Blue Shirt","price":34.95,"city":"Boston","coords":"-71.057083, 42.361145"}
{"id":46885,"gender":"Boys","season":["Fall"],"description":"Ben 10 Boys Navy Blue Slippers","price":45.99,"city":"Denver","coords":"-104.991531, 39.742043"}
```

### Suffix <a name="suffix"></a>
Find all documents that contain a word that ends with a given suffix value.
#### Command
```c#
foreach (var doc in ft.Search("idx1", new Query("@description:*Watch"))
                    .ToJson())
{
    Console.WriteLine(doc);
}
```
#### Result
```json
{"id":59263,"gender":"Women","season":["Fall","Winter","Spring","Summer"],"description":"Titan Women Silver Watch","price":129.99,"city":"Dallas","coords":"-96.808891, 32.779167"}
```

### Fuzzy <a name="fuzzy"></a>
Find all documents that contain a word that is within 1 Levenshtein distance of a given word.
#### Command
```c#
foreach (var doc in ft.Search("idx1", new Query("@description:%wavy%"))
                    .ToJson())
{
    Console.WriteLine(doc);
}
```
#### Result
```json
{"id":15970,"gender":"Men","season":["Fall","Winter"],"description":"Turtle Check Men Navy Blue Shirt","price":34.95,"city":"Boston","coords":"-71.057083, 42.361145"}
{"id":46885,"gender":"Boys","season":["Fall"],"description":"Ben 10 Boys Navy Blue Slippers","price":45.99,"city":"Denver","coords":"-104.991531, 39.742043"}
```

### Geo <a name="geo"></a>
Find all documents that have geographic coordinates within a given range of a given coordinate.
Colorado Springs coords (long, lat) = -104.800644, 38.846127
#### Command
```c#
foreach (var doc in ft.Search("idx1", new Query("@coords:[-104.800644 38.846127 100 mi]"))
                    .ToJson())
{
    Console.WriteLine(doc);
}
```
#### Result
```json
{"id":46885,"gender":"Boys","season":["Fall"],"description":"Ben 10 Boys Navy Blue Slippers","price":45.99,"city":"Denver","coords":"-104.991531, 39.742043"}
```