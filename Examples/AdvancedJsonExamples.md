# Lab 4 - Advanced JSON
Redis JSON array filtering examples
## Contents
1.  [Business Value Statement](#value)
2.  [Data Set](#dataset)
3.  [Data Loading](#dataload)
4.  [Array Filtering Examples](#arrayfiltering)
    1.  [All Properties of Array](#allprops)
    2.  [All Properties of a Field](#allfield)
    3.  [Relational - Equality](#equality)
    4.  [Relational - Less Than](#lessthan)
    5.  [Relational - Greater Than or Equal](#greaterthan)
    6.  [Logical AND](#logicaland)
    7.  [Logical OR](#logicalor)
    8.  [Regex - Contains Exact](#regex_exact)
    9.  [Regex - Contains, Case Insensitive](#regex_contains)
    10.  [Regex - Begins With](#regex_begins)

## Business Value Statement <a name="value"></a>
The ability to query within a JSON object unlocks further value to the underlying data.  Redis supports JSONPath array filtering natively.
## Data Set <a name="dataset"></a>
```JSON
{
    "city": "Boston",
    "location": "42.361145, -71.057083",
    "inventory": [
        {
            "id": 15970,
            "gender": "Men",
            "season":["Fall", "Winter"],
            "description": "Turtle Check Men Navy Blue Shirt",
            "price": 34.95
        },
        {
            "id": 59263,
            "gender": "Women",
            "season": ["Fall", "Winter", "Spring", "Summer"],
            "description": "Titan Women Silver Watch",
            "price": 129.99
        },
        {
            "id": 46885,
            "gender": "Boys",
            "season": ["Fall"],
            "description": "Ben 10 Boys Navy Blue Slippers",
            "price": 45.99
        }
    ]
}
```
## Data Loading <a name="dataload"></a>
```c#
IJsonCommands json = db.JSON();
json.Set("warehouse:1", "$", new {
    city = "Boston",
    location = "42.361145, -71.057083",
    inventory = new[] {
        new {
            id = 15970,
            gender = "Men",
            season = new[] {"Fall", "Winter"},
            description = "Turtle Check Men Navy Blue Shirt",
            price = 34.95
        },
        new {
            id = 59263,
            gender = "Women",
            season = new[] {"Fall", "Winter", "Spring", "Summer"},
            description = "Titan Women Silver Watch",
            price = 129.99
        },
        new {
            id = 46885,
            gender = "Boys",
            season = new[] {"Fall"},
            description =  "Ben 10 Boys Navy Blue Slippers",
            price = 45.99
        }
    }
});
```
## Array Filtering Examples <a name="arrayfiltering"></a>
### Syntax
[JSON.GET](https://redis.io/commands/json.get/)

### All Properties of Array <a name="allprops"></a>
Fetch all properties of an array.
#### Command
```c#
Console.WriteLine(json.Get(key: "warehouse:1",
    path: "$.inventory[*]",
    indent: "\t",
    newLine: "\n"
));
```
#### Result
```json
[
        {
                "id":15970,
                "gender":"Men",
                "season":[
                        "Fall",
                        "Winter"
                ],
                "description":"Turtle Check Men Navy Blue Shirt",
                "price":34.95
        },
        {
                "id":59263,
                "gender":"Women",
                "season":[
                        "Fall",
                        "Winter",
                        "Spring",
                        "Summer"
                ],
                "description":"Titan Women Silver Watch",
                "price":129.99
        },
        {
                "id":46885,
                "gender":"Boys",
                "season":[
                        "Fall"
                ],
                "description":"Ben 10 Boys Navy Blue Slippers",
                "price":45.99
        }
]
```

### All Properties of a Field <a name="allfield"></a>
Fetch all values of a field within an array.
#### Command
```c#
Console.WriteLine(json.Get(key: "warehouse:1",
    path: "$.inventory[*].price",
    indent: "\t",
    newLine: "\n"
));
```
#### Result
```json
[
        34.95,
        129.99,
        45.99
]
```

### Relational - Equality <a name="equality"></a>
Fetch all items within an array where a text field matches a given value.
#### Command
```c#
Console.WriteLine(json.Get(key: "warehouse:1",
    path: "$.inventory[?(@.description==\"Turtle Check Men Navy Blue Shirt\")]",
    indent: "\t",
    newLine: "\n"
));
```
#### Result
```json
[
        {
                "id":15970,
                "gender":"Men",
                "season":[
                        "Fall",
                        "Winter"
                ],
                "description":"Turtle Check Men Navy Blue Shirt",
                "price":34.95
        }
]
```

### Relational - Less Than <a name="lessthan"></a>
Fetch all items within an array where a numeric field is less than a given value.
#### Command
```c#
Console.WriteLine(json.Get(key: "warehouse:1",
    path: "$.inventory[?(@.price<100)]",
    indent: "\t",
    newLine: "\n"
));
```
#### Result
```json
[
        {
                "id":15970,
                "gender":"Men",
                "season":[
                        "Fall",
                        "Winter"
                ],
                "description":"Turtle Check Men Navy Blue Shirt",
                "price":34.95
        },
        {
                "id":46885,
                "gender":"Boys",
                "season":[
                        "Fall"
                ],
                "description":"Ben 10 Boys Navy Blue Slippers",
                "price":45.99
        }
]
```

### Relational - Greater Than or Equal <a name="greaterthan"></a>
Fetch all items within an array where a numeric field is greater than or equal to a given value.
#### Command
```c#
Console.WriteLine(json.Get(key: "warehouse:1",
    path: "$.inventory[?(@.id>=20000)]",
    indent: "\t",
    newLine: "\n"
));
```
#### Result
```json
[
        {
                "id":59263,
                "gender":"Women",
                "season":[
                        "Fall",
                        "Winter",
                        "Spring",
                        "Summer"
                ],
                "description":"Titan Women Silver Watch",
                "price":129.99
        },
        {
                "id":46885,
                "gender":"Boys",
                "season":[
                        "Fall"
                ],
                "description":"Ben 10 Boys Navy Blue Slippers",
                "price":45.99
        }
]
```

### Logical AND <a name="logicaland"></a>
Fetch all items within an array that meet two relational operations.
#### Command
```c#
Console.WriteLine(json.Get(key: "warehouse:1",
    path: "$.inventory[?(@.gender==\"Men\"&&@.price>20)]",
    indent: "\t",
    newLine: "\n"
));
```
#### Result
```json
[
        {
                "id":15970,
                "gender":"Men",
                "season":[
                        "Fall",
                        "Winter"
                ],
                "description":"Turtle Check Men Navy Blue Shirt",
                "price":34.95
        }
]
```

### Logical OR <a name="logicalor"></a>
Fetch all items within an array that meet at least one relational operation.  In this case, return only the ids of those items.
#### Command
```c#
Console.WriteLine(json.Get(key: "warehouse:1",
    path: "$.inventory[?(@.price<100||@.gender==\"Women\")].id",
    indent: "\t",
    newLine: "\n"
));
```
#### Result
```json
[
        15970,
        59263,
        46885
]
```

### Regex - Contains Exact <a name="regex_exact"></a>
Fetch all items within an array that match a given regex pattern.
#### Command
```c#
Console.WriteLine(json.Get(key: "warehouse:1",
    path: "$.inventory[?(@.description =~ \"Blue\")]",
    indent: "\t",
    newLine: "\n"
));
```
#### Result
```json
[
        {
                "id":15970,
                "gender":"Men",
                "season":[
                        "Fall",
                        "Winter"
                ],
                "description":"Turtle Check Men Navy Blue Shirt",
                "price":34.95
        },
        {
                "id":46885,
                "gender":"Boys",
                "season":[
                        "Fall"
                ],
                "description":"Ben 10 Boys Navy Blue Slippers",
                "price":45.99
        }
]
```

### Regex - Contains, Case Insensitive <a name="regex_contains"></a>
Fetch all items within an array where a field contains a term, case insensitive.
#### Command
```c#
Console.WriteLine(json.Get(key: "warehouse:1",
    path: "$.inventory[?(@.description =~ \"(?i)watch\")]",
    indent: "\t",
    newLine: "\n"
));
```
#### Result
```json
[
        {
                "id":59263,
                "gender":"Women",
                "season":[
                        "Fall",
                        "Winter",
                        "Spring",
                        "Summer"
                ],
                "description":"Titan Women Silver Watch",
                "price":129.99
        }
]
```

### Regex - Begins With <a name="regex_begins"></a>
Fetch all items within an array where a field begins with a given expression.
#### Command
```c#
Console.WriteLine(json.Get(key: "warehouse:1",
    path: "$.inventory[?(@.description =~ \"^T\")]",
    indent: "\t",
    newLine: "\n"
));
```
#### Result
```json
[
        {
                "id":59263,
                "gender":"Women",
                "season":[
                        "Fall",
                        "Winter",
                        "Spring",
                        "Summer"
                ],
                "description":"Titan Women Silver Watch",
                "price":129.99
        }
]

*** Lab 4 - Regex - Begins With ***
[
        {
                "id":15970,
                "gender":"Men",
                "season":[
                        "Fall",
                        "Winter"
                ],
                "description":"Turtle Check Men Navy Blue Shirt",
                "price":34.95
        },
        {
                "id":59263,
                "gender":"Women",
                "season":[
                        "Fall",
                        "Winter",
                        "Spring",
                        "Summer"
                ],
                "description":"Titan Women Silver Watch",
                "price":129.99
        }
]
```