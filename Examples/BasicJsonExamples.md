# Basic JSON Operations
Create, read, update, delete (CRUD) operations with the Redis JSON data type
## Contents
1.  [Business Value Statement](#value)
2.  [Create](#create)
    1.  [Key Value Pair](#kvp)
    2.  [Single String Property](#single_string)
    3.  [Multiple Properties](#multiple_properties)
    4.  [Multiple Properties + Data Types](#multiple_types)
    5.  [JSON Arrays](#arrays)
    6.  [JSON Objects](#objects)
    7.  [Mix](#mix)
3.  [Read](#read)
    1.  [Key Fetch](#key_fetch)
    2.  [Single Property Fetch](#single_fetch)
    3.  [Multi-Property Fetch](#multiple_fetch)
    4.  [Nested Property Fetch](#nested_fetch)
    5.  [Array Fetch](#array_fetch)
4.  [Update](#update)
    1.  [Entire Object](#entire_update)
    2.  [Single Property](#single_update)
    3.  [Nested Property](#nested_update)
    4.  [Array Item](#array_update)
5.  [Delete](#delete)
    1.  [Entire Object](#entire_delete)
    2.  [Single Property](#single_delete)
    3.  [Nested Property](#nested_delete)
    4.  [Array Item](#array_delete)

## Business value statement <a name="value"></a>
Document stores are a NoSQL database type that provide flexible schemas and access patterns familiar to developers. Redis natively provides document store functionality with its JSON data type. Hence, Redis complements existing document store databases such as MongoDB or provides standalone JSON document storage.

## Create <a name="create"></a>
### Syntax
[JSON.SET](https://redis.io/commands/json.set/)

### Key-value pair <a name="kvp"></a>
Insert a simple KVP as a JSON object.
#### Command
```c#
JsonCommands json = db.JSON();
Console.WriteLine(json.Set("ex1:1", "$", "\"val\""));
```
#### Result
```bash
True
```


### Single string property <a name="single_string"></a>
Insert a single-property JSON object.
#### Command
```c#
Console.WriteLine(json.Set("ex1:2", "$", new {field1 = "val1" }));
```
#### Result
```bash
True
```

### Multiple Properties <a name="multiple_properties"></a>
Insert a JSON object with multiple properties.
#### Command
```c#
Console.WriteLine(json.Set("ex1:3", "$", new {
    field1 = "val1",
    field2 = "val2"
}));
```
#### Result
```bash
True
```

### Multiple Properties + Data Types <a name="multiple_types"></a>
Insert a JSON object with multiple properties of different data types.
#### Command
```c#
Console.WriteLine(json.Set("ex1:4", "$", new {
    field1 = "val1",
    field2 = "val2",
    field3 = true,
    field4 = (string?) null
}));
```
#### Result
```bash
True
```

### JSON Arrays <a name="arrays"></a>
Insert a JSON object that contains an array.
#### Command
```c#
Console.WriteLine(json.Set("ex1:5", "$", new {
    arr1 = new [] {"val1", "val2", "val3"}
}));
```
#### Result
```bash
True
```

### JSON Objects <a name="objects"></a>
Insert a JSON object that contains a nested object.
#### Command
```c#
Console.WriteLine(json.Set("ex1:6", "$", new {
    obj1 = new {
        str1 = "val1",
        num2 = 2
    }
}));
```
#### Result
```bash
True
```

### Mix <a name="mix"></a>
Insert a JSON object with a mixture of property data types.
#### Command
```c#
Console.WriteLine(json.Set("ex1:7", "$", new {
    str1 = "val1",
    str2 = "val2",
    arr1 = new [] {1,2,3,4},
    obj1 = new {
        num1 = 1,
        arr2 = new [] {"val1","val2", "val3"}
    }
}));
```
#### Result
```bash
True
```

## Read <a name="read"></a>
### Syntax
[JSON.GET](https://redis.io/commands/json.get/)

### Key Fetch <a name="key_fetch"></a>
Set and Fetch a simple JSON KVP.
#### Command
```c#
json.Set("ex2:1", "$", "\"val\"");
Console.WriteLine(json.Get(key: "ex2:1",
    path: "$",
    indent: "\t",
    newLine: "\n"
));
```
#### Result
```bash
[
        "val"
]
```

### Single Property Fetch <a name="single_fetch"></a>
Set and Fetch a single property from a JSON object.
#### Command
```c#
json.Set("ex2:2", "$", new {
    field1 = "val1"
});
Console.WriteLine(json.Get(key: "ex2:2",
    path: "$.field1",
    indent: "\t",
    newLine: "\n"
));
```
#### Result
```bash
[
        "val1"
]
```

### Multi-Property Fetch <a name="multiple_fetch"></a>
Fetch multiple properties.
#### Command
```c#
json.Set("ex2:3", "$", new {
    field1 = "val1",
    field2 = "val2"
});
Console.WriteLine(json.Get(key: "ex2:3",
    paths: new[] {"$.field1", "$.field2" },
    indent: "\t",
    newLine: "\n"
));
```
#### Result
```bash
{
        "$.field1":[
                "val1"
        ],
        "$.field2":[
                "val2"
        ]
}
```

### Nested Property Fetch <a name="nested_fetch"></a>
Fetch a property nested in another JSON object.
#### Command
```c#
json.Set("ex2:4", "$", new {
    obj1 = new {
    str1 = "val1",
    num2 = 2
    }
});
Console.WriteLine(json.Get(key: "ex2:4",
    path: "$.obj1.num2",
    indent: "\t",
    newLine: "\n"
));
```
#### Result
```bash
[
        2
]
```

### Array Fetch <a name="array_fetch"></a>
Fetch properties within an array and utilize array subscripting.
#### Command
```c#
json.Set("ex2:5", "$",new {
    str1 = "val1",
    str2 = "val2",
    arr1 = new[] {1,2,3,4},
    obj1 = new {
        num1 = 1,
        arr2 = new[] {"val1","val2", "val3"}
    }
});
Console.WriteLine(json.Get(key: "ex2:5",
    path: "$.obj1.arr2",
    indent: "\t",
    newLine: "\n"
));
Console.WriteLine(json.Get(key: "ex2:5",
    path: "$.arr1[1]",
    indent: "\t",
    newLine: "\n"
));
Console.WriteLine(json.Get(key: "ex2:5",
    path: "$.obj1.arr2[0:2]",
    indent: "\t",
    newLine: "\n"
));
Console.WriteLine(json.Get(key: "ex2:5",
    path: "$.arr1[-2:]",
    indent: "\t",
    newLine: "\n"
));
```
#### Results
```bash
[
        [
                "val1",
                "val2",
                "val3"
        ]
]
[
        2
]
[
        "val1",
        "val2"
]
[
        3,
        4
]
```

## Update <a name="update"></a>
### Syntax
[JSON.SET](https://redis.io/commands/json.set/)

### Entire Object <a name="entire_update"></a>
Update an entire JSON object.
#### Command
```c#
json.Set("ex3:1", "$", new {field1 = "val1"});
json.Set("ex3:1", "$", new {foo = "bar"});
Console.WriteLine(json.Get(key: "ex3:1",
    indent: "\t",
    newLine: "\n"
));
```
#### Result
```bash
{
        "foo":"bar"
}
```

### Single Property <a name="single_update"></a>
Update a single property within an object.
#### Command
```c#
json.Set("ex3:2", "$", new {
    field1 = "val1",
    field2 = "val2"
});
json.Set("ex3:2", "$.field1", "\"foo\"");
Console.WriteLine(json.Get(key: "ex3:2",
    indent: "\t",
    newLine: "\n"
));
```
#### Result
```bash
{
        "field1":"foo",
        "field2":"val2"
}
```

### Nested Property <a name="nested_update"></a>
Update a property in an embedded JSON object.
#### Command
```c#
json.Set("ex3:3", "$", new {
    obj1 = new {
        str1 = "val1",
        num2 = 2
    }
});
json.Set("ex3:3", "$.obj1.num2", 3);
Console.WriteLine(json.Get(key: "ex3:3",
    indent: "\t",
    newLine: "\n"
));
```
#### Result
```bash
{
        "obj1":{
                "str1":"val1",
                "num2":3
        }
}
```

### Array Item <a name="array_update"></a>
Update an item in an array via index.
#### Command
```c#
json.Set("ex3:4", "$", new {
    arr1 = new[] {"val1", "val2", "val3"}
});
json.Set("ex3:4", "$.arr1[0]", "\"foo\"");
Console.WriteLine(json.Get(key: "ex3:4",
    indent: "\t",
    newLine: "\n"
));
```
#### Result
```bash
{
        "arr1":[
                "foo",
                "val2",
                "val3"
        ]
}
```

## Delete <a name="delete"></a>
### Syntax
[JSON.DEL](https://redis.io/commands/json.del/)

### Entire object <a name="entire_delete"></a>
Delete entire object/key.
#### Command
```c#
json.Set("ex4:1", "$", new {field1 = "val1"});
json.Del("ex4:1");
Console.WriteLine(json.Get(key: "ex4:1",
    indent: "\t",
    newLine: "\n"
));
```
#### Result
```bash

```

### Single Property <a name="single_delete"></a>
Delete a single property from an object.
#### Command
```c#
json.Set("ex4:2", "$", new {
    field1 = "val1",
    field2 =  "val2"
});
json.Del("ex4:2", "$.field1");
Console.WriteLine(json.Get(key: "ex4:2",
    indent: "\t",
    newLine: "\n"
));
```
#### Result
```bash
{
        "field2":"val2"
}
```

### Nested property <a name="nested_delete"></a>
Delete a property from an embedded object.
#### Command
```c#
json.Set("ex4:3", "$", new {
    obj1 = new {
        str1 = "val1",
        num2 = 2
    }
});
json.Del("ex4:3", "$.obj1.num2");
Console.WriteLine(json.Get(key: "ex4:3",
    indent: "\t",
    newLine: "\n"
));
```
#### Result
```bash
{
        "obj1":{
                "str1":"val1"
        }
}
```

### Array item <a name="array_delete"></a>
Delete a single item from an array.
#### Command
```c#
json.Set("ex4:4", "$", new {
    arr1 = new[] {"val1", "val2", "val3"}
});
json.Del("ex4:4", "$.arr1[0]");
Console.WriteLine(json.Get(key: "ex4:4",
    indent: "\t",
    newLine: "\n"
));
```
#### Result
```bash
{
        "arr1":[
                "val2",
                "val3"
        ]
}
```