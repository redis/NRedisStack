# Transaction

## An example of transaction with Redis modules (JSON.SET, JSON.GET & JSON.NUMINCRBY)

Connect to the Redis server:

```cs
var redis = await ConnectionMultiplexer.ConnectAsync("localhost");
var db = redis.GetDatabase();
```

Create the transaction:

```cs
var tran = new Transaction(db);
```

Store the account details as JSON:

```csharp
tran.Json.SetAsync("accdetails:Jeeva", "$", new { name = "Jeeva", totalAmount= 1000, bankName = "City" });
tran.Json.SetAsync("accdetails:Shachar", "$", new { name = "Shachar", totalAmount = 1000, bankName = "City" });
```

Retrieve the responses

```csharp
var getShachar = tran.Json.GetAsync("accdetails:Shachar");
var getJeeva = tran.Json.GetAsync("accdetails:Jeeva");
```

Debit 200 from Jeeva

```cs
tran.Json.NumIncrbyAsync("accdetails:Jeeva", "$.totalAmount", -200);
```

Credit 200 from Shachar

```cs
tran.Json.NumIncrbyAsync("accdetails:Shachar", "$.totalAmount", 200);
```

Get total amount for both Jeeva = 800 & Shachar = 1200

```cs
var totalAmtOfJeeva = tran.Json.GetAsync("accdetails:Jeeva", path:"$.totalAmount");
var totalAmtOfShachar = tran.Json.GetAsync("accdetails:Shachar", path:"$.totalAmount");
```

Execute the transaction

```cs
var condition = tran.ExecuteAsync();
```
