# Transaction

## An example of transactions with Redis modules (JSON.SET, JSON.GET & JSON.NUMINCRBY)

Connect to the Redis server

```cs
var redis = await ConnectionMultiplexer.ConnectAsync("localhost");
var db = redis.GetDatabase();
```

Setup transaction

```cs
var tran = new Transactions(db);
```

Add account details with Json.Set

```cs
tran.Json.SetAsync("accdetails:Jeeva", "$", new { name = "Jeeva", totalAmount= 1000, bankName = "City" });
tran.Json.SetAsync("accdetails:Shachar", "$", new { name = "Shachar", totalAmount = 1000, bankName = "City" });
```

Get the Json response for both Jeeva & Shachar

```cs
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
