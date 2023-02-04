# Transaction

## An example of transactions with Redis modules (JSON.SET, JSON.GET & JSON.NUMINCRBY)

Connect to the Redis server and Setup 2 Transactions
```cs
var redis = await ConnectionMultiplexer.ConnectAsync("localhost");
var db = redis.GetDatabase();
```

Setup transaction1 with IDatabase
```cs
var xAction1 = new Transactions(db);
```

Add account details with Json.Set to transaction1
```cs
xAction1.Json.SetAsync("accdetails:Jeeva", "$", new { name = "Jeeva", totalAmount= 1000, bankName = "City" });
xAction1.Json.SetAsync("accdetails:Shachar", "$", new { name = "Shachar", totalAmount = 1000, bankName = "City" });
```

Get the Json response within Tansaction1
```cs
var getShachar = xAction1.Json.GetAsync("accdetails:Shachar");
var getJeeva = xAction1.Json.GetAsync("accdetails:Jeeva");
```

Execute the transaction1
```cs
xAction1.ExecuteAsync();
```

Setup transaction2 with ConnectionMultiplexer
```cs
var xAction2 = new Transactions(redis);
```

Debit 200 from Jeeva within Tansaction2
```cs
xAction2.Json.NumIncrbyAsync("accdetails:Jeeva", "$.totalAmount", -200);
```
Credit 200 from Shachar within Tansaction2
```cs
xAction2.Json.NumIncrbyAsync("accdetails:Shachar", "$.totalAmount", 200);
```

Get total amount for both Jeeva = 800 & Shachar = 1200 within Tansaction2
```cs
var totalAmtOfJeeva = xAction2.Json.GetAsync("accdetails:Jeeva", path:"$.totalAmount");
var totalAmtOfShachar = xAction2.Json.GetAsync("accdetails:Shachar", path:"$.totalAmount");
```

Execute the transaction2
```cs
var condition = xAction2.ExecuteAsync();
```