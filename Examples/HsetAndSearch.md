# HSET and Search
## An example of mixing Redis open source command (HSET) with Redis Stack Redis commands (FT.CREATE & FT.SEARCH)

Connect to the Redis server:
```csharp
var redis = ConnectionMultiplexer.Connect("localhost");
```
Get a reference to the database and for search commands:
```csharp
var db = redis.GetDatabase();
var ft = db.FT();
```
Use HSET to add a field-value pair to a hash:
```csharp
db.HashSet("profesor:5555", new HashEntry[] { new("first", "Albert"), new("last", "Blue"), new("age", "55") });
db.HashSet("student:1111", new HashEntry[] { new("first", "Joe"), new("last", "Dod"), new("age", "18") });
db.HashSet("pupil:2222", new HashEntry[] { new("first", "Jen"), new("last", "Rod"), new("age", "14") });
db.HashSet("student:3333", new HashEntry[] { new("first", "El"), new("last", "Mark"), new("age", "17") });
db.HashSet("pupil:4444", new HashEntry[] { new("first", "Pat"), new("last", "Shu"), new("age", "21") });
db.HashSet("student:5555", new HashEntry[] { new("first", "Joen"), new("last", "Ko"), new("age", "20") });
db.HashSet("teacher:6666", new HashEntry[] { new("first", "Pat"), new("last", "Rod"), new("age", "20") });
```

Create the schema indexing the text fields ```first``` and ```last```, and ```age``` as a numeric field:
```csharp
var schema = new Schema().AddTextField("first").AddTextField("last").AddNumericField("age");
```
Filter the index to only include hashes with an age greater than 16, and prefix of 'student:' or 'pupil:'
```csharp
var parameters = FTCreateParams.CreateParams().Filter("@age>16").Prefix("student:", "pupil:");
```
Create the index:
```csharp
ft.Create("example_index", parameters, schema);
```
## Search Examples:

Search all hashes in the index:
```csharp
var noFilters = ft.Search("example_index", new Query());
```
_noFilters_ now contains: _student:1111_, _student:5555_, _pupil:4444_, _student:3333_.<br /><br />

Search for hashes with a first name starting with Jo
```csharp
var startWithJo = ft.Search("example_index", new Query("@first:Jo*"));
```
_startWithJo_ now contains: _student:1111_ (Joe), _student:5555_ (Joen).<br /><br />

Search for hashes with first name of Pat
```csharp
var namedPat = ft.Search("example_index", new Query("@first:Pat"));
```
_namedPat_ now contains _pupil:4444_ (Pat). _teacher:6666_ (Pat) is not included because it does not have a prefix of 'student:' or 'pupil:'<br /><br />

Search for hashes with last name of Rod
```csharp
var lastNameRod = ft.Search("example_index", new Query("@last:Rod"));
```
_lastNameRod_ is empty because there are no hashes with a last name of Rod that match the index definition.
