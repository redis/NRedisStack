using StackExchange.Redis;

namespace NRedisStack;

public interface IJsonCommands
{
    /// <summary>
    /// Appends the provided items to the array at the provided path.
    /// </summary>
    /// <param name="key">The key to append to</param>
    /// <param name="path">The path to append to</param>
    /// <param name="values">the values to append</param>
    /// <returns>The new array sizes for the appended paths</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.arrappend"/></remarks>
    long?[] ArrAppend(RedisKey key, string? path = null, params object[] values);

    /// <summary>
    /// Finds the index of the provided item within the provided range
    /// </summary>
    /// <param name="key">The key to look up.</param>
    /// <param name="path">The json path.</param>
    /// <param name="value">The value to find the index of.</param>
    /// <param name="start">The starting index within the array. Inclusive.</param>
    /// <param name="stop">The ending index within the array. Exclusive</param>
    /// <returns>The index of the value for each array the path resolved to.</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.arrindex"/></remarks>
    long?[] ArrIndex(RedisKey key, string path, object value, long? start = null, long? stop = null);

    /// <summary>
    /// Inserts the provided items at the provided index within a json array.
    /// </summary>
    /// <param name="key">The key to insert into.</param>
    /// <param name="path">The path of the array(s) within the key to insert into.</param>
    /// <param name="index">The index to insert at.</param>
    /// <param name="values">The values to insert</param>
    /// <returns>The new size of each array the item was inserted into.</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.arrinsert"/></remarks>
    long?[] ArrInsert(RedisKey key, string path, long index, params object[] values);

    /// <summary>
    /// Gets the length of the arrays resolved by the provided path.
    /// </summary>
    /// <param name="key">The key of the json object.</param>
    /// <param name="path">The path to the array(s)</param>
    /// <returns>The length of each array resolved by the json path.</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.arrlen"/></remarks>
    long?[] ArrLen(RedisKey key, string? path = null);

    /// <summary>
    /// Pops an item from the array(s) at the provided index. Or the last element if no index is provided.
    /// </summary>
    /// <param name="key">The json key to use.</param>
    /// <param name="path">The path of the array(s).</param>
    /// <param name="index">The index to pop from</param>
    /// <returns>The items popped from the array</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.arrpop"/></remarks>
    RedisResult[] ArrPop(RedisKey key, string? path = null, long? index = null);

    /// <summary>
    /// Trims the array(s) at the provided path, leaving the range between the specified indexes (inclusive).
    /// </summary>
    /// <param name="key">The key to trim from.</param>
    /// <param name="path">The path of the array(s) within the json object to trim.</param>
    /// <param name="start">the starting index to retain.</param>
    /// <param name="stop">The ending index to retain.</param>
    /// <returns>The new length of the array(s) after they're trimmed.</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.arrtrim"/></remarks>
    long?[] ArrTrim(RedisKey key, string path, long start, long stop);

    /// <summary>
    /// Clear's container values(arrays/objects), and sets numeric values to 0.
    /// </summary>
    /// <param name="key">The key to clear.</param>
    /// <param name="path">The path to clear.</param>
    /// <returns>number of values cleared</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.clear"/></remarks>
    long Clear(RedisKey key, string? path = null);

    /// <summary>
    /// Deletes a json value.
    /// </summary>
    /// <param name="key">The key to delete from.</param>
    /// <param name="path">The path to delete.</param>
    /// <returns>number of path's deleted</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.del"/></remarks>
    long Del(RedisKey key, string? path = null);

    /// <summary>
    /// Deletes a json value.
    /// </summary>
    /// <param name="key">The key to delete from.</param>
    /// <param name="path">The path to delete.</param>
    /// <returns>number of path's deleted</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.forget"/></remarks>
    long Forget(RedisKey key, string? path = null);

    /// <summary>
    /// Gets the value stored at the key and path in redis.
    /// </summary>
    /// <param name="key">The key to retrieve.</param>
    /// <param name="indent">the indentation string for nested levels</param>
    /// <param name="newLine">sets the string that's printed at the end of each line</param>
    /// <param name="space">sets the string that's put between a key and a value</param>
    /// <param name="path">the path to get.</param>
    /// <returns>The requested Items</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.get"/></remarks>
    RedisResult Get(RedisKey key, RedisValue? indent = null, RedisValue? newLine = null, RedisValue? space = null, RedisValue? path = null);

    /// <summary>
    /// Gets the values stored at the provided paths in redis.
    /// </summary>
    /// <param name="key">The key to pull from.</param>
    /// <param name="paths">The paths within the key to pull.</param>
    /// <param name="indent">the indentation string for nested levels</param>
    /// <param name="newLine">sets the string that's printed at the end of each line</param>
    /// <param name="space">sets the string that's put between a key and a value</param>
    /// <returns></returns>
    RedisResult Get(RedisKey key, string[] paths, RedisValue? indent = null, RedisValue? newLine = null, RedisValue? space = null);

    /// <summary>
    /// Generically gets an Item stored in Redis.
    /// </summary>
    /// <param name="key">The key to retrieve</param>
    /// <param name="path">The path to retrieve</param>
    /// <typeparam name="T">The type retrieved</typeparam>
    /// <returns>The object requested</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.get"/></remarks>
    T? Get<T>(RedisKey key, string path = "$");

    /// <summary>
    /// retrieves a group of items stored in redis, appropriate if the path will  resolve to multiple records.
    /// </summary>
    /// <param name="key">The key to pull from.</param>
    /// <param name="path">The path to pull.</param>
    /// <typeparam name="T">The type.</typeparam>
    /// <returns>An enumerable of the requested tyep</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.get"/></remarks>
    IEnumerable<T?> GetEnumerable<T>(RedisKey key, string path = "$");

    /// <summary>
    /// Gets the provided path from multiple keys
    /// </summary>
    /// <param name="keys">The keys to retrieve from.</param>
    /// <param name="path">The path to retrieve</param>
    /// <returns>An array of RedisResults with the requested data.</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.mget"/></remarks>
    RedisResult[] MGet(RedisKey[] keys, string path);

    /// <summary>
    /// Increments the fields at the provided path by the provided number.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="path">The path to increment.</param>
    /// <param name="value">The value to increment by.</param>
    /// <returns>The new values after being incremented, or null if the path resolved a non-numeric.</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.numincrby"/></remarks>
    double?[] NumIncrby(RedisKey key, string path, double value);

    /// <summary>
    /// Gets the keys of the object at the provided path.
    /// </summary>
    /// <param name="key">the key of the json object.</param>
    /// <param name="path">The path of the object(s)</param>
    /// <returns>the keys of the resolved object(s)</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.objkeys"/></remarks>
    IEnumerable<HashSet<string>> ObjKeys(RedisKey key, string? path = null);

    /// <summary>
    /// returns the number of keys in the object(s) at the provided path.
    /// </summary>
    /// <param name="key">The key of the json object.</param>
    /// <param name="path">The path of the object(s) to resolve.</param>
    /// <returns>The length of the object(s) keyspace.</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.objlen"/></remarks>
    long?[] ObjLen(RedisKey key, string? path = null);

    /// <summary>
    /// Gets the key in RESP(Redis Serialization Protocol) form.
    /// </summary>
    /// <param name="key">The key to get.</param>
    /// <param name="path">Path within the key to get.</param>
    /// <returns>the resultant resp</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.resp"/></remarks>
    RedisResult[] Resp(RedisKey key, string? path = null);

    /// <summary>
    /// Set's the key/path to the provided value.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="path">The path to set within the key.</param>
    /// <param name="obj">The value to set.</param>
    /// <param name="when">When to set the value.</param>
    /// <returns>The disposition of the command</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.set"/></remarks>
    bool Set(RedisKey key, RedisValue path, object obj, When when = When.Always);

    /// <summary>
    /// Set's the key/path to the provided value.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="path">The path to set within the key.</param>
    /// <param name="json">The value to set.</param>
    /// <param name="when">When to set the value.</param>
    /// <returns>The disposition of the command</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.set"/></remarks>
    bool Set(RedisKey key, RedisValue path, RedisValue json, When when = When.Always);

    /// <summary>
    /// Set json file from the provided file Path.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="path">The path to set within the key.</param>
    /// <param name="filePath">The path of the file to set.</param>
    /// <param name="when">When to set the value.</param>
    /// <returns>The disposition of the command</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.set"/></remarks>
    bool SetFile(RedisKey key, RedisValue path, string filePath, When when = When.Always);

    /// <summary>
    /// Appends the provided string to the string(s) at the provided path.
    /// </summary>
    /// <param name="key">The key to append to.</param>
    /// <param name="path">The path of the string(s) to append to.</param>
    /// <param name="value">The value to append.</param>
    /// <returns>The new length of the string(s) appended to, those lengths will be null if the path did not resolve ot a string.</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.strappend"/></remarks>
    long?[] StrAppend(RedisKey key, string value, string? path = null);

    /// <summary>
    /// Check's the length of the string(s) at the provided path.
    /// </summary>
    /// <param name="key">The key of the json object.</param>
    /// <param name="path">The path of the string(s) within the json object.</param>
    /// <returns>The length of the string(s) appended to, those lengths will be null if the path did not resolve ot a string.</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.strlen"/></remarks>
    public long?[] StrLen(RedisKey key, string? path = null);

    /// <summary>
    /// Toggles the boolean value(s) at the provided path.
    /// </summary>
    /// <param name="key">The key of the json object.</param>
    /// <param name="path">The path of the value(s) to toggle.</param>
    /// <returns>the new value(s). Which will be null if the path did not resolve to a boolean.</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.toggle"/></remarks>
    bool?[] Toggle(RedisKey key, string? path = null);

    /// <summary>
    /// Gets the type(s) of the item(s) at the provided json path.
    /// </summary>
    /// <param name="key">The key of the JSON object.</param>
    /// <param name="path">The path to resolve.</param>
    /// <returns>An array of types.</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.type"/></remarks>
    JsonType[] Type(RedisKey key, string? path = null);

    /// <summary>
    /// Report a value's memory usage in bytes. path defaults to root if not provided.
    /// </summary>
    /// <param name="key">The object's key</param>
    /// <param name="path">The path within the object.</param>
    /// <returns>the value's size in bytes.</returns>
    long DebugMemory(string key, string? path = null);

    /// <summary>
    /// Appends the provided items to the array at the provided path.
    /// </summary>
    /// <param name="key">The key to append to</param>
    /// <param name="path">The path to append to</param>
    /// <param name="values">the values to append</param>
    /// <returns>The new array sizes for the appended paths</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.arrappend"/></remarks>
    Task<long?[]> ArrAppendAsync(RedisKey key, string? path = null, params object[] values);

    /// <summary>
    /// Finds the index of the provided item within the provided range
    /// </summary>
    /// <param name="key">The key to look up.</param>
    /// <param name="path">The json path.</param>
    /// <param name="value">The value to find the index of.</param>
    /// <param name="start">The starting index within the array. Inclusive.</param>
    /// <param name="stop">The ending index within the array. Exclusive</param>
    /// <returns>The index of the value for each array the path resolved to.</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.arrindex"/></remarks>
    Task<long?[]> ArrIndexAsync(RedisKey key, string path, object value, long? start = null, long? stop = null);

    /// <summary>
    /// Inserts the provided items at the provided index within a json array.
    /// </summary>
    /// <param name="key">The key to insert into.</param>
    /// <param name="path">The path of the array(s) within the key to insert into.</param>
    /// <param name="index">The index to insert at.</param>
    /// <param name="values">The values to insert</param>
    /// <returns>The new size of each array the item was inserted into.</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.arrinsert"/></remarks>
    Task<long?[]> ArrInsertAsync(RedisKey key, string path, long index, params object[] values);

    /// <summary>
    /// Gets the length of the arrays resolved by the provided path.
    /// </summary>
    /// <param name="key">The key of the json object.</param>
    /// <param name="path">The path to the array(s)</param>
    /// <returns>The length of each array resolved by the json path.</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.arrlen"/></remarks>
    Task<long?[]> ArrLenAsync(RedisKey key, string? path = null);

    /// <summary>
    /// Pops an item from the array(s) at the provided index. Or the last element if no index is provided.
    /// </summary>
    /// <param name="key">The json key to use.</param>
    /// <param name="path">The path of the array(s).</param>
    /// <param name="index">The index to pop from</param>
    /// <returns>The items popped from the array</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.arrpop"/></remarks>
    Task<RedisResult[]> ArrPopAsync(RedisKey key, string? path = null, long? index = null);

    /// <summary>
    /// Trims the array(s) at the provided path, leaving the range between the specified indexes (inclusive).
    /// </summary>
    /// <param name="key">The key to trim from.</param>
    /// <param name="path">The path of the array(s) within the json object to trim.</param>
    /// <param name="start">the starting index to retain.</param>
    /// <param name="stop">The ending index to retain.</param>
    /// <returns>The new length of the array(s) after they're trimmed.</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.arrtrim"/></remarks>
    Task<long?[]> ArrTrimAsync(RedisKey key, string path, long start, long stop);

    /// <summary>
    /// Clear's container values(arrays/objects), and sets numeric values to 0.
    /// </summary>
    /// <param name="key">The key to clear.</param>
    /// <param name="path">The path to clear.</param>
    /// <returns>number of values cleared</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.clear"/></remarks>
    Task<long> ClearAsync(RedisKey key, string? path = null);

    /// <summary>
    /// Deletes a json value.
    /// </summary>
    /// <param name="key">The key to delete from.</param>
    /// <param name="path">The path to delete.</param>
    /// <returns>number of path's deleted</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.del"/></remarks>
    Task<long> DelAsync(RedisKey key, string? path = null);

    /// <summary>
    /// Deletes a json value.
    /// </summary>
    /// <param name="key">The key to delete from.</param>
    /// <param name="path">The path to delete.</param>
    /// <returns>number of path's deleted</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.forget"/></remarks>
    Task<long> ForgetAsync(RedisKey key, string? path = null);

    /// <summary>
    /// Gets the value stored at the key and path in redis.
    /// </summary>
    /// <param name="key">The key to retrieve.</param>
    /// <param name="indent">the indentation string for nested levels</param>
    /// <param name="newLine">sets the string that's printed at the end of each line</param>
    /// <param name="space">sets the string that's put between a key and a value</param>
    /// <param name="path">the path to get.</param>
    /// <returns>The requested Items</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.get"/></remarks>
    Task<RedisResult> GetAsync(RedisKey key, RedisValue? indent = null, RedisValue? newLine = null, RedisValue? space = null, RedisValue? path = null);

    /// <summary>
    /// Gets the values stored at the provided paths in redis.
    /// </summary>
    /// <param name="key">The key to pull from.</param>
    /// <param name="paths">The paths within the key to pull.</param>
    /// <param name="indent">the indentation string for nested levels</param>
    /// <param name="newLine">sets the string that's printed at the end of each line</param>
    /// <param name="space">sets the string that's put between a key and a value</param>
    /// <returns></returns>
    Task<RedisResult> GetAsync(RedisKey key, string[] paths, RedisValue? indent = null, RedisValue? newLine = null, RedisValue? space = null);

    /// <summary>
    /// Generically gets an Item stored in Redis.
    /// </summary>
    /// <param name="key">The key to retrieve</param>
    /// <param name="path">The path to retrieve</param>
    /// <typeparam name="T">The type retrieved</typeparam>
    /// <returns>The object requested</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.get"/></remarks>
    Task<T?> GetAsync<T>(RedisKey key, string path = "$");

    /// <summary>
    /// retrieves a group of items stored in redis, appropriate if the path will  resolve to multiple records.
    /// </summary>
    /// <param name="key">The key to pull from.</param>
    /// <param name="path">The path to pull.</param>
    /// <typeparam name="T">The type.</typeparam>
    /// <returns>An enumerable of the requested tyep</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.get"/></remarks>
    Task<IEnumerable<T?>> GetEnumerableAsync<T>(RedisKey key, string path = "$");

    /// <summary>
    /// Gets the provided path from multiple keys
    /// </summary>
    /// <param name="keys">The keys to retrieve from.</param>
    /// <param name="path">The path to retrieve</param>
    /// <returns>An array of RedisResults with the requested data.</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.mget"/></remarks>
    Task<RedisResult[]> MGetAsync(RedisKey[] keys, string path);

    /// <summary>
    /// Increments the fields at the provided path by the provided number.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="path">The path to increment.</param>
    /// <param name="value">The value to increment by.</param>
    /// <returns>The new values after being incremented, or null if the path resolved a non-numeric.</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.numincrby"/></remarks>
    Task<double?[]> NumIncrbyAsync(RedisKey key, string path, double value);

    /// <summary>
    /// Gets the keys of the object at the provided path.
    /// </summary>
    /// <param name="key">the key of the json object.</param>
    /// <param name="path">The path of the object(s)</param>
    /// <returns>the keys of the resolved object(s)</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.objkeys"/></remarks>
    Task<IEnumerable<HashSet<string>>> ObjKeysAsync(RedisKey key, string? path = null);

    /// <summary>
    /// returns the number of keys in the object(s) at the provided path.
    /// </summary>
    /// <param name="key">The key of the json object.</param>
    /// <param name="path">The path of the object(s) to resolve.</param>
    /// <returns>The length of the object(s) keyspace.</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.objlen"/></remarks>
    Task<long?[]> ObjLenAsync(RedisKey key, string? path = null);

    /// <summary>
    /// Gets the key in RESP(Redis Serialization Protocol) form.
    /// </summary>
    /// <param name="key">The key to get.</param>
    /// <param name="path">Path within the key to get.</param>
    /// <returns>the resultant resp</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.resp"/></remarks>
    Task<RedisResult[]> RespAsync(RedisKey key, string? path = null);

    /// <summary>
    /// Set's the key/path to the provided value.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="path">The path to set within the key.</param>
    /// <param name="obj">The value to set.</param>
    /// <param name="when">When to set the value.</param>
    /// <returns>The disposition of the command</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.set"/></remarks>
    Task<bool> SetAsync(RedisKey key, RedisValue path, object obj, When when = When.Always);

    /// <summary>
    /// Set's the key/path to the provided value.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="path">The path to set within the key.</param>
    /// <param name="json">The value to set.</param>
    /// <param name="when">When to set the value.</param>
    /// <returns>The disposition of the command</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.set"/></remarks>
    Task<bool> SetAsync(RedisKey key, RedisValue path, RedisValue json, When when = When.Always);

    /// <summary>
    /// Set json file from the provided file Path.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="path">The path to set within the key.</param>
    /// <param name="filePath">The path of the file to set.</param>
    /// <param name="when">When to set the value.</param>
    /// <returns>The disposition of the command</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.set"/></remarks>
    Task<bool> SetFileAsync(RedisKey key, RedisValue path, string filePath, When when = When.Always);

    /// <summary>
    /// Appends the provided string to the string(s) at the provided path.
    /// </summary>
    /// <param name="key">The key to append to.</param>
    /// <param name="path">The path of the string(s) to append to.</param>
    /// <param name="value">The value to append.</param>
    /// <returns>The new length of the string(s) appended to, those lengths will be null if the path did not resolve ot a string.</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.strappend"/></remarks>
    Task<long?[]> StrAppendAsync(RedisKey key, string value, string? path = null);

    /// <summary>
    /// Check's the length of the string(s) at the provided path.
    /// </summary>
    /// <param name="key">The key of the json object.</param>
    /// <param name="path">The path of the string(s) within the json object.</param>
    /// <returns>The length of the string(s) appended to, those lengths will be null if the path did not resolve ot a string.</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.strlen"/></remarks>
    Task<long?[]> StrLenAsync(RedisKey key, string? path = null);

    /// <summary>
    /// Toggles the boolean value(s) at the provided path.
    /// </summary>
    /// <param name="key">The key of the json object.</param>
    /// <param name="path">The path of the value(s) to toggle.</param>
    /// <returns>the new value(s). Which will be null if the path did not resolve to a boolean.</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.toggle"/></remarks>
    Task<bool?[]> ToggleAsync(RedisKey key, string? path = null);

    /// <summary>
    /// Gets the type(s) of the item(s) at the provided json path.
    /// </summary>
    /// <param name="key">The key of the JSON object.</param>
    /// <param name="path">The path to resolve.</param>
    /// <returns>An array of types.</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.type"/></remarks>
    Task<JsonType[]> TypeAsync(RedisKey key, string? path = null);

    /// <summary>
    /// Report a value's memory usage in bytes. path defaults to root if not provided.
    /// </summary>
    /// <param name="key">The object's key</param>
    /// <param name="path">The path within the object.</param>
    /// <returns>the value's size in bytes.</returns>
    Task<long> DebugMemoryAsync(string key, string? path = null);
}