using StackExchange.Redis;

namespace NRedisStack.Search;

/// <summary>
/// Document represents a single indexed document or entity in the engine
/// </summary>
public class Document
{
    public string Id { get; }
    public double Score { get; set; }
    public byte[]? Payload { get; }
    public string[]? ScoreExplained { get; private set; } // TODO: check if this is needed (Jedis does not have it)
    internal readonly Dictionary<string, RedisValue> _properties;
    public Document(string id, double score, byte[]? payload) : this(id, null, score, payload) { }
    public Document(string id) : this(id, null, 1.0, null) { }

    public Document(string id, Dictionary<string, RedisValue> fields, double score = 1.0) : this(id, fields, score, null) { }

    public Document(string id, Dictionary<string, RedisValue>? fields, double score, byte[]? payload)
    {
        Id = id;
        _properties = fields ?? new Dictionary<string, RedisValue>();
        Score = score;
        Payload = payload;
    }

    public IEnumerable<KeyValuePair<string, RedisValue>> GetProperties() => _properties;

    public static Document Load(string id, double score, byte[]? payload, RedisValue[]? fields)
    {
        Document ret = new(id, score, payload);
        if (fields == null) return ret;
        if (fields.Length == 1 && fields[0].IsNull)
        {
            return ret;
        }
        for (int i = 0; i < fields.Length; i += 2)
        {
            string fieldName = fields[i]!;
            if (fieldName == "$")
            {
                ret["json"] = fields[i + 1];
            }
            else
            {
                ret[fieldName] = fields[i + 1];
            }
        }
        return ret;
    }

    internal static Document Load(RedisResult src) // used from HybridSearch
    {
        // ReSharper disable once ConditionIsAlwaysTrueOrFalseAccordingToNullableAPIContract
        if (src is null || src.IsNull || src.Length < 0) return null!;

        var fields = src.ToArray();
        string id = "";
        double score = double.NaN;
        var fieldCount = fields.Length / 2;
        for (int i = 0; i < fieldCount; i++)
        {
            var key = fields[2 * i];
            if (key.Resp2Type == ResultType.BulkString && !key.IsNull)
            {
                var blob = (byte[])key!;
                switch (blob.Length)
                {
                    case 5 when "__key"u8.SequenceEqual(blob):
                        id = fields[(2 * i) + 1].ToString();
                        break;
                    case 7 when "__score"u8.SequenceEqual(blob):
                        score = (double)fields[(2 * i) + 1];
                        break;
                }
            }
        }
        Document doc = new(id, score, null);
        for (int i = 0; i < fieldCount; i++)
        {
            var key = fields[2 * i];
            if (key.Resp2Type == ResultType.BulkString && !key.IsNull)
            {
                var blob = (byte[])key!;
                switch (blob.Length)
                {
                    case 5 when "__key"u8.SequenceEqual(blob):
                    case 7 when "__score"u8.SequenceEqual(blob):
                        break; // skip, already parsed
                    default:
                        doc[key.ToString()] = (RedisValue)fields[(2 * i) + 1];
                        break;
                }
            }
        }

        return doc;
    }

    public static Document Load(string id, double score, byte[]? payload, RedisValue[]? fields, string[]? scoreExplained)
    {
        Document ret = Load(id, score, payload, fields);
        if (scoreExplained != null)
        {
            ret.ScoreExplained = scoreExplained;
        }
        return ret;
    }

    public RedisValue this[string key]
    {
        get => _properties.TryGetValue(key, out var val) ? val : default(RedisValue);
        internal set => _properties[key] = value;
    }

    public Document Set(string field, RedisValue value)
    {
        this[field] = value;
        return this;
    }

    public Document SetScore(double score)
    {
        Score = score;
        return this;
    }
}