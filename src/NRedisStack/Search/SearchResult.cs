using System.Diagnostics;
using StackExchange.Redis;

namespace NRedisStack.Search;

/// <summary>
/// SearchResult encapsulates the returned result from a search query.
/// It contains publically accessible fields for the total number of results, and an array of <see cref="Document"/>
/// objects conatining the actual returned documents.
/// </summary>
public class SearchResult
{
    public long TotalResults { get; }
    public List<Document> Documents { get; }

    /// <summary>
    /// Converts the documents to a list of json strings. only works on a json documents index.
    /// </summary>
    public List<string> ToJson() => Documents.Select(x => x["json"].ToString())
        .Where(x => !string.IsNullOrEmpty(x)).ToList();

    internal SearchResult(RedisResult root, bool hasContent, bool hasScores, bool hasPayloads/*, bool shouldExplainScore*/)
    {
        if (root.Length <= 0)
        {
            // unexpected empty case
            TotalResults = 0;
            Documents = [];
            Debug.Assert(false, "Empty result from FT.SEARCH"); // debug only, flag as a problem
        }
        else if (root.Resp3Type is ResultType.Map && root.Length > 0) // RESP3
        {
            Documents = new(ResponseParser.ParseSearchResultsMap(
                root, static (attributes, values) =>
                {
                    string id = "";
                    double score = 1.0;
                    byte[]? payload = null;
                    RedisValue[]? fields = null;
                    string[]? scoreExplained = null;
                    for (int i = 0; i + 1 < attributes.Length; i += 2)
                    {
                        var key = attributes[i].ToString();
                        var value = values[i + 1];
                        switch (key)
                        {
                            case "id":
                                id = value.ToString();
                                break;
                            case "payload": // implicit "when hasPayloads"
                                payload = (byte[]?)value;
                                break;
                            case "score": // implicit "when hasScores"
                                score = ParseScore(value, out scoreExplained);
                                break;
                            case "extra_attributes": // implicit "when hasContent"
                                fields = (RedisValue[]?)value;
                                break;
                        }
                    }
                    return Document.Load(id, score, payload, fields, scoreExplained);
                }, out long totalResults));
            TotalResults = totalResults;
        }
        else // RESP2
        {
            RedisResult[] resp = (RedisResult[])root!;
            // Calculate the step distance to walk over the results.
            // The order of results is id, score (if withScore), payLoad (if hasPayloads), fields
            int stride = 1;
            if (hasScores) stride++;
            if (hasPayloads) stride++;
            if (hasContent) stride++;

            // the first element is always the number of results

            TotalResults = (long)resp[0];
            int count = checked((int)(resp.Length - 1) / stride);
            var docs = Documents = new List<Document>(count);
            int offset = 1; // skip the first element which is the number of results
            for (int docIndex = 0; docIndex < count; docIndex++)
            {
                var id = resp[offset++].ToString();
                double score = 1.0;
                byte[]? payload = null;
                RedisValue[]? fields = null;
                string[]? scoreExplained = null;
                if (hasScores)
                {
                    score = ParseScore(resp[offset++], out scoreExplained);
                }

                if (hasPayloads) // match logic from setup
                {
                    payload = (byte[]?)resp[offset++];
                }

                if (hasContent)
                {
                    fields = (RedisValue[]?)resp[offset++];
                }

                docs.Add(Document.Load(id, score, payload, fields, scoreExplained));
            }
        }
        static double ParseScore(RedisResult scoreResult, out string[]? scoreExplained)
        {
            scoreExplained = null;
            double score;
            if (scoreResult.Resp2Type is ResultType.Array) // implicit shouldExplainScore
            {
                var scoreResultArr = (RedisResult[])scoreResult!;
                score = (double)scoreResultArr[0];
                // var redisResultsScoreExplained = (RedisResult[])scoreResultArr[1];
                // scoreExplained = FlatRedisResultArray(redisResultsScoreExplained).ToArray();
            }
            else
            {
                score = (double)scoreResult;
            }
            return score;
        }
    }
}