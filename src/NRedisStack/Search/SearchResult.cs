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

    internal SearchResult(RedisResult[] resp, bool hasContent, bool hasScores, bool hasPayloads/*, bool shouldExplainScore*/)
    {
        // Calculate the step distance to walk over the results.
        // The order of results is id, score (if withScore), payLoad (if hasPayloads), fields
        int stride = 1;
        if (hasScores) stride++;
        if (hasPayloads) stride++;
        if (hasContent) stride++;

        // the first element is always the number of results
        if (resp is not { Length: > 0 })
        {
            // unexpected empty case
            TotalResults = 0;
            Documents = [];
            Debug.Assert(false, "Empty result from FT.SEARCH"); // debug only, flag as a problem 
        }
        else
        {
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
                    // if (shouldExplainScore)
                    // {
                    //     var scoreResult = (RedisResult[])resp[offset++];
                    //     score = (double) scoreResult[0];
                    //     var redisResultsScoreExplained = (RedisResult[]) scoreResult[1];
                    //     scoreExplained = FlatRedisResultArray(redisResultsScoreExplained).ToArray();
                    // }
                    //else
                    //{
                    score = (double)resp[offset++];
                    //}
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
    }
}