using System.Diagnostics.CodeAnalysis;

namespace NRedisStack.Search;

/// <summary>
/// See https://redis.io/docs/latest/develop/ai/search-and-query/advanced-concepts/scoring/ for more details
/// </summary>
[Experimental(Experiments.Server_8_4, UrlFormat = Experiments.UrlFormat)]
public abstract class Scorer
{
    private protected Scorer()
    {
    }

    /// <inheritdoc />
    public override string ToString() => Method;

    internal abstract string Method { get; }

    /// <summary>
    /// Basic TF-IDF scoring with a few extra features,
    /// </summary>
    public static Scorer TfIdf { get; } = new SimpleScorer("TFIDF");
    
    /// <summary>
    /// Identical to the default TFIDF scorer, with one important distinction: Term frequencies are normalized by the length of the document, expressed as the total number of terms.
    /// </summary>
    public static Scorer TfIdfDocNorm { get; } = new SimpleScorer("TFIDF.DOCNORM");
    
    /// <summary>
    /// A variation on the basic TFIDF scorer.
    /// </summary>
    public static Scorer BM25Std { get; } = new SimpleScorer("BM25STD");

    /// <summary>
    /// A variation of BM25STD, where the scores are normalized by the minimum and maximum scores.
    /// </summary>
    public static Scorer BM25StdNorm { get; } = new SimpleScorer("BM25STD.NORM");

    /// <summary>
    /// A variation of BM25STD.NORM, where the scores are normalised by the linear function tanh(x).  
    /// </summary>
    /// <param name="y">used to smooth the function and the score values.</param>
    public static Scorer BM25StdTanh(int y = Bm25StdTanh.DEFAULT_Y) => Bm25StdTanh.Create(y);

    /// <summary>
    /// A simple scorer that sums up the frequencies of matched terms. In the case of union clauses, it will give the maximum value of those matches. No other penalties or factors are applied.
    /// </summary>
    public static Scorer DisMax { get; } = new SimpleScorer("DISMAX");

    /// <summary>
    /// A scoring function that just returns the presumptive score of the document without applying any calculations to it. Since document scores can be updated, this can be useful if you'd like to use an external score and nothing further.
    /// </summary>
    public static Scorer DocScore { get; } = new SimpleScorer("DOCSCORE");

    /// <summary>
    /// Scoring by the inverse Hamming distance between the document's payload and the query payload is performed.
    /// </summary>
    public static Scorer Hamming { get; } = new SimpleScorer("HAMMING");

    private sealed class Bm25StdTanh : Scorer
    {
        private readonly int _y;

        private Bm25StdTanh(int y) => _y = y;

        private static Bm25StdTanh? s_Default;
        internal const int DEFAULT_Y = 4;
        internal static Bm25StdTanh Create(int y) => y == DEFAULT_Y
            ? (s_Default ??= new Bm25StdTanh(DEFAULT_Y)) : new(y);
        internal override string Method => "BM25STD.TANH";

        /// <inheritdoc />
        public override string ToString() => $"{Method} BM25STD_TANH_FACTOR {_y}";

        internal override int GetOwnArgsCount() => 3;
        internal override void AddOwnArgs(List<object> args)
        {
            args.Add(Method);
            args.Add("BM25STD_TANH_FACTOR");
            args.Add(_y);
        }
    }

    private sealed class SimpleScorer(string method) : Scorer // no args
    {
        internal override string Method => method;
        internal override int GetOwnArgsCount() => 1;
        internal override void AddOwnArgs(List<object> args) => args.Add(method);
    }

    internal abstract int GetOwnArgsCount();

    internal abstract void AddOwnArgs(List<object> args);
}