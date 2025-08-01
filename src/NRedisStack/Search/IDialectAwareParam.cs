namespace NRedisStack.Search;

/// <summary>
/// Interface for dialect-aware parameters.
/// To provide a single interface to manage default dialect version under which to execute the query.
/// </summary>
internal interface IDialectAwareParam
{
    /// <summary>
    ///  Selects the dialect version under which to execute the query.
    /// </summary>
    internal int? Dialect { get; set; }
}