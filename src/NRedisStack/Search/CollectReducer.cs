using NRedisStack.Search.Literals;

namespace NRedisStack.Search.Aggregation;

/// <summary>
/// Reducer for <c>REDUCE COLLECT</c>, which gathers per-document projections within a <c>GROUPBY</c> group and returns them
/// as an array of per-entry maps under the reducer alias, optionally sorted and bounded.
/// <para>
/// The grammar implemented by this reducer is:
/// <code>
/// REDUCE COLLECT &lt;narg&gt;
///     FIELDS ( * | &lt;num_fields&gt; &lt;@field&gt; [&lt;@field&gt; ...] )
///     [SORTBY &lt;narg&gt; &lt;@field&gt; [ASC|DESC] [&lt;@field&gt; [ASC|DESC] ...]]
///     [LIMIT &lt;offset&gt; &lt;count&gt;]
///   [AS &lt;alias&gt;]
/// </code>
/// </para>
/// <para>
/// Field and sort-key names are referenced with an <c>@</c> prefix on the wire (this builder adds it automatically when it is
/// missing). The output map keys are the bare names. <c>FIELDS *</c> projects whatever the pipeline has materialized at the
/// <c>COLLECT</c> stage; it does not implicitly fetch the full document.
/// </para>
/// <para>
/// Configure the reducer fully before attaching it to a <c>GROUPBY</c>: <see cref="AggregationRequest.GroupBy(string, Reducer[])"/>
/// serializes the reducer immediately, so builder calls made after that point cannot reach the wire and throw
/// <see cref="InvalidOperationException"/>.
/// </para>
/// <para>
/// <b>Experimental.</b> Both the underlying Redis Search feature and this API may change. <c>COLLECT</c> is gated behind
/// <c>search-enable-unstable-features</c>; enable it on the server (for example via
/// <c>CONFIG SET search-enable-unstable-features yes</c>) before issuing aggregations that use this reducer, otherwise the
/// server replies with an error.
/// </para>
/// </summary>
/// <seealso cref="Reducers.Collect()"/>
public sealed class CollectReducer : Reducer
{
    private bool _allFields = false;
    private readonly List<string> _fields = new List<string>();
    private readonly List<SortedField> _sortFields = new List<SortedField>();
    private int? _limitOffset;
    private int? _limitCount;
    private bool _serialized;

    internal CollectReducer() : base(null) { }

    public override string Name => "COLLECT";

    /// <summary>
    /// Project the named fields for every document in the group. Names may be supplied with or without a leading <c>@</c>;
    /// the builder normalizes each to a single <c>@&lt;name&gt;</c> on the wire. Use <c>@__key</c> or ordinary document field
    /// names. Mutually exclusive with <see cref="FieldsAll"/>; may be called multiple times to append further fields.
    /// </summary>
    public CollectReducer Fields(params string[] fields)
    {
        EnsureMutable();
        if (_allFields)
            throw new InvalidOperationException("REDUCE COLLECT cannot mix FIELDS * with explicit field names");
        _fields.AddRange(fields);
        return this;
    }

    /// <summary>
    /// Project every field present in the pipeline at the <c>COLLECT</c> stage (<c>FIELDS *</c>). Per the COLLECT
    /// specification, <c>*</c> does not trigger an implicit load — fields must already be in the pipeline (typically via
    /// <c>LOAD *</c> or because they are grouping keys / reducer aliases). Mutually exclusive with <see cref="Fields"/>.
    /// </summary>
    public CollectReducer FieldsAll()
    {
        EnsureMutable();
        if (_fields.Count > 0)
            throw new InvalidOperationException("REDUCE COLLECT cannot mix FIELDS * with explicit field names");
        _allFields = true;
        return this;
    }

    /// <summary>
    /// In-group sort by one or more fields. May be called multiple times to append further sort keys.
    /// </summary>
    public CollectReducer SortBy(params SortedField[] fields)
    {
        EnsureMutable();
        _sortFields.AddRange(fields);
        return this;
    }

    /// <summary>Convenience for <c>SortBy(SortedField.Asc(field))</c>.</summary>
    public CollectReducer SortByAsc(string field) => SortBy(SortedField.Asc(field));

    /// <summary>Convenience for <c>SortBy(SortedField.Desc(field))</c>.</summary>
    public CollectReducer SortByDesc(string field) => SortBy(SortedField.Desc(field));

    /// <summary>Bound the output per group to the first <paramref name="count"/> entries (offset 0).</summary>
    public CollectReducer Limit(int count) => Limit(0, count);

    /// <summary>Bound the output per group to <paramref name="count"/> entries starting at <paramref name="offset"/>.</summary>
    public CollectReducer Limit(int offset, int count)
    {
        EnsureMutable();
        if (offset < 0 || count < 0)
            throw new ArgumentException("LIMIT offset and count must be non-negative");
        _limitOffset = offset;
        _limitCount = count;
        return this;
    }

    protected override int GetOwnArgsCount()
    {
        if (!_allFields && _fields.Count == 0)
            throw new InvalidOperationException(
                "REDUCE COLLECT requires either Fields(...) or FieldsAll() to be configured");

        int count = _allFields ? 2 : 2 + _fields.Count;
        if (_sortFields.Count > 0)
            count += 2 + _sortFields.Count * 2; // SORTBY <narg> then a @field/ASC|DESC pair per key
        if (_limitOffset != null)
            count += 3; // LIMIT <offset> <count>
        return count;
    }

    protected override void AddOwnArgs(List<object> args)
    {
        // GroupBy serializes the reducer eagerly; once the tokens are emitted, later builder
        // calls could never reach the wire, so reject them instead of silently ignoring them.
        _serialized = true;

        args.Add(SearchArgs.FIELDS);
        if (_allFields)
        {
            args.Add("*");
        }
        else
        {
            args.Add(_fields.Count);
            foreach (var field in _fields)
                args.Add(WithAtPrefix(field));
        }

        if (_sortFields.Count > 0)
        {
            args.Add(SearchArgs.SORTBY);
            args.Add(_sortFields.Count * 2);
            foreach (var sortField in _sortFields)
            {
                args.Add(WithAtPrefix(sortField.FieldName));
                args.Add(sortField.Order.ToString());
            }
        }

        if (_limitOffset != null)
        {
            args.Add(SearchArgs.LIMIT);
            args.Add(_limitOffset.Value);
            args.Add(_limitCount!.Value);
        }
    }

    private void EnsureMutable()
    {
        if (_serialized)
            throw new InvalidOperationException(
                "REDUCE COLLECT cannot be modified after it has been serialized into an aggregation request; " +
                "configure Fields/SortBy/Limit before passing the reducer to GroupBy");
    }

    private static string WithAtPrefix(string name) => name.StartsWith("@") ? name : "@" + name;
}
