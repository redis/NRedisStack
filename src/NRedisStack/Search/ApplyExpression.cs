using System.Diagnostics.CodeAnalysis;

namespace NRedisStack.Search;

/// <summary>
/// Represents an APPLY expression in an aggregation query.
/// </summary>
/// <param name="expression">The expression to apply.</param>
/// <param name="alias">The alias for the expression in the results.</param>
[Experimental(Experiments.Server_8_4, UrlFormat = Experiments.UrlFormat)]
public readonly struct ApplyExpression(string expression, string? alias = null)
{
    public string Expression { get; } = expression;
    public string? Alias { get; } = alias;
    public override string ToString() => Expression;
    public override int GetHashCode() => (Expression?.GetHashCode() ?? 0) ^ (Alias?.GetHashCode() ?? 0);

    public override bool Equals(object? obj) => obj is ApplyExpression other &&
                                                (Expression == other.Expression &&
                                                 Alias == other.Alias);

    public static implicit operator ApplyExpression(string expression) => new(expression);
}