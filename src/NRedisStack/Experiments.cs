namespace NRedisStack
{
    // [Experimental(Experiments.SomeFeature, UrlFormat = Experiments.UrlFormat)]
    // where SomeFeature has the next label, for example "NRS042", and /docs/exp/NRS042.md exists.
    //
    // Retired diagnostic ids (features graduated out of [Experimental]; the /docs/exp/ entries are kept
    // for history). Do not reuse these ids:
    //   NRS001 - Redis 8.4 hybrid search
    //   NRS002 - Redis 8.8 multi-aggregate time-series
    internal static class Experiments
    {
        // {0} is substituted with the diagnostic id, e.g. NRS042 -> https://redis.github.io/NRedisStack/exp/NRS042
        public const string UrlFormat = "https://redis.github.io/NRedisStack/exp/{0}";
    }
}

#if !NET8_0_OR_GREATER
#pragma warning disable SA1403
namespace System.Diagnostics.CodeAnalysis
#pragma warning restore SA1403
{
    [AttributeUsage(
        AttributeTargets.Assembly |
        AttributeTargets.Module |
        AttributeTargets.Class |
        AttributeTargets.Struct |
        AttributeTargets.Enum |
        AttributeTargets.Constructor |
        AttributeTargets.Method |
        AttributeTargets.Property |
        AttributeTargets.Field |
        AttributeTargets.Event |
        AttributeTargets.Interface |
        AttributeTargets.Delegate,
        Inherited = false)]
    internal sealed class ExperimentalAttribute(string diagnosticId) : Attribute
    {
        public string DiagnosticId { get; } = diagnosticId;
        public string? UrlFormat { get; set; }
        public string? Message { get; set; }
    }
}
#endif
