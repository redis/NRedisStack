namespace NRedisStack
{
    // [Experimental(Experiments.SomeFeature, UrlFormat = Experiments.UrlFormat)]
    // where SomeFeature has the next label, for example "NRS042", and /docs/exp/NRS042.md exists
    internal static class Experiments
    {
        public const string UrlFormat = "https://redis.github.io/NRedisStack/exp/";

        // ReSharper disable once InconsistentNaming
        public const string Server_8_4 = "NRS001";
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