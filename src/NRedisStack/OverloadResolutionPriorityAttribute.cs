// ReSharper disable once CheckNamespace
namespace System.Runtime.CompilerServices;

#if !NET9_0_OR_GREATER
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
internal sealed class OverloadResolutionPriorityAttribute(int priority) : Attribute
{
    public int Priority { get; } = priority;
}
#endif