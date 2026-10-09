using StackExchange.Redis;

namespace NRedisStack.Downlevel;

/// <summary>
/// The command groups as <b>methods</b>, for compilers older than C# 14.
/// </summary>
/// <remarks>
/// <para>
/// <c>db.CountMinSketch</c> is an extension <i>property</i>, which needs C# 14; a consumer on an older language
/// version (the .NET 8 SDK's default is C# 12, and netstandard2.0 / .NET Framework projects often sit lower)
/// cannot see extension-block members at all. The commands themselves are ordinary extension methods and bind
/// anywhere, so only the group accessors are out of reach - and <c>db.CountMinSketch().InitByDimAsync(...)</c>
/// puts them back.
/// </para>
/// <para>
/// <b>Do not import this namespace on C# 14 or later.</b> There the property is visible, and having both in
/// scope makes <c>db.CountMinSketch</c> ambiguous (<c>CS9339</c>). That is a compile error rather than a silent
/// misbind, which is why this is a separate namespace you opt into. Mirrors <c>StackExchange.Redis.Downlevel.RespGroups</c>.
/// </para>
/// </remarks>
public static class RedisStackGroups
{
    /// <summary>The Count-Min Sketch commands.</summary>
    public static RespCountMinSketch CountMinSketch<TTarget>(this TTarget target) where TTarget : IRespKeyspaceTarget
        => new((RespContext)target.Context);
}
