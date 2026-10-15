using StackExchange.Redis;

namespace NRedisStack;

/// <summary>
/// The Count-Min Sketch command group: <c>db.CountMinSketch.IncrByAsync(...)</c>.
/// </summary>
/// <remarks>
/// A context plus a name, and nothing else; the commands are extension methods on this type (see
/// <see cref="CountMinSketchCommands"/>), so the surface can grow without touching the struct.
/// </remarks>
public readonly struct RespCountMinSketch
{
    /// <summary>Group the Count-Min Sketch commands of a context.</summary>
    /// <param name="context">The context to send through.</param>
    public RespCountMinSketch(RespContext context) => Context = context;

    /// <summary>The context these commands are sent through.</summary>
    internal readonly RespContext Context;
}

/// <summary>
/// The accessors that reach the module command groups: <c>db.CountMinSketch</c>, and so on.
/// </summary>
/// <remarks>
/// One partial file per group. A single generic accessor over <see cref="IRespKeyspaceTarget"/> covers
/// <see cref="IDatabase"/>, <see cref="IBatch"/>, <see cref="ITransaction"/> and <see cref="RespDatabaseContext"/>
/// itself, so a prefixed or blocking context (<c>db.Context.AppendKeyPrefix(...)</c>, <c>db.Context.Blocking()</c>)
/// reaches the group too. For a struct target the JIT specialises the call, so this costs what a dedicated
/// accessor would.
/// </remarks>
public static partial class RedisStackExtensions
{
    extension<TTarget>(TTarget target) where TTarget : IRespKeyspaceTarget
    {
        /// <summary>The Count-Min Sketch commands.</summary>
        public RespCountMinSketch CountMinSketch => new((RespContext)target.Context);
    }
}
