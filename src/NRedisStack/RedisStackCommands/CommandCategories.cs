using StackExchange.Redis;

namespace NRedisStack.RedisStackCommands;

/// <summary>
/// Command side-effect categories, used to tell StackExchange.Redis whether (and how aggressively) a
/// command may be replayed by <c>WithRetry</c>.
/// </summary>
/// <remarks>
/// <para>
/// These mirror the <c>CommandFlags.CommandRetry*</c> members added in StackExchange.Redis 3.1.0, but are
/// declared here as casts so that NRedisStack can keep a 3.0.x floor: on older versions the bits fall
/// outside the library's user-selectable mask and are silently discarded, which reproduces today's
/// behaviour exactly. <c>ServerSpecific</c> is accepted by 3.1.0 but is not named on its public enum.
/// </para>
/// <para>
/// The ladder runs from least to most side-effecting; a retry policy specifies the most side-effecting
/// category it is willing to replay, so the numeric ordering is load-bearing. <c>ServerSpecific</c> is a
/// single orthogonal bit, combinable with any rung.
/// </para>
/// </remarks>
internal static class CommandCategories
{
    /// <summary>Always safe to replay, regardless of connection or server state.</summary>
    internal const CommandFlags Always = (CommandFlags)(1 << 13);

    /// <summary>Connection-level metadata, e.g. <c>CLIENT SETINFO</c>.</summary>
    internal const CommandFlags Connection = (CommandFlags)(4 << 13);

    /// <summary>Pure read; replay observes state but does not change it.</summary>
    internal const CommandFlags ReadOnly = (CommandFlags)(8 << 13);

    /// <summary>Conditional write; a replay is checked against server state and rejected.</summary>
    internal const CommandFlags WriteChecked = (CommandFlags)(12 << 13);

    /// <summary>Unconditional overwrite; a replay lands on the same value (last-writer-wins).</summary>
    internal const CommandFlags WriteLastWins = (CommandFlags)(16 << 13);

    /// <summary>Cumulative write; a replay double-applies and changes the result.</summary>
    internal const CommandFlags WriteAccumulating = (CommandFlags)(20 << 13);

    /// <summary>Server administration, e.g. <c>FT.CONFIG SET</c>.</summary>
    internal const CommandFlags ServerAdmin = (CommandFlags)(24 << 13);

    /// <summary>Never replay, under any policy.</summary>
    internal const CommandFlags Never = (CommandFlags)(31 << 13);

    /// <summary>
    /// The command is bound to a particular endpoint (typically because it carries a server-side cursor
    /// or iterator, or reads node-local state), so it must not be replayed against a different one.
    /// Combine with the rung describing the command's own side-effects.
    /// </summary>
    /// <remarks>
    /// This narrows rather than vetoes: StackExchange.Redis strips failover from the permitted retry
    /// targets but still allows a same-server retry. It is also checked *after* the category ladder, so
    /// it cannot rescue a command whose rung already exceeds the policy.
    /// </remarks>
    internal const CommandFlags ServerSpecific = (CommandFlags)(1 << 18);

    /// <summary>
    /// The bits a caller-supplied category may occupy: the retry-category region (13-17) plus
    /// <see cref="ServerSpecific"/> (18). Anything else is masked off before dispatch, so a stray
    /// <c>FireAndForget</c> or replica preference cannot leak in through the category parameter.
    /// </summary>
    internal const CommandFlags Mask = Never | ServerSpecific;
}
