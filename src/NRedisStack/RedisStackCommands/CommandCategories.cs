using StackExchange.Redis;

namespace NRedisStack.RedisStackCommands;

/// <summary>
/// Command side-effect categories, used to tell StackExchange.Redis whether (and how aggressively) a
/// command may be replayed by <c>WithRetry</c>.
/// </summary>
/// <remarks>
/// <para>
/// The rungs are aliases for the <c>CommandFlags.CommandRetry*</c> members, so that the categories carry
/// names that describe the command rather than the retry mechanism, and so that the documentation below has
/// somewhere to live. <c>ServerSpecific</c> is honoured by StackExchange.Redis but is not named on its
/// public enum, so that one is still a bit.
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
    internal const CommandFlags Always = CommandFlags.CommandRetryAlways;

    /// <summary>Connection-level metadata, e.g. <c>CLIENT SETINFO</c>.</summary>
    internal const CommandFlags Connection = CommandFlags.CommandRetryConnection;

    /// <summary>Pure read; replay observes state but does not change it.</summary>
    internal const CommandFlags ReadOnly = CommandFlags.CommandRetryReadOnly;

    /// <summary>Conditional write; a replay is checked against server state and rejected.</summary>
    internal const CommandFlags WriteChecked = CommandFlags.CommandRetryWriteChecked;

    /// <summary>Unconditional overwrite; a replay lands on the same value (last-writer-wins).</summary>
    internal const CommandFlags WriteLastWins = CommandFlags.CommandRetryWriteLastWins;

    /// <summary>Cumulative write; a replay double-applies and changes the result.</summary>
    /// <remarks>
    /// Also carries a second, less obvious class of command: one whose replay leaves state *correct* but
    /// returns an error, because the server rejects the repeat ("Index already exists", "key already
    /// exists", "compaction rule does not exist"). Most module DDL behaves this way. That is not literally
    /// cumulative, but the ladder has no rung for "idempotent, yet errors on replay", and the alternatives
    /// are worse: <see cref="WriteChecked"/> and <see cref="WriteLastWins"/> both sit at or below the
    /// default policy ceiling, so a lost reply would be replayed *by default* and report failure for an
    /// operation that actually succeeded. Sitting here keeps that closed unless the caller deliberately
    /// raises the ceiling, while still allowing the known-never-sent replay (see <see cref="Never"/>),
    /// which is the case that matters for riding out <c>-LOADING</c> during setup.
    /// </remarks>
    internal const CommandFlags WriteAccumulating = CommandFlags.CommandRetryWriteAccumulating;

    /// <summary>Server administration, e.g. <c>FT.CONFIG SET</c>.</summary>
    internal const CommandFlags ServerAdmin = CommandFlags.CommandRetryServerAdmin;

    /// <summary>Never replay, under any policy.</summary>
    /// <remarks>
    /// The only category a retry policy cannot override. StackExchange.Redis tests for it before both the
    /// <c>MaxCommandRetryCategory</c> comparison and the known-never-sent (<c>NotApplied</c>) bypass, so
    /// unlike the rungs below it this holds even when a caller raises the ceiling - which they may do as
    /// far as <c>Never</c> itself. Use it where a replay would corrupt state or report a spurious error
    /// even though the original attempt succeeded, and where forgoing retry entirely is the lesser cost.
    /// </remarks>
    internal const CommandFlags Never = CommandFlags.CommandRetryNever;

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
    /// The rung bits (13-17): the severity ladder on its own, without <see cref="ServerSpecific"/>. A
    /// category is only meaningful if it names a rung, since <see cref="ServerSpecific"/> alone leaves
    /// StackExchange.Redis to substitute a default rung.
    /// </summary>
    internal const CommandFlags LadderMask = Never;

    /// <summary>
    /// The bits a caller-supplied category may occupy: the retry-category region (13-17) plus
    /// <see cref="ServerSpecific"/> (18). Anything else is masked off before dispatch, so a stray
    /// <c>FireAndForget</c> or replica preference cannot leak in through the category parameter.
    /// </summary>
    internal const CommandFlags Mask = LadderMask | ServerSpecific;
}
