using NRedisStack.DataTypes;
using NRedisStack.Literals.Enums;
using NRedisStack.RedisStackCommands;
using NRedisStack.Search;
using StackExchange.Redis;
using Xunit;

namespace NRedisStack.Tests;

/// <summary>
/// Unit tests (no server) for the command side-effect categories that let NRedisStack commands take part in
/// StackExchange.Redis' <c>WithRetry</c>.
/// </summary>
public class CommandCategoryTests
{
    // NRedisStack keeps a StackExchange.Redis 3.0.x floor, so CommandCategories declares these as casts
    // rather than referencing the (3.1.0+) named members. That only stays correct while the numbers agree,
    // and a silent renumbering upstream would silently re-categorize every command we issue - so pin it.
    // The test project deliberately overrides to 3.1.0 so the named members are available here.
    [Theory]
    [InlineData(CommandCategories.Always, CommandFlags.CommandRetryAlways)]
    [InlineData(CommandCategories.Connection, CommandFlags.CommandRetryConnection)]
    [InlineData(CommandCategories.ReadOnly, CommandFlags.CommandRetryReadOnly)]
    [InlineData(CommandCategories.WriteChecked, CommandFlags.CommandRetryWriteChecked)]
    [InlineData(CommandCategories.WriteLastWins, CommandFlags.CommandRetryWriteLastWins)]
    [InlineData(CommandCategories.WriteAccumulating, CommandFlags.CommandRetryWriteAccumulating)]
    [InlineData(CommandCategories.ServerAdmin, CommandFlags.CommandRetryServerAdmin)]
    [InlineData(CommandCategories.Never, CommandFlags.CommandRetryNever)]
    public void CategoryMatchesStackExchangeRedis(CommandFlags ours, CommandFlags theirs)
        => Assert.Equal(theirs, ours);

    // Message.CommandServerSpecific is internal to StackExchange.Redis, so there is no named member to
    // compare against; assert the bit position instead, which is what their mask actually tests.
    [Fact]
    public void ServerSpecificIsBit18() => Assert.Equal(1 << 18, (int)CommandCategories.ServerSpecific);

    [Fact]
    public void MaskCoversTheCategoryRegionAndServerSpecificOnly()
        => Assert.Equal((31 << 13) | (1 << 18), (int)CommandCategories.Mask);

    // the category parameter is typed as CommandFlags for convenience, but it is not a general-purpose
    // flags channel: anything outside the category region is dropped rather than silently altering routing
    [Theory]
    [InlineData(CommandFlags.FireAndForget)]
    [InlineData(CommandFlags.DemandReplica)]
    [InlineData(CommandFlags.NoScriptCache)]
    public void UnrelatedFlagsCannotLeakThroughTheCategory(CommandFlags smuggled)
    {
        var command = new SerializedCommand(CommandCategories.ReadOnly | smuggled, "FT.SEARCH", "idx", "*");
        Assert.Equal(CommandCategories.ReadOnly, command.CommandCategory);
    }

    [Fact]
    public void UncategorizedCommandsReportNone()
    {
#pragma warning disable CS0618 // exercising the obsolete, uncategorized ctor is the point
        var command = new SerializedCommand("FT.SEARCH", "idx", "*");
#pragma warning restore CS0618
        Assert.Equal(CommandFlags.None, command.CommandCategory);
    }

    // Pins the categorizations that are argument-dependent, i.e. the ones a reader is most likely to get
    // wrong when editing the builders. The uniform cases are enforced by the compiler instead: the
    // uncategorized SerializedCommand ctors are [Obsolete] and CS0618 is an error inside the library.
    public static TheoryData<string, CommandFlags, SerializedCommand> ArgumentDependentCases() => new()
    {
        // JSON.SET is a blind overwrite unless NX/XX make it conditional
        { "JSON.SET", CommandCategories.WriteLastWins, JsonCommandBuilder.Set("k", "$", "1") },
        { "JSON.SET NX", CommandCategories.WriteChecked, JsonCommandBuilder.Set("k", "$", "1", When.NotExists, JsonNumericArrayStorage.NotSpecified) },
        { "JSON.SET XX", CommandCategories.WriteChecked, JsonCommandBuilder.Set("k", "$", "1", When.Exists, JsonNumericArrayStorage.NotSpecified) },

        // FT.SUGADD INCR adds to the existing score; without it the score is simply set
        { "FT.SUGADD", CommandCategories.WriteLastWins, SearchCommandBuilder.SugAdd("k", "s", 1d) },
        { "FT.SUGADD INCR", CommandCategories.WriteAccumulating, SearchCommandBuilder.SugAdd("k", "s", 1d, increment: true) },

        // a cursored aggregate leaves state on the serving node, so it must not fail over
        { "FT.AGGREGATE", CommandCategories.ReadOnly, SearchCommandBuilder.Aggregate("idx", new AggregationRequest("*")) },
        {
            "FT.AGGREGATE WITHCURSOR",
            CommandCategories.ReadOnly | CommandCategories.ServerSpecific,
            SearchCommandBuilder.Aggregate("idx", new AggregationRequest("*").Cursor(10))
        },

        // reading a cursor advances it, so a replay skips a page rather than repeating one
        {
            "FT.CURSOR READ",
            CommandCategories.WriteAccumulating | CommandCategories.ServerSpecific,
            SearchCommandBuilder.CursorRead("idx", 1)
        },
        {
            "FT.CURSOR READ COUNT",
            CommandCategories.WriteAccumulating | CommandCategories.ServerSpecific,
            SearchCommandBuilder.CursorRead("idx", 1, 10)
        },
        {
            "FT.CURSOR DEL",
            CommandCategories.WriteLastWins | CommandCategories.ServerSpecific,
            SearchCommandBuilder.CursorDel("idx", 1)
        },

        // TS.ADD: only an explicit, non-SUM ON_DUPLICATE makes a replay provably idempotent
        { "TS.ADD ON_DUPLICATE LAST", CommandCategories.WriteLastWins, TsAdd(1L, TsDuplicatePolicy.LAST) },
        { "TS.ADD ON_DUPLICATE FIRST", CommandCategories.WriteLastWins, TsAdd(1L, TsDuplicatePolicy.FIRST) },
        { "TS.ADD ON_DUPLICATE MIN", CommandCategories.WriteLastWins, TsAdd(1L, TsDuplicatePolicy.MIN) },
        { "TS.ADD ON_DUPLICATE MAX", CommandCategories.WriteLastWins, TsAdd(1L, TsDuplicatePolicy.MAX) },

        // SUM adds the value again
        { "TS.ADD ON_DUPLICATE SUM", CommandCategories.WriteAccumulating, TsAdd(1L, TsDuplicatePolicy.SUM) },

        // BLOCK errors on a duplicate, so replaying a write whose reply was merely lost would surface a
        // spurious error rather than succeeding idempotently - worse for the caller than not retrying
        { "TS.ADD ON_DUPLICATE BLOCK", CommandCategories.WriteAccumulating, TsAdd(1L, TsDuplicatePolicy.BLOCK) },

        // no ON_DUPLICATE: the series' stored DUPLICATE_POLICY governs, and it may be SUM
        { "TS.ADD no ON_DUPLICATE", CommandCategories.WriteAccumulating, TsAdd(1L, policy: null) },

        // "*" appends a new sample on every attempt, whatever the policy says
        { "TS.ADD * ON_DUPLICATE LAST", CommandCategories.WriteAccumulating, TsAdd("*", TsDuplicatePolicy.LAST) },

        // TS.MADD has no per-sample ON_DUPLICATE, so it can never be narrowed
        {
            "TS.MADD",
            CommandCategories.WriteAccumulating,
            TimeSeriesCommandsBuilder.MAdd([("k", new TimeStamp(1L), 1d)])
        },
    };

    // goes via TsAddParamsBuilder, i.e. the path that flattens to an argument list before the command
    // builder sees it - the reason the category has to be resolved and carried on TsAddParams
    private static SerializedCommand TsAdd(TimeStamp timestamp, TsDuplicatePolicy? policy)
    {
        var builder = new TsAddParamsBuilder().AddTimestamp(timestamp).AddValue(1d);
        if (policy is { } p) builder = builder.AddOnDuplicate(p);
        return TimeSeriesCommandsBuilder.Add("k", builder.build());
    }

    [Theory]
    [MemberData(nameof(ArgumentDependentCases))]
    public void ArgumentDependentCategoriesAreCorrect(string description, CommandFlags expected, SerializedCommand command)
    {
        _ = description; // present so a failure names the case
        Assert.Equal(expected, command.CommandCategory);
    }

    [Fact]
    public void EffectiveFlagsIncludeTheCategory()
    {
        var command = SearchCommandBuilder.Search("idx", new Query("*"));
        Assert.Equal(CommandCategories.ReadOnly, command.CommandCategory);
        Assert.Equal(CommandCategories.ReadOnly, command.EffectiveFlags & CommandCategories.Mask);
    }
}
