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
    // The rungs are aliases of the CommandFlags.CommandRetry* members, so there is nothing left to compare
    // them against. What still needs pinning is the numbering the masks assume, and the one value with no
    // named member: Message.CommandServerSpecific is internal to StackExchange.Redis, so ServerSpecific is
    // ours to keep aligned with the bit their mask actually tests.
    [Fact]
    public void ServerSpecificIsBit18() => Assert.Equal(1 << 18, (int)CommandCategories.ServerSpecific);

    // consequently this also pins that the rung ladder still occupies bits 13-17 upstream: a renumbering
    // there would move Never, and every masking decision built on it
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

    // a category that names no rung is almost certainly a mistake, and silently accepting it would mean
    // "never retry" dressed up as a declaration
    [Theory]
    [InlineData(CommandFlags.None)]
    [InlineData(CommandFlags.FireAndForget)]              // real flag, wrong parameter
    [InlineData(CommandFlags.DemandReplica)]
    [InlineData(CommandCategories.ServerSpecific)]        // sticky bit, but no rung
    public void ConstructingWithNoCategoryThrows(CommandFlags category)
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new SerializedCommand(category, "FT.SEARCH", "idx", "*"));
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new SerializedCommand(category, "FT.SEARCH", new List<object> { "idx", "*" }));
    }

    [Fact]
    public void ARungPlusServerSpecificIsAccepted()
    {
        var command = new SerializedCommand(
            CommandCategories.ReadOnly | CommandCategories.ServerSpecific, "BF.SCANDUMP", "k", 0);
        Assert.Equal(CommandCategories.ReadOnly | CommandCategories.ServerSpecific, command.CommandCategory);
    }

    // the obsolete ctors must keep working and keep meaning "uncategorized", or every existing caller
    // would go from a compile-time warning to a runtime exception
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

        // A cursored aggregate allocates cursor state, so every replay leaks another cursor. Never rather
        // than a high rung: ServerSpecific only stops failover, and any rung below Never can be re-enabled
        // by a caller raising MaxCommandRetryCategory.
        { "FT.AGGREGATE", CommandCategories.ReadOnly, SearchCommandBuilder.Aggregate("idx", new AggregationRequest("*")) },
        {
            "FT.AGGREGATE WITHCURSOR",
            CommandCategories.Never,
            SearchCommandBuilder.Aggregate("idx", new AggregationRequest("*").Cursor(10))
        },
        { "FT.PROFILE SEARCH", CommandCategories.ReadOnly, SearchCommandBuilder.ProfileSearch("idx", new Query("*")) },
        {
            "FT.PROFILE AGGREGATE",
            CommandCategories.ReadOnly,
            SearchCommandBuilder.ProfileAggregate("idx", new AggregationRequest("*"))
        },
        {
            // must match FT.AGGREGATE WITHCURSOR: profiling still allocates the cursor
            "FT.PROFILE AGGREGATE WITHCURSOR",
            CommandCategories.Never,
            SearchCommandBuilder.ProfileAggregate("idx", new AggregationRequest("*").Cursor(10))
        },

        // reading a cursor advances it, so a replay skips a page rather than repeating one
        {
            "FT.CURSOR READ",
            CommandCategories.Never,
            SearchCommandBuilder.CursorRead("idx", 1)
        },
        {
            "FT.CURSOR READ COUNT",
            CommandCategories.Never,
            SearchCommandBuilder.CursorRead("idx", 1, 10)
        },
        {
            // RediSearch errors on a repeat delete rather than treating it as an OK no-op, so a replay
            // reports failure for cleanup that succeeded
            "FT.CURSOR DEL",
            CommandCategories.Never,
            SearchCommandBuilder.CursorDel("idx", 1)
        },

        // Module DDL leaves state correct on replay but *errors* ("Index already exists", "Index not
        // found"), so it must sit above the default ceiling or a lost reply gets replayed by default and
        // reports failure for something that succeeded. Verified against a live server; see
        // CommandCategories.WriteAccumulating.
        {
            "FT.CREATE",
            CommandCategories.WriteAccumulating,
            SearchCommandBuilder.Create("idx", new FTCreateParams(), new Schema().AddTextField("n"))
        },
        { "FT.DROPINDEX", CommandCategories.WriteAccumulating, SearchCommandBuilder.DropIndex("idx") },
        { "FT.ALIASADD", CommandCategories.WriteAccumulating, SearchCommandBuilder.AliasAdd("al", "idx") },
        { "FT.ALIASDEL", CommandCategories.WriteAccumulating, SearchCommandBuilder.AliasDel("al") },
        { "BF.RESERVE", CommandCategories.WriteAccumulating, BloomCommandBuilder.Reserve("k", 0.01, 100) },
        { "TS.CREATE", CommandCategories.WriteAccumulating, TimeSeriesCommandsBuilder.Create("k", new TsCreateParamsBuilder().build()) },

        // by contrast, the true SETNX analogues return 0 rather than erroring, so they stay put
        { "BF.ADD", CommandCategories.WriteChecked, BloomCommandBuilder.Add("k", "item") },
        { "CF.ADDNX", CommandCategories.WriteChecked, CuckooCommandBuilder.AddNX("k", "item") },

        // ...and the deletes that report 0 rather than erroring stay idempotent overwrites
        { "FT.DICTDEL", CommandCategories.WriteLastWins, SearchCommandBuilder.DictDel("d", "term") },
        { "FT.SUGDEL", CommandCategories.WriteLastWins, SearchCommandBuilder.SugDel("k", "s") },

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
