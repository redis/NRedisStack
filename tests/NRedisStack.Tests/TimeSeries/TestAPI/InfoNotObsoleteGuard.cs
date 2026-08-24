using NRedisStack.RedisStackCommands;
using StackExchange.Redis;

namespace NRedisStack.Tests.TimeSeries.TestAPI;

// COMPILE-TIME test - deliberately NOT an xUnit [Fact]/[Theory], and never executed (no server, no
// assertions). The check is performed entirely by the compiler, so this file must never be given a
// file-level obsolete-suppression pragma.
//
// Why it exists: TS.INFO is not deprecated, but Info/InfoAsync on the concrete command types carried a
// bare [Obsolete] from #184 (v0.10.0) until #548. Nothing about the command was deprecated; the attribute
// was added to silence CS0618 raised inside TimeSeriesInformation's constructor, which still has to
// populate the genuinely-deprecated MaxSamplesPerChunk, and it then cascaded outwards through
// ToTimeSeriesInfo to the public API. The interfaces were never marked, so the warning only hit callers
// going through db.TS() - which is to say, essentially everyone.
//
// This project compiles CS0612/CS0618 as errors (see NRedisStack.Tests.csproj), so if any of these ever
// becomes obsolete again, this file fails to BUILD. That build failure IS the test.
internal static class InfoNotObsoleteGuard
{
    public static void SyncInterface(ITimeSeriesCommands ts) => _ = ts.Info("k").ChunkSize;

    public static Task AsyncInterface(ITimeSeriesCommandsAsync ts) => ts.InfoAsync("k");

    public static void Concrete(IDatabase db)
    {
        // db.TS() returns the concrete TimeSeriesCommands - the dominant real-world call pattern, and the
        // one that actually produced the warning reported in #548.
        _ = db.TS().Info("k").ChunkSize;
        _ = db.TS().InfoAsync("k");
    }
}
