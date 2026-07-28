using NRedisStack;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;

namespace NRedisStack.Tests.TimeSeries.TestAPI;

// COMPILE-TIME test — deliberately NOT an xUnit [Fact]/[Theory], and never executed (no server, no
// assertions, the parameters are never dereferenced). The check is performed entirely by the compiler.
//
// Why it exists: an integer tuple such as (42, 42) is implicitly convertible to BOTH the current
// (double, double) filterByValue API and the retained [Obsolete] (long, long) binary-compat overloads, and
// int -> long is normally the "better" conversion — so without [OverloadResolutionPriority] keeping the
// (long, long) shims de-prioritised, callers would silently bind to the obsolete overloads. This project
// compiles CS0612/CS0618 as errors (see NRedisStack.Tests.csproj), so if any call below ever resolves to a
// hidden [Obsolete] overload, this file fails to BUILD. That build failure IS the test.
//
// Why not a [Fact]: there is nothing to assert at run time — for an integer tuple the (double, double) and
// (long, long) overloads behave identically on the wire, so the only observable difference is which overload
// the compiler selected, which is exactly what "does this compile without an obsolete-usage diagnostic"
// proves. Executing it would require a live server and prove nothing extra.
internal static class ObsoleteOverloadResolutionGuard
{
    private static readonly string[] Filter = ["sensor=1"];

    public static void SyncInterface(ITimeSeriesCommands ts)
    {
        _ = ts.Range("k", "-", "+", filterByValue: (42, 42));
        _ = ts.RevRange("k", "-", "+", filterByValue: (42, 42));
        _ = ts.MRange("-", "+", Filter, filterByValue: (42, 42));                 // flags overload
        _ = ts.MRange("-", "+", Filter, latest: false, filterByValue: (42, 42));  // latest overload
        _ = ts.MRevRange("-", "+", Filter, filterByValue: (42, 42));
        _ = ts.MRevRange("-", "+", Filter, latest: false, filterByValue: (42, 42));
    }

    public static void AsyncInterface(ITimeSeriesCommandsAsync ts)
    {
        _ = ts.RangeAsync("k", "-", "+", filterByValue: (42, 42));
        _ = ts.RevRangeAsync("k", "-", "+", filterByValue: (42, 42));
        _ = ts.MRangeAsync("-", "+", Filter, filterByValue: (42, 42));
        _ = ts.MRangeAsync("-", "+", Filter, latest: false, filterByValue: (42, 42));
        _ = ts.MRevRangeAsync("-", "+", Filter, filterByValue: (42, 42));
        _ = ts.MRevRangeAsync("-", "+", Filter, latest: false, filterByValue: (42, 42));
    }

    public static void Concrete(IDatabase db)
    {
        // db.TS() returns the concrete TimeSeriesCommands - the dominant real-world call pattern.
        _ = db.TS().Range("k", "-", "+", filterByValue: (42, 42));
        _ = db.TS().RevRange("k", "-", "+", filterByValue: (42, 42));
        _ = db.TS().MRange("-", "+", Filter, filterByValue: (42, 42));
        _ = db.TS().MRevRange("-", "+", Filter, filterByValue: (42, 42));
        _ = db.TS().RangeAsync("k", "-", "+", filterByValue: (42, 42));
        _ = db.TS().MRangeAsync("-", "+", Filter, filterByValue: (42, 42));
    }

    public static void Builder()
    {
        _ = TimeSeriesCommandsBuilder.Range("k", "-", "+", filterByValue: (42, 42));
        _ = TimeSeriesCommandsBuilder.RevRange("k", "-", "+", filterByValue: (42, 42));
        _ = TimeSeriesCommandsBuilder.MRange("-", "+", Filter, filterByValue: (42, 42));
        _ = TimeSeriesCommandsBuilder.MRevRange("-", "+", Filter, filterByValue: (42, 42));
    }
}
