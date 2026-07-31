using NRedisStack.Literals;
using StackExchange.Redis;
using NRedisStack.RedisStackCommands;

namespace NRedisStack;

public static class TdigestCommandBuilder
{
    public static SerializedCommand Add(RedisKey key, params double[] values)
    {
        var args = new object[values.Length + 1];
        args[0] = key;
        for (int i = 0; i < values.Length; i++)
        {
            args[i + 1] = values[i];
        }

        return new(CommandCategories.WriteAccumulating, TDIGEST.ADD, args);
    }

    public static SerializedCommand CDF(RedisKey key, params double[] values)
    {
        var args = new List<object>(values.Length + 1) { key };
        foreach (var value in values) args.Add(value);
        return new(CommandCategories.ReadOnly, TDIGEST.CDF, args);
    }

    public static SerializedCommand Create(RedisKey key, long compression = 100)
    {
        return new(CommandCategories.WriteChecked, TDIGEST.CREATE, key, TdigestArgs.COMPRESSION, compression);
    }

    public static SerializedCommand Info(RedisKey key)
    {
        return new(CommandCategories.ReadOnly, TDIGEST.INFO, key);
    }

    public static SerializedCommand Max(RedisKey key)
    {
        return new(CommandCategories.ReadOnly, TDIGEST.MAX, key);
    }

    public static SerializedCommand Min(RedisKey key)
    {
        return new(CommandCategories.ReadOnly, TDIGEST.MIN, key);
    }

    public static SerializedCommand Merge(RedisKey destinationKey, long compression = default(long), bool overide = false, params RedisKey[] sourceKeys)
    {
        if (sourceKeys.Length < 1) throw new ArgumentOutOfRangeException(nameof(sourceKeys));

        int numkeys = sourceKeys.Length;
        var args = new List<object>() { destinationKey, numkeys };
        foreach (var key in sourceKeys)
        {
            args.Add(key);
        }

        if (compression != default(long))
        {
            args.Add("COMPRESSION");
            args.Add(compression);
        }

        if (overide)
        {
            args.Add("OVERRIDE");
        }

        return new(CommandCategories.WriteAccumulating, TDIGEST.MERGE, args);
    }

    public static SerializedCommand Quantile(RedisKey key, params double[] quantile)
    {
        if (quantile.Length < 1) throw new ArgumentOutOfRangeException(nameof(quantile));

        var args = new List<object> { key };
        foreach (var q in quantile) args.Add(q);

        return new(CommandCategories.ReadOnly, TDIGEST.QUANTILE, args);
    }

    public static SerializedCommand Rank(RedisKey key, params long[] values)
    {
        if (values.Length < 1) throw new ArgumentOutOfRangeException(nameof(values));

        var args = new List<object>(values.Length + 1) { key };
        foreach (var v in values) args.Add(v);
        return new(CommandCategories.ReadOnly, TDIGEST.RANK, args);
    }

    public static SerializedCommand RevRank(RedisKey key, params long[] values)
    {
        if (values.Length < 1) throw new ArgumentOutOfRangeException(nameof(values));

        var args = new List<object>(values.Length + 1) { key };
        foreach (var v in values) args.Add(v);
        return new(CommandCategories.ReadOnly, TDIGEST.REVRANK, args);
    }

    public static SerializedCommand ByRank(RedisKey key, params long[] ranks)
    {
        if (ranks.Length < 1) throw new ArgumentOutOfRangeException(nameof(ranks));

        var args = new List<object>(ranks.Length + 1) { key };
        foreach (var v in ranks) args.Add(v);
        return new(CommandCategories.ReadOnly, TDIGEST.BYRANK, args);
    }

    public static SerializedCommand ByRevRank(RedisKey key, params long[] ranks)
    {
        if (ranks.Length < 1) throw new ArgumentOutOfRangeException(nameof(ranks));

        var args = new List<object>(ranks.Length + 1) { key };
        foreach (var v in ranks) args.Add(v);
        return new(CommandCategories.ReadOnly, TDIGEST.BYREVRANK, args);
    }

    public static SerializedCommand Reset(RedisKey key)
    {
        return new(CommandCategories.WriteLastWins, TDIGEST.RESET, key);
    }

    public static SerializedCommand TrimmedMean(RedisKey key, double lowCutQuantile, double highCutQuantile)
    {
        return new(CommandCategories.ReadOnly, TDIGEST.TRIMMED_MEAN, key, lowCutQuantile, highCutQuantile);
    }
}