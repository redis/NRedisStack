using NRedisStack.Search;
using Xunit;

namespace NRedisStack.Tests;

public static class CustomAssertions
{
    // Generic method to assert that 'actual' is greater than 'expected'
    public static void GreaterThan<T>(T actual, T expected) where T : IComparable<T>
    {
        Assert.True(actual.CompareTo(expected) > 0,
            $"Failure: Expected value to be greater than {expected}, but found {actual}.");
    }

    // Generic method to assert that 'actual' is less than 'expected'
    public static void LessThan<T>(T actual, T expected) where T : IComparable<T>
    {
        Assert.True(actual.CompareTo(expected) < 0,
            $"Failure: Expected value to be less than {expected}, but found {actual}.");
    }

    /// <summary>
    /// Asserts that RediSearch has indexed exactly <paramref name="expected"/> documents in <paramref name="index"/>.
    /// Indexing can lag behind the writes, so FT.INFO num_docs is re-read a few times to let indexing catch up before the
    /// exact-equality assertion.
    /// </summary>
    public static void AssertIndexSize(ISearchCommands ft, string index, long expected)
    {
        long indexed = -1;
        // allow search time to catch up
        for (int i = 0; i < 10; i++)
        {
            indexed = ft.Info(index).NumDocs;

            if (indexed == expected)
                break;
        }
        Assert.Equal(expected, indexed);
    }

    /// <inheritdoc cref="AssertIndexSize"/>
    public static async Task AssertIndexSizeAsync(ISearchCommandsAsync ft, string index, long expected)
    {
        long indexed = -1;
        // allow search time to catch up
        for (int i = 0; i < 10; i++)
        {
            indexed = (await ft.InfoAsync(index)).NumDocs;

            if (indexed == expected)
                break;
        }
        Assert.Equal(expected, indexed);
    }
}
