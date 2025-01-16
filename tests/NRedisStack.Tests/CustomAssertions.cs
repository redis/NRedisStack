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
}
