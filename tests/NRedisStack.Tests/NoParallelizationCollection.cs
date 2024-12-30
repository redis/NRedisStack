using Xunit;

namespace NRedisStack.Tests;

[CollectionDefinition("NoParallelization", DisableParallelization = true)]
public class NoParallelizationCollection { }
