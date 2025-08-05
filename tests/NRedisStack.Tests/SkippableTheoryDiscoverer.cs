using Xunit.Sdk;

namespace NRedisStack.Tests;

// TODO(imalinovskiy): Remove this file once tests are migrated to Xunit v3

/// Copyright (c) Andrew Arnott. All rights reserved.
//  Licensed under the Microsoft Public License (Ms-PL).
//  https://github.com/AArnott/Xunit.SkippableFact/blob/main/src/Xunit.SkippableFact/Sdk/SkippableTheoryDiscoverer.cs
//  See https://github.com/AArnott/Xunit.SkippableFact/blob/main/LICENSE for full license information.

using System.Collections.Generic;
using Validation;
using Xunit.Abstractions;

/// <summary>
/// Patched TestCase discoverer to support SkipIfRedisAttribute.
/// </summary>
public class SkippableTheoryDiscoverer : IXunitTestCaseDiscoverer
{
    /// <summary>
    /// The diagnostic message sink provided to the constructor.
    /// </summary>
    private readonly IMessageSink diagnosticMessageSink;

    /// <summary>
    /// The complex theory discovery process that we wrap.
    /// </summary>
    private readonly TheoryDiscoverer theoryDiscoverer;

    /// <summary>
    /// Initializes a new instance of the <see cref="SkippableTheoryDiscoverer"/> class.
    /// </summary>
    /// <param name="diagnosticMessageSink">The message sink used to send diagnostic messages.</param>
    public SkippableTheoryDiscoverer(IMessageSink diagnosticMessageSink)
    {
        this.diagnosticMessageSink = diagnosticMessageSink;
        theoryDiscoverer = new(diagnosticMessageSink);
    }

    /// <inheritdoc />
    public virtual IEnumerable<IXunitTestCase> Discover(ITestFrameworkDiscoveryOptions discoveryOptions, ITestMethod testMethod, IAttributeInfo factAttribute)
    {
        Requires.NotNull(factAttribute, nameof(factAttribute));
        string[] skippingExceptionNames = ["Xunit.SkippableFact.SkipException", "Xunit.SkipException"];
        TestMethodDisplay defaultMethodDisplay = discoveryOptions.MethodDisplayOrDefault();

        IEnumerable<IXunitTestCase>? basis = theoryDiscoverer.Discover(discoveryOptions, testMethod, factAttribute);
        foreach (IXunitTestCase? testCase in basis)
        {
            if (testCase is XunitTheoryTestCase)
            {
                yield return new SkippableTheoryTestCase(skippingExceptionNames, diagnosticMessageSink, defaultMethodDisplay, discoveryOptions.MethodDisplayOptionsOrDefault(), testCase.TestMethod);
            }
            else
            {
                yield return new SkippableFactTestCase(skippingExceptionNames, diagnosticMessageSink, defaultMethodDisplay, discoveryOptions.MethodDisplayOptionsOrDefault(), testCase.TestMethod, testCase.TestMethodArguments);
            }
        }
    }
}