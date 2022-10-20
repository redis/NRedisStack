using System;
using NRedisStack.Graph;
using Xunit;

namespace NRedisStack.Tests.Graph
{
    public class PathBuilderTest
    {
        [Fact]
        public void TestPathBuilderSizeException()
        {
            var thrownException = Assert.Throws<ArgumentException>(()=>
            {
                var pathBuilder = new PathBuilder(0);

                pathBuilder.Build();
            });

            Assert.Equal("Path builder nodes count should be edge count + 1", thrownException.Message);
        }

        [Fact]
        public void TestPathBuilderArgumentsExceptionNodeExpected()
        {
            var thrownException = Assert.Throws<ArgumentException>(() =>
            {
                var builder = new PathBuilder(0);

                builder.Append(new Edge());
            });

            Assert.Equal("Path builder expected Node but was Edge.", thrownException.Message);
        }

        [Fact]
        public void TestPathBuilderArgumentsExceptionPathExpected()
        {
            var thrownException = Assert.Throws<ArgumentException>(() =>
            {
                var builder = new PathBuilder(0);

                builder.Append(new Node());
                builder.Append(new Node());
            });

            Assert.Equal("Path builder expected Edge but was Node.", thrownException.Message);
        }
    }
}