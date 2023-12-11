using Xunit;
using System.Runtime.InteropServices;

namespace NRedisStack.Tests
{
    public class ImportDllTest : AbstractNRedisStackTest, IDisposable
    {
        const string LibName = "libcommands";

        [DllImport(LibName, EntryPoint = "numbers")]
        private static extern int numbers(int a, int b);

        [DllImport(LibName, EntryPoint = "hello_world")]
        private static extern string hello_world();

        public ImportDllTest(RedisFixture redisFixture) : base(redisFixture) { }

        [Fact]
        public void TestImportDll()
        {
            Assert.Equal(3, numbers(1, 2));
            Assert.Equal("Hello, world", hello_world());
        }
    }
}