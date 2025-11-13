using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using StackExchange.Redis;

namespace NRedisStack.Search;

[Experimental(Experiments.Server_8_4, UrlFormat = Experiments.UrlFormat)]
public abstract class VectorData
{
    private protected VectorData()
    {
    }

    /// <summary>
    /// A vector of <see cref="Single"/> entries.
    /// </summary>
    public static VectorData Create(ReadOnlyMemory<float> vector) => new VectorDataSingle(vector);

    /// <summary>
    /// A raw vector payload.
    /// </summary>
    public static VectorData Raw(ReadOnlyMemory<byte> bytes) => new VectorDataRaw(bytes);

    /// <summary>
    /// Represent a vector as a parameter to be supplied later.
    /// </summary>
    public static VectorData Parameter(string name) => new VectorParameter(name);

    /// <summary>
    /// A vector of <see cref="Single"/> entries.
    /// </summary>
    public static implicit operator VectorData(float[] data) => new VectorDataSingle(data);

    /// <inheritdoc cref="Create"/>
    public static implicit operator VectorData(ReadOnlyMemory<float> vector) => new VectorDataSingle(vector);

    /// <inheritdoc cref="Parameter"/>
    public static implicit operator VectorData(string name) => new VectorParameter(name);

    internal abstract object GetSingleArg();

    /// <inheritdoc/>
    public override string ToString() => GetType().Name;

    private sealed class VectorDataSingle(ReadOnlyMemory<float> vector) : VectorData
    {
        internal override object GetSingleArg() => ToBase64();
        public override string ToString() => ToBase64();

        private string ToBase64()
        {
            if (!BitConverter.IsLittleEndian) ThrowBigEndian(); // we could loop and reverse each, but...how to test?
            var bytes = MemoryMarshal.AsBytes(vector.Span);
#if NET || NETSTANDARD2_1_OR_GREATER
            return Convert.ToBase64String(bytes);
#else
            var oversized = ArrayPool<byte>.Shared.Rent(bytes.Length);
            bytes.CopyTo(oversized);
            var result = Convert.ToBase64String(oversized, 0, bytes.Length);
            ArrayPool<byte>.Shared.Return(oversized);
            return result;
#endif
        }
    }

    private sealed class VectorDataRaw(ReadOnlyMemory<byte> bytes) : VectorData
    {
        internal override object GetSingleArg() => (RedisValue)bytes;
    }

    private sealed class VectorParameter : VectorData
    {
        private readonly string name;

        public VectorParameter(string name)
        {
            if (string.IsNullOrEmpty(name) || name[0] != '$') Throw();
            this.name = name;
            static void Throw() => throw new ArgumentException("Parameter tokens must start with the character '$'.");
        }

        public override string ToString() => name;
        internal override object GetSingleArg() => name;
    }

    private protected static void ThrowBigEndian() =>
        throw new PlatformNotSupportedException("Big-endian CPUs are not currently supported for this operation");
}