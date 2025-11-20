using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using StackExchange.Redis;

namespace NRedisStack.Search;

[Experimental(Experiments.Server_8_4, UrlFormat = Experiments.UrlFormat)]
public abstract class VectorData<T> : VectorData, IDisposable where T : unmanaged
{
    private protected VectorData()
    {
    }

    public abstract Span<T> Span { get; }
    internal sealed class VectorBytesData(int byteLength) : VectorData<T> 
    {
        private byte[]? _oversized = ArrayPool<byte>.Shared.Rent(byteLength);
        public override Span<T> Span => MemoryMarshal.Cast<byte, T>(Array.AsSpan(0, byteLength));
        internal override object GetSingleArg() => (RedisValue)new ReadOnlyMemory<byte>(Array,  0, byteLength);

        private byte[] Array => _oversized ?? ThrowDisposed();
        static byte[] ThrowDisposed() => throw new ObjectDisposedException(nameof(VectorData));
        public override void Dispose()
        {
            var tmp = _oversized;
            _oversized = null;
            if (tmp is not null) ArrayPool<byte>.Shared.Return(tmp);
        }
    }

    public abstract void Dispose();
}

[Experimental(Experiments.Server_8_4, UrlFormat = Experiments.UrlFormat)]
public abstract class VectorData
{
    /// <summary>
    /// Lease a vector that can hold values of <typeparamref name="T"/>.
    /// No quantization occurs - the data is transmitted as the raw bytes of the corresponding size.
    /// </summary>
    /// <param name="dimension">The number of values to be held.</param>
    /// <typeparam name="T">The data type to be represented</typeparam>
    public static VectorData<T> Lease<T>(int dimension) where T : unmanaged
    {
        if (dimension < 0) ThrowDimension();
        if (!BitConverter.IsLittleEndian) ThrowBigEndian();
        return new VectorData<T>.VectorBytesData(Unsafe.SizeOf<T>() * dimension);

        static void ThrowDimension() => throw new ArgumentOutOfRangeException(nameof(dimension));
    }

    /// <summary>
    /// Lease a vector that can hold values of <typeparamref name="T"/>, copying in the supplied values.
    /// </summary>
    public static VectorData<T> LeaseWithValues<T>(params ReadOnlySpan<T> values) where T : unmanaged
    {
        var lease = Lease<T>(values.Length);
        values.CopyTo(lease.Span);
        return lease;
    }

    private protected VectorData()
    {
    }

    /// <summary>
    /// A raw vector payload.
    /// </summary>
    public static VectorData Raw(ReadOnlyMemory<byte> bytes) => new VectorDataRaw(bytes);

    /// <summary>
    /// Represent a vector as a parameter to be supplied later.
    /// </summary>
    public static VectorData Parameter(string name) => new VectorParameter(name);

    /*
    /// <summary>
    /// A vector of <see cref="Single"/> entries.
    /// </summary>
    public static implicit operator VectorData(float[] data) => new VectorDataSingle(data);

    /// <inheritdoc cref="Create"/>
    public static implicit operator VectorData(ReadOnlyMemory<float> vector) => new VectorDataSingle(vector);
*/
    
    /// <inheritdoc cref="Parameter"/>
    public static implicit operator VectorData(string name) => new VectorParameter(name);

    internal abstract object GetSingleArg();

    /// <inheritdoc/>
    public override string ToString() => GetType().Name;

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