using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;

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
    /// A pre-formatted base-64 value.
    /// </summary>
    public static VectorData FromBase64(string base64) => new VectorDataBase64(base64);

    /// <summary>
    /// A vector of <see cref="Single"/> entries.
    /// </summary>
    public static implicit operator VectorData(float[] data) => new VectorDataSingle(data);

    /// <summary>
    /// A vector of <see cref="Single"/> entries.
    /// </summary>
    public static implicit operator VectorData(ReadOnlyMemory<float> vector) => new VectorDataSingle(vector);

    internal virtual void AddArgs(List<object> args) => args.Add(ToString() ?? "");
    internal virtual int ArgsCount() => 1;

    private sealed class VectorDataSingle(ReadOnlyMemory<float> vector) : VectorData
    {
        public override string ToString()
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

    private sealed class VectorDataBase64(string vector) : VectorData
    {
        public override string ToString() => vector;
    }

    private protected static void ThrowBigEndian() =>
        throw new PlatformNotSupportedException("Big-endian CPUs are not currently supported for this operation");
}