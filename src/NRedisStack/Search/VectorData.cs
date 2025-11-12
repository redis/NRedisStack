using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;

namespace NRedisStack.Search;

[Experimental(Experiments.Server_8_4, UrlFormat = Experiments.UrlFormat)]
public readonly struct VectorData
{
    // intended to allow future flexibility in how we express vectors
    private readonly ReadOnlyMemory<byte> _data;
    private VectorData(ReadOnlyMemory<byte> data)
    {
        _data = data;
    }

    public static implicit operator VectorData(byte[] data) => new(data);
    public static implicit operator VectorData(ReadOnlyMemory<byte> data) => new(data);
    internal void AddOwnArgs(List<object> args)
    {
#if NET || NETSTANDARD2_1_OR_GREATER
        args.Add(Convert.ToBase64String(_data.Span));
#else
            if (MemoryMarshal.TryGetArray(_data, out ArraySegment<byte> segment))
            {
                args.Add(Convert.ToBase64String(segment.Array!, segment.Offset, segment.Count));
            }
            else
            {
                var span = _data.Span;
                var oversized = ArrayPool<byte>.Shared.Rent(span.Length);
                span.CopyTo(oversized);
                args.Add(Convert.ToBase64String(oversized, 0, span.Length));
                ArrayPool<byte>.Shared.Return(oversized);
            }
#endif
    }
    internal int GetOwnArgsCount() => 1;
    internal bool HasValue => _data.Length > 0;
}