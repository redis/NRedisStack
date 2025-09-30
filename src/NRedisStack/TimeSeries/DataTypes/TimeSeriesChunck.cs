namespace NRedisStack.DataTypes;

/// <summary>
/// A key-value pair class represetns metadata label of time-series.
/// </summary>
public class TimeSeriesChunck
{
    /// <summary>
    /// First timestamp present in the chunk
    /// </summary>
    public long StartTimestamp { get; }

    /// <summary>
    /// Last timestamp present in the chunk
    /// </summary>
    public long EndTimestamp { get; }

    /// <summary>
    /// Total number of samples in the chunk
    /// </summary>
    public long Samples { get; }

    /// <summary>
    /// The chunk data size in bytes. This is the exact size that used for
    /// data only inside the chunk. It does not include other overheads.
    /// </summary>
    public long Size { get; }

    /// <summary>
    /// Ratio of size and samples
    /// </summary>
    public string BytesPerSample { get; }

    public TimeSeriesChunck(long startTimestamp, long endTimestamp, long samples, long size, string bytesPerSample)
    {
        StartTimestamp = startTimestamp;
        EndTimestamp = endTimestamp;
        Samples = samples;
        Size = size;
        BytesPerSample = bytesPerSample;
    }
}