namespace NRedisStack.CountMinSketch.DataTypes;

/// <summary>
/// This class represents the response for CMS.INFO command.
/// This object has Read-only properties and cannot be generated outside a CMS.INFO response.
/// </summary>
public class CmsInformation
{
    public long Width { get; private set; }
    public long Depth { get; private set; }
    public long Count { get; private set; }

    /// <summary>
    /// The counter byte width, in bytes. Not reported by older servers, in which case this is -1.
    /// </summary>
    public int CellSize { get; private set; }

    internal CmsInformation(long width, long depth, long count, int cellSize)
    {
        Width = width;
        Depth = depth;
        Count = count;
        CellSize = cellSize;
    }
}