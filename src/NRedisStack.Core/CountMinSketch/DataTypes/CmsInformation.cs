namespace NRedisStack.Core.CountMinSketch.DataTypes
{
    /// <summary>
    /// This class represents the response for CMS.INFO command.
    /// This object has Read-only properties and cannot be generated outside a CMS.INFO response.
    /// </summary>
    public class CmsInformation
    {
        public long Width { get; private set; }
        public long Depth { get; private set; }
        public long Count { get; private set; }


        internal CmsInformation(long width, long depth, long count)
        {
            Width = width;
            Depth = depth;
            Count = count;
        }
    }
}