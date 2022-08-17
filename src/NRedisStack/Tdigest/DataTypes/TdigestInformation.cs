namespace NRedisStack.Tdigest.DataTypes
{
    /// <summary>
    /// This class represents the response for TDIGEST.INFO command.
    /// This object has Read-only properties and cannot be generated outside a TDIGEST.INFO response.
    /// </summary>
    public class TdigestInformation
    {
        public long Compression { get; private set; }
        public long Capacity { get; private set; }
        public long MergedNodes { get; private set; }
        public long UnmergedNodes { get; private set; }
        public double MergedWeight { get; private set; }
        public double UnmergedWeight { get; private set; }

        public long TotalCompressions { get; private set; }


        internal TdigestInformation(long compression, long capacity, long mergedNodes,
                                    long unmergedNodes, double mergedWeight,
                                    double unmergedWeight, long totalCompressions)

        {
            Compression = compression;
            Capacity = capacity;
            MergedNodes = mergedNodes;
            UnmergedNodes = unmergedNodes;
            MergedWeight = mergedWeight;
            UnmergedWeight = unmergedWeight;
            TotalCompressions = totalCompressions;
        }
    }
}