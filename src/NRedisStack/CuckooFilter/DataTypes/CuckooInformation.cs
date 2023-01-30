namespace NRedisStack.CuckooFilter.DataTypes
{
    /// <summary>
    /// This class represents the response for CF.INFO command.
    /// This object has Read-only properties and cannot be generated outside a CF.INFO response.
    /// </summary>
    public class CuckooInformation
    {
        public long Size { get; private set; }
        public long NumberOfBuckets { get; private set; }
        public long NumberOfFilters { get; private set; }
        public long NumberOfItemsInserted { get; private set; }
        public long NumberOfItemsDeleted { get; private set; }
        public long BucketSize { get; private set; }
        public long ExpansionRate { get; private set; }
        public long MaxIterations { get; private set; }

        internal CuckooInformation(long size, long numberOfBuckets, long numberOfFilter,
                                   long numberOfItemsInserted, long numberOfItemsDeleted,
                                   long bucketSize, long expansionRate, long maxIteration)
        {
            Size = size;
            NumberOfBuckets = numberOfBuckets;
            NumberOfFilters = numberOfFilter;
            NumberOfItemsInserted = numberOfItemsInserted;
            NumberOfItemsDeleted = numberOfItemsDeleted;
            BucketSize = bucketSize;
            ExpansionRate = expansionRate;
            MaxIterations = maxIteration;
        }
    }
}