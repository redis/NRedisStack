namespace NRedisStack.Bloom.DataTypes
{
    /// <summary>
    /// This class represents the response for BF.INFO command.
    /// This object has Read-only properties and cannot be generated outside a BF.INFO response.
    /// </summary>
    public class BloomInformation
    {
        public long Capacity { get; private set; }
        public long Size { get; private set; }
        public long NumberOfFilters { get; private set; }
        public long NumberOfItemsInserted { get; private set; }
        public long ExpansionRate { get; private set; }

        internal BloomInformation(long capacity, long size, long numberOfFilters, long numberOfItemsInserted, long expansionRate)
        {
            Capacity = capacity;
            Size = size;
            NumberOfFilters = numberOfFilters;
            NumberOfItemsInserted = numberOfItemsInserted;
            ExpansionRate = expansionRate;
        }
    }
}