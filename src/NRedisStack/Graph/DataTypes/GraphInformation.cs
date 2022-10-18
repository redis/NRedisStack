namespace NRedisStack.Graph.DataTypes
{
    /// <summary>
    /// This class represents the response for GRAPH.INFO command.
    /// This object has Read-only properties and cannot be generated outside a GRAPH.INFO response.
    /// </summary>
    public class GraphInformation
    {
        public long Capacity { get; private set; }
        public long Size { get; private set; }
        public long NumberOfFilters { get; private set; }
        public long NumberOfItemsInserted { get; private set; }
        public long ExpansionRate { get; private set; }

        internal GraphInformation(long capacity, long size, long numberOfFilters, long numberOfItemsInserted, long expansionRate)
        {
            Capacity = capacity;
            Size = size;
            NumberOfFilters = numberOfFilters;
            NumberOfItemsInserted = numberOfItemsInserted;
            ExpansionRate = expansionRate;
        }
    }
}