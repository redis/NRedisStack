namespace NRedisStack.TopK.DataTypes
{
    /// <summary>
    /// This class represents the response for TOPK.INFO command.
    /// This object has Read-only properties and cannot be generated outside a TOPK.INFO response.
    /// </summary>
    public class TopKInformation
    {
        public long K { get; private set; }
        public long Width { get; private set; }
        public long Depth { get; private set; }
        public double Decay { get; private set; }


        internal TopKInformation(long k, long width, long depth, double decay)
        {
            K = k;
            Width = width;
            Depth = depth;
            Decay = decay;
        }
    }
}