namespace NRedisStack.Core.Literals
{
    internal class TDIGEST
    {
        public static string CREATE => "TDIGEST.CREATE";
        public static string RESET => "TDIGEST.RESET";
        public static string ADD => "TDIGEST.ADD";
        public static string MERGE => "TDIGEST.MERGE";
        public static string MERGESTORE => "TDIGEST.MERGESTORE";
        public static string MIN => "TDIGEST.MIN";
        public static string MAX => "TDIGEST.MAX";
        public static string QUANTILE => "TDIGEST.QUANTILE";
        public static string CDF => "TDIGEST.CDF";
        public static string TRIMMED_MEAN => "TDIGEST.TRIMMED_MEAN";
        public static string INFO => "TDIGEST.INFO";
    }
}