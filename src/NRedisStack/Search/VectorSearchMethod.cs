using System.Diagnostics.CodeAnalysis;

namespace NRedisStack.Search;

public abstract class VectorSearchMethod
{
    private protected VectorSearchMethod()
    {
    }

    private protected abstract string Method { get; }

    internal abstract int GetOwnArgsCount();
    internal abstract void AddOwnArgs(List<object> args);

    /// <inheritdoc />
    public override string ToString() => Method;

    public static VectorSearchMethod Range(double radius, double? epsilon = null) => RangeVectorSearchMethod.Create(radius, epsilon, null);
    internal static VectorSearchMethod Range(double radius, double? epsilon, string? distanceAlias)
        => RangeVectorSearchMethod.Create(radius, epsilon, distanceAlias);

    public static VectorSearchMethod NearestNeighbour(
        int count, int? maxCandidates) // retained for binary compat
        => NearestNeighbourVectorSearchMethod.Create(count, maxCandidates, null, null);

    public static VectorSearchMethod NearestNeighbour(
        int? count = NearestNeighbourVectorSearchMethod.DEFAULT_NEAREST_NEIGHBOUR_COUNT, int? maxTopCandidates = null, string? distanceAlias = null, double? shardRatio = null)
        => NearestNeighbourVectorSearchMethod.Create(count ?? NearestNeighbourVectorSearchMethod.DEFAULT_NEAREST_NEIGHBOUR_COUNT, maxTopCandidates, distanceAlias, shardRatio);

    private sealed class NearestNeighbourVectorSearchMethod : VectorSearchMethod
    {
        private static NearestNeighbourVectorSearchMethod? s_Default;

        internal static NearestNeighbourVectorSearchMethod Create(int count, int? maxTopCandidates,
            string? distanceAlias, double? shardRatio)
            => count == DEFAULT_NEAREST_NEIGHBOUR_COUNT & maxTopCandidates == null & distanceAlias == null & !shardRatio.HasValue
                ? (s_Default ??= new NearestNeighbourVectorSearchMethod(DEFAULT_NEAREST_NEIGHBOUR_COUNT, null, null, null))
                : new(count, maxTopCandidates, distanceAlias, shardRatio);

        private NearestNeighbourVectorSearchMethod(int nearestNeighbourCount, int? maxTopCandidates,
            string? distanceAlias, double? shardRatio)
        {
            NearestNeighbourCount = nearestNeighbourCount;
            MaxTopCandidates = maxTopCandidates;
            DistanceAlias = distanceAlias;
            ShardRatio = shardRatio;
        }

        internal const int DEFAULT_NEAREST_NEIGHBOUR_COUNT = 10;
        private protected override string Method => "KNN";

        /// <summary>
        /// The number of nearest neighbors to find. This is the K in KNN.
        /// </summary>
        public int NearestNeighbourCount { get; }

        /// <summary>
        /// Max top candidates during KNN search. Higher values increase accuracy, but also increase search latency.
        /// This corresponds to the HNSW "EF_RUNTIME" parameter.
        /// </summary>
        public int? MaxTopCandidates { get; }

        /// <summary>
        /// Include the distance from the query vector in the results.
        /// </summary>
        public string? DistanceAlias { get; }

        /// <summary>
        /// Limits the number of documents processed per shard. Only relevant for cluster scenarios. This corresponds
        /// to the "SHARD_K_RATIO" parameter.
        /// </summary>
        public double? ShardRatio { get; }

        internal override int GetOwnArgsCount()
        {
            int count = 4;
            if (MaxTopCandidates.HasValue) count += 2;
            if (DistanceAlias != null) count += 2;
            if (ShardRatio.HasValue) count += 2;
            return count;
        }

        internal override void AddOwnArgs(List<object> args)
        {
            args.Add(Method);
            int tokens = 2;
            if (MaxTopCandidates.HasValue) tokens += 2;
            if (DistanceAlias != null) tokens += 2;
            if (ShardRatio.HasValue) tokens += 2;
            args.Add(tokens);
            args.Add("K");
            args.Add(NearestNeighbourCount);
            if (MaxTopCandidates.HasValue)
            {
                args.Add("EF_RUNTIME");
                args.Add(MaxTopCandidates.GetValueOrDefault());
            }
            if (DistanceAlias != null)
            {
                args.Add("YIELD_DISTANCE_AS");
                args.Add(DistanceAlias);
            }

            if (ShardRatio.HasValue)
            {
                args.Add("SHARD_K_RATIO");
                args.Add(ShardRatio.GetValueOrDefault());
            }
        }
    }

    private sealed class RangeVectorSearchMethod : VectorSearchMethod
    {
        internal static RangeVectorSearchMethod Create(double radius, double? epsilon, string? distanceAlias)
            => new(radius, epsilon, distanceAlias);

        private RangeVectorSearchMethod(double radius, double? epsilon, string? distanceAlias)
        {
            Radius = radius;
            Epsilon = epsilon;
            DistanceAlias = distanceAlias;
        }

        private protected override string Method => "RANGE";

        /// <summary>
        /// The search radius/threshold. Finds all vectors within this distance.
        /// </summary>
        public double Radius { get; }

        /// <summary>
        /// Relative factor that sets the boundaries in which a range query may search for candidates. That is, vector candidates whose distance from the query vector is radius * (1 + EPSILON) are potentially scanned, allowing more extensive search and more accurate results, at the expense of run time.
        /// </summary>
        public double? Epsilon { get; }

        /// <summary>
        /// Include the distance from the query vector in the results.
        /// </summary>
        public string? DistanceAlias { get; }

        internal override int GetOwnArgsCount()
        {
            int count = 4;
            if (Epsilon != null) count += 2;
            if (DistanceAlias != null) count += 2;
            return count;
        }

        internal override void AddOwnArgs(List<object> args)
        {
            args.Add(Method);
            int tokens = 2;
            if (Epsilon != null) tokens += 2;
            if (DistanceAlias != null) tokens += 2;
            args.Add(tokens);
            args.Add("RADIUS");
            args.Add(Radius);
            if (Epsilon != null)
            {
                args.Add("EPSILON");
                args.Add(Epsilon);
            }

            if (DistanceAlias != null)
            {
                args.Add("YIELD_DISTANCE_AS");
                args.Add(DistanceAlias);
            }
        }
    }
}