namespace NRedisStack.Search;

public sealed partial class HybridSearchQuery
{
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

        public static VectorSearchMethod Range(double radius, double? epsilon = null, string? distanceAlias = null)
            => RangeVectorSearchMethod.Create(radius, epsilon, distanceAlias);
        public static VectorSearchMethod NearestNeighbour(int count = NearestNeighbourVectorSearchMethod.DEFAULT_NEAREST_NEIGHBOUR_COUNT, int? maxTopCandidates = null, string? distanceAlias = null)
            => NearestNeighbourVectorSearchMethod.Create(count, maxTopCandidates, distanceAlias);
        private sealed class NearestNeighbourVectorSearchMethod : VectorSearchMethod
        {
            private static NearestNeighbourVectorSearchMethod? s_Default;
            internal static NearestNeighbourVectorSearchMethod Create(int count, int? maxTopCandidates, string? distanceAlias)
                => count == DEFAULT_NEAREST_NEIGHBOUR_COUNT & maxTopCandidates == null & distanceAlias == null
                ? (s_Default ??= new NearestNeighbourVectorSearchMethod(DEFAULT_NEAREST_NEIGHBOUR_COUNT, null, null))
                :  new(count, maxTopCandidates, distanceAlias);
            private NearestNeighbourVectorSearchMethod(int nearestNeighbourCount, int? maxTopCandidates, string? distanceAlias)
            {
                NearestNeighbourCount = nearestNeighbourCount;
                MaxTopCandidates = maxTopCandidates;
                DistanceAlias = distanceAlias;
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

            internal override int GetOwnArgsCount()
            {
                int count = 4;
                if (MaxTopCandidates != null) count += 2;
                if (DistanceAlias != null) count += 2;
                return count;
            }

            internal override void AddOwnArgs(List<object> args)
            {
                args.Add(Method);
                int tokens = 2;
                if (MaxTopCandidates != null) tokens += 2;
                if (DistanceAlias != null) tokens += 2;
                args.Add(tokens);
                args.Add("K");
                args.Add(NearestNeighbourCount);
                if (MaxTopCandidates != null)
                {
                    args.Add("EF_RUNTIME");
                    args.Add(MaxTopCandidates);
                }

                if (DistanceAlias != null)
                {
                    args.Add("YIELD_DISTANCE_AS");
                    args.Add(DistanceAlias);
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
}