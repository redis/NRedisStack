namespace NRedisStack.Search;

public sealed partial class HybridSearchQuery
{
    public abstract class Combiner
    {
        internal abstract string Method { get; }

        /// <inheritdoc />
        public override string ToString() => Method;

        public static Combiner ReciprocalRankFusion(int? window = null, double? constant = null)
            => ReciprocalRankFusionCombiner.Create(window, constant);

        public static Combiner Linear(double alpha = LinearCombiner.DEFAULT_ALPHA, double beta = LinearCombiner.DEFAULT_BETA)
            => LinearCombiner.Create(alpha, beta);

        internal abstract int GetOwnArgsCount();
        internal abstract void AddOwnArgs(List<object> args);

        private sealed class ReciprocalRankFusionCombiner : Combiner
        {
            private readonly int? _window;
            private readonly double? _constant;

            private ReciprocalRankFusionCombiner(int? window, double? constant)
            {
                _window = window;
                _constant = constant;
            }

            internal static ReciprocalRankFusionCombiner? s_Default;

            internal static ReciprocalRankFusionCombiner Create(int? window, double? constant)
                => window is null & constant is null
                    ? (s_Default ??= new ReciprocalRankFusionCombiner(null, null))
                    : new(window, constant);

            internal override string Method => "RRF";
            public override string ToString() => $"{Method} {_window} {_constant}";

            internal override int GetOwnArgsCount()
            {
                int count = 2;
                if (_window is not null) count += 2;
                if (_constant is not null) count += 2;
                return count;
            }

            internal override void AddOwnArgs(List<object> args)
            {
                args.Add(Method);
                int tokens = 0;
                if (_window is not null) tokens += 2;
                if (_constant is not null) tokens += 2;
                args.Add(tokens);
                if (_window is not null)
                {
                    args.Add("WINDOW");
                    args.Add(_window);
                }

                if (_constant is not null)
                {
                    args.Add("CONSTANT");
                    args.Add(_constant);
                }
            }
        }

        private sealed class LinearCombiner : Combiner
        {
            private readonly double _alpha, _beta;

            private LinearCombiner(double alpha, double beta)
            {
                _alpha = alpha;
                _beta = beta;
            }

            internal static LinearCombiner? s_Default;

            internal static LinearCombiner Create(double alpha, double beta)
                // ReSharper disable CompareOfFloatsByEqualityOperator
                => alpha == DEFAULT_ALPHA & beta == DEFAULT_BETA
                    // ReSharper restore CompareOfFloatsByEqualityOperator
                    ? (s_Default ??= new LinearCombiner(DEFAULT_ALPHA, DEFAULT_BETA))
                    : new(alpha, beta);

            internal const double DEFAULT_ALPHA = 0.3, DEFAULT_BETA = 0.7;
            internal override string Method => "LINEAR";

            public override string ToString() => $"{Method} {_alpha} {_beta}";

            internal override int GetOwnArgsCount() => IsDefault ? 2 : 6;

            private bool IsDefault => ReferenceEquals(this, s_Default);

            internal override void AddOwnArgs(List<object> args)
            {
                args.Add(Method);
                if (IsDefault)
                {
                    args.Add(0);
                }
                else
                {
                    args.Add(4);
                    args.Add("ALPHA");
                    args.Add(_alpha);
                    args.Add("BETA");
                    args.Add(_beta);
                }
            }
        }
    }
}