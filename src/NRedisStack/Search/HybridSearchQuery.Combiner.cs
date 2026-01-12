using System.Runtime.CompilerServices;

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

        public static Combiner Linear(double alpha = LinearCombiner.DEFAULT_ALPHA, double beta = LinearCombiner.DEFAULT_BETA, int? window = null)
            => LinearCombiner.Create(alpha, beta, window);

        internal abstract int GetOwnArgsCount();
        internal abstract void AddOwnArgs(List<object> args, int limit);

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
                int count = 4;
                if (_constant is not null) count += 2;
                return count;
            }

            private static readonly object BoxedDefaultWindow = 20;

            internal override void AddOwnArgs(List<object> args, int limit)
            {
                args.Add(Method);
                int tokens = 2;
                if (_constant is not null) tokens += 2;
                args.Add(tokens);
                args.Add("WINDOW");
                args.Add(_window ?? (limit > 0 ? limit : BoxedDefaultWindow));

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
            private readonly int? _window;

            private LinearCombiner(double alpha, double beta, int? window)
            {
                _alpha = alpha;
                _beta = beta;
                _window = window;
            }

            internal static LinearCombiner? s_Default;

            internal static LinearCombiner Create(double alpha, double beta, int? window = null)
                // ReSharper disable CompareOfFloatsByEqualityOperator
                => alpha == DEFAULT_ALPHA & beta == DEFAULT_BETA & window is null
                    // ReSharper restore CompareOfFloatsByEqualityOperator
                    ? (s_Default ??= new LinearCombiner(DEFAULT_ALPHA, DEFAULT_BETA, null))
                    : new(alpha, beta, window);

            internal const double DEFAULT_ALPHA = 0.3, DEFAULT_BETA = 0.7;

            internal override string Method => "LINEAR";

            public override string ToString() => $"{Method} {_alpha} {_beta} {_window}";

            internal override int GetOwnArgsCount() => _window.HasValue ? 8 : 6;

            private bool IsDefault => ReferenceEquals(this, s_Default);

            private static readonly object BoxedDefaultAlpha = DEFAULT_ALPHA, BoxedDefaultBeta = DEFAULT_BETA;

            internal override void AddOwnArgs(List<object> args, int limit)
            {
                args.Add(Method);
                args.Add(_window.HasValue ? 6 : 4);
                bool isDefault = ReferenceEquals(this, s_Default);
                args.Add("ALPHA");
                args.Add(isDefault ? BoxedDefaultAlpha : _alpha);
                args.Add("BETA");
                args.Add(isDefault ? BoxedDefaultBeta : _beta);
                if (_window.HasValue)
                {
                    args.Add("WINDOW");
                    args.Add(_window.GetValueOrDefault());
                }
            }
        }
    }
}