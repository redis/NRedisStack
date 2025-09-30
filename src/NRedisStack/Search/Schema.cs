using System.Diagnostics;
using NRedisStack.Search.Literals;
using static NRedisStack.Search.Schema.GeoShapeField;
using static NRedisStack.Search.Schema.VectorField;

namespace NRedisStack.Search;

/// <summary>
/// Schema abstracts the schema definition when creating an index.
/// Documents can contain fields not mentioned in the schema, but the index will only index pre-defined fields
/// </summary>
public sealed class Schema
{
    public enum FieldType
    {
        Text,
        Geo,
        GeoShape,
        Numeric,
        Tag,
        Vector,
    }

    public class Field
    {
        public FieldName FieldName { get; }
        public FieldType Type { get; }

        internal Field(string name, FieldType type)
            : this(FieldName.Of(name), type) { }

        internal Field(FieldName name, FieldType type)
        {
            FieldName = name;
            Type = type;
        }

        internal void AddSchemaArgs(List<object> args)
        {
            static object GetForRedis(FieldType type) => type switch
            {
                FieldType.Text => "TEXT",
                FieldType.Geo => "GEO",
                FieldType.GeoShape => "GEOSHAPE",
                FieldType.Numeric => "NUMERIC",
                FieldType.Tag => "TAG",
                FieldType.Vector => "VECTOR",
                _ => throw new ArgumentOutOfRangeException(nameof(type)),
            };
            FieldName.AddCommandArguments(args);
            args.Add(GetForRedis(Type));
            AddFieldTypeArgs(args);
        }
        internal virtual void AddFieldTypeArgs(List<object> args) { }
    }

    public class TextField : Field
    {
        public double Weight { get; }
        public bool NoStem { get; }
        public string? Phonetic { get; }
        public bool Sortable { get; }
        public bool Unf { get; }
        public bool NoIndex { get; }
        public bool WithSuffixTrie { get; }
        public bool MissingIndex { get; }
        public bool EmptyIndex { get; }

        public TextField(FieldName name, double weight = 1.0, bool noStem = false,
            string? phonetic = null, bool sortable = false, bool unf = false,
            bool noIndex = false, bool withSuffixTrie = false, bool missingIndex = false, bool emptyIndex = false)
            : base(name, FieldType.Text)
        {
            Weight = weight;
            NoStem = noStem;
            Phonetic = phonetic;
            Sortable = sortable;
            if (unf && !sortable)
            {
                throw new ArgumentException("UNF can't be applied on a non-sortable field.");
            }
            Unf = unf;
            NoIndex = noIndex;
            WithSuffixTrie = withSuffixTrie;
            MissingIndex = missingIndex;
            EmptyIndex = emptyIndex;
        }

        public TextField(string name, double weight = 1.0, bool noStem = false,
            string? phonetic = null, bool sortable = false, bool unf = false,
            bool noIndex = false, bool withSuffixTrie = false, bool missingIndex = false, bool emptyIndex = false)
            : this(FieldName.Of(name), weight, noStem, phonetic, sortable, unf, noIndex, withSuffixTrie, missingIndex, emptyIndex) { }

        internal override void AddFieldTypeArgs(List<object> args)
        {
            if (NoStem) args.Add(SearchArgs.NOSTEM);
            if (NoIndex) args.Add(SearchArgs.NOINDEX);
            AddPhonetic(args);
            AddWeight(args);
            if (WithSuffixTrie) args.Add(SearchArgs.WITHSUFFIXTRIE);
            if (Unf) args.Add(SearchArgs.UNF);
            if (MissingIndex) args.Add(FieldOptions.INDEXMISSING);
            if (EmptyIndex) args.Add(FieldOptions.INDEXEMPTY);
            if (Sortable) args.Add(FieldOptions.SORTABLE);
        }

        private void AddWeight(List<object> args)
        {
            // ReSharper disable once CompareOfFloatsByEqualityOperator
            if (Weight != 1.0)
            {
                args.Add(SearchArgs.WEIGHT);
                args.Add(Weight);
            }
        }

        private void AddPhonetic(List<object> args)
        {
            if (Phonetic != null)
            {
                args.Add(SearchArgs.PHONETIC);
                args.Add(this.Phonetic);
            }
        }
    }

    public class TagField : Field
    {
        public bool Sortable { get; }
        public bool Unf { get; }
        public bool NoIndex { get; }
        public string Separator { get; }
        public bool CaseSensitive { get; }
        public bool WithSuffixTrie { get; }
        public bool MissingIndex { get; }
        public bool EmptyIndex { get; }

        internal TagField(FieldName name, bool sortable = false, bool unf = false,
            bool noIndex = false, string separator = ",",
            bool caseSensitive = false, bool withSuffixTrie = false, bool missingIndex = false, bool emptyIndex = false)
            : base(name, FieldType.Tag)
        {
            Sortable = sortable;
            Unf = unf;
            NoIndex = noIndex;
            Separator = separator;
            CaseSensitive = caseSensitive;
            WithSuffixTrie = withSuffixTrie;
            EmptyIndex = emptyIndex;
            MissingIndex = missingIndex;
        }

        internal TagField(string name, bool sortable = false, bool unf = false,
            bool noIndex = false, string separator = ",",
            bool caseSensitive = false, bool withSuffixTrie = false, bool missingIndex = false, bool emptyIndex = false)
            : this(FieldName.Of(name), sortable, unf, noIndex, separator, caseSensitive, withSuffixTrie, missingIndex, emptyIndex) { }

        internal override void AddFieldTypeArgs(List<object> args)
        {
            if (NoIndex) args.Add(SearchArgs.NOINDEX);
            if (WithSuffixTrie) args.Add(SearchArgs.WITHSUFFIXTRIE);
            if (Separator != ",")
            {

                args.Add(SearchArgs.SEPARATOR);
                args.Add(Separator);
            }
            if (CaseSensitive) args.Add(SearchArgs.CASESENSITIVE);
            if (Unf) args.Add(SearchArgs.UNF);
            if (MissingIndex) args.Add(FieldOptions.INDEXMISSING);
            if (EmptyIndex) args.Add(FieldOptions.INDEXEMPTY);
            if (Sortable) args.Add(FieldOptions.SORTABLE);
        }
    }

    public class GeoField : Field
    {
        public bool Sortable { get; }
        public bool NoIndex { get; }
        public bool MissingIndex { get; }

        internal GeoField(FieldName name, bool sortable = false, bool noIndex = false, bool missingIndex = false)
            : base(name, FieldType.Geo)
        {
            Sortable = sortable;
            NoIndex = noIndex;
            MissingIndex = missingIndex;
        }

        internal GeoField(string name, bool sortable = false, bool noIndex = false, bool missingIndex = false)
            : this(FieldName.Of(name), sortable, noIndex, missingIndex) { }

        internal override void AddFieldTypeArgs(List<object> args)
        {
            if (NoIndex) args.Add(SearchArgs.NOINDEX);
            if (MissingIndex) args.Add(FieldOptions.INDEXMISSING);
            if (Sortable) args.Add(FieldOptions.SORTABLE);
        }
    }

    public class GeoShapeField : Field
    {
        public enum CoordinateSystem
        {
            /// <summary>
            /// For cartesian (X,Y).
            /// </summary>
            FLAT,

            /// <summary>
            /// For geographic (lon, lat).
            /// </summary>
            SPHERICAL
        }
        private CoordinateSystem system { get; }
        public bool MissingIndex { get; }

        internal GeoShapeField(FieldName name, CoordinateSystem system, bool missingIndex = false)
            : base(name, FieldType.GeoShape)
        {
            this.system = system;
            MissingIndex = missingIndex;
        }

        internal GeoShapeField(string name, CoordinateSystem system, bool missingIndex = false)
            : this(FieldName.Of(name), system, missingIndex) { }

        internal override void AddFieldTypeArgs(List<object> args)
        {
            args.Add(system.ToString());
            if (MissingIndex) args.Add(FieldOptions.INDEXMISSING);
        }
    }

    public class NumericField : Field
    {
        public bool Sortable { get; }
        public bool NoIndex { get; }
        public bool MissingIndex { get; }

        internal NumericField(FieldName name, bool sortable = false, bool noIndex = false, bool missingIndex = false)
            : base(name, FieldType.Numeric)
        {
            Sortable = sortable;
            NoIndex = noIndex;
            MissingIndex = missingIndex;
        }

        internal NumericField(string name, bool sortable = false, bool noIndex = false, bool missingIndex = false)
            : this(FieldName.Of(name), sortable, noIndex, missingIndex) { }

        internal override void AddFieldTypeArgs(List<object> args)
        {
            if (NoIndex) args.Add(SearchArgs.NOINDEX);
            if (MissingIndex) args.Add(FieldOptions.INDEXMISSING);
            if (Sortable) args.Add(FieldOptions.SORTABLE);
        }
    }

    public class VectorField : Field
    {
        public enum VectorAlgo
        {
            FLAT,
            HNSW,
            SVS_VAMANA,
        }

        public VectorAlgo Algorithm { get; }

        public Dictionary<string, object>? Attributes { get; }
        public bool MissingIndex { get; }

        public VectorField(FieldName name, VectorAlgo algorithm, Dictionary<string, object>? attributes = null, bool missingIndex = false)
            : base(name, FieldType.Vector)
        {
            Algorithm = algorithm;
            Attributes = attributes;
            MissingIndex = missingIndex;
        }

        internal VectorField(FieldName name, VectorAlgo algorithm,
            VectorType type, int dimensions, VectorDistanceMetric distanceMetric,
            Dictionary<string, object>? attributes, bool missingIndex)
            : this(name, algorithm, attributes, missingIndex)
        {
            Type = type;
            Dimensions = dimensions;
            DistanceMetric = distanceMetric;
        }

        public VectorField(string name, VectorAlgo algorithm, Dictionary<string, object>? attributes = null, bool missingIndex = false)
            : this(FieldName.Of(name), algorithm, attributes, missingIndex) { }

        internal override void AddFieldTypeArgs(List<object> args)
        {
            args.Add(Algorithm switch
            {
                VectorAlgo.FLAT => "FLAT",
                VectorAlgo.HNSW => "HNSW",
                VectorAlgo.SVS_VAMANA => "SVS-VAMANA",
                _ => Algorithm.ToString(), // fallback
            });
            var attribs = Attributes;
            var count = DirectAttributeCount + (attribs?.Count ?? 0);
            args.Add(count * 2);
#if DEBUG
            int before = args.Count;
#endif
            AddDirectAttributes(args);
            if (attribs != null)
            {
                foreach (var attribute in attribs)
                {
                    args.Add(attribute.Key);
                    args.Add(attribute.Value);
                }
            }
#if DEBUG
            Debug.Assert(args.Count == (before + (count * 2)), "Arg count mismatch; check " + nameof(AddDirectAttributes) + " vs " + nameof(DirectAttributeCount));
#endif
            if (MissingIndex) args.Add(FieldOptions.INDEXMISSING);
        }

        // attributes handled inside the type-system (rather than via Attributes)

        /// <summary>
        /// The width, or number of dimensions, of the vector embeddings stored in this field. In other words, the number of floating point elements comprising the vector. DIM must be a positive integer. The vector used to query this field must have the exact dimensions as the field itself.
        /// </summary>
        public int Dimensions { get; set; } // = 0;
        /// <summary>
        /// The storage type for a vector field.
        /// </summary>
        public new VectorType Type { get; set; } = VectorType.NotSpecified;
        /// <summary>
        /// The distance metric to use for vector similarity search.
        /// </summary>
        public VectorDistanceMetric DistanceMetric { get; set; } = VectorDistanceMetric.NotSpecified;

        internal virtual int DirectAttributeCount
            => (Dimensions == 0 ? 0 : 1)
               + (Type == VectorType.NotSpecified ? 0 : 1)
               + (DistanceMetric == VectorDistanceMetric.NotSpecified ? 0 : 1);

        internal virtual void AddDirectAttributes(List<object> args)
        {
            if (Dimensions != 0)
            {
                args.Add("DIM");
                args.Add(Dimensions);
            }
            if (Type != VectorType.NotSpecified)
            {
                args.Add("TYPE");
                args.Add(Type switch
                {
                    VectorType.FLOAT32 => "FLOAT32",
                    VectorType.FLOAT64 => "FLOAT64",
                    VectorType.BFLOAT16 => "BFLOAT16",
                    VectorType.FLOAT16 => "FLOAT16",
                    _ => Type.ToString(), // fallback
                });
            }

            if (DistanceMetric != VectorDistanceMetric.NotSpecified)
            {
                args.Add("DISTANCE_METRIC");
                args.Add(DistanceMetric switch
                {
                    VectorDistanceMetric.EuclideanDistance => "L2",
                    VectorDistanceMetric.InnerProduct => "IP",
                    VectorDistanceMetric.CosineDistance => "COSINE",
                    _ => DistanceMetric.ToString(), // fallback
                });
            }
        }

        /// <summary>
        /// The storage type for a vector field.
        /// </summary>
        public enum VectorType
        {
            /// <summary>
            /// Not specified, or specified via <see cref="VectorField.Attributes"/>.
            /// </summary>
            NotSpecified = 0,
            /// <summary>
            /// 32-bit floating point.
            /// </summary>
            FLOAT32 = 1,
            /// <summary>
            /// 64-bit floating point.
            /// </summary>
            FLOAT64 = 2,
            /// <summary>
            /// 16-bit "brain" floating point
            /// </summary>
            BFLOAT16 = 3, // requires v2.10 or later.
            /// <summary>
            /// 16-bit floating point.
            /// </summary>
            FLOAT16 = 4, // requires v2.10 or later.
        }

        /// <summary>
        /// The distance metric to use for vector similarity search.
        /// </summary>
        public enum VectorDistanceMetric
        {
            /// <summary>
            /// Not specified, or specified via <see cref="VectorField.Attributes"/>.
            /// </summary>
            NotSpecified = 0,
            /// <summary>
            /// Euclidean distance between two vectors - this corresponds to the L2 option in Redis.
            /// </summary>
            EuclideanDistance = 1,
            /// <summary>
            /// Inner product of two vectors - this corresponds to the IP option in Redis.
            /// </summary>
            InnerProduct = 2,
            /// <summary>
            /// Cosine distance of two vectors - this corresponds to the COSINE option in Redis.
            /// </summary>
            CosineDistance = 3,
        }

        /// <summary>
        /// The distance metric to use for vector similarity search.
        /// </summary>
        public enum VectorCompressionAlgorithm
        {
            /// <summary>
            /// Not specified, or specified via <see cref="VectorField.Attributes"/>.
            /// </summary>
            NotSpecified = 0,
            LVQ8 = 1,
            LVQ4 = 2,
            // ReSharper disable once InconsistentNaming
            LVQ4x4 = 3,
            // ReSharper disable once InconsistentNaming
            LVQ4x8 = 4,
            // ReSharper disable once InconsistentNaming
            LeanVec4x8 = 5,
            // ReSharper disable once InconsistentNaming
            LeanVec8x8 = 6,
        }
    }


    /// <summary>
    /// A <see cref="VectorField"/> that uses the <see cref="VectorField.VectorAlgo.FLAT"/> algorithm.
    /// </summary>
    public class FlatVectorField(
        FieldName name,
        VectorType type,
        int dimensions,
        VectorDistanceMetric distanceMetric,
        Dictionary<string, object>? attributes = null,
        bool missingIndex = false)
        : VectorField(name, VectorAlgo.FLAT, type, dimensions, distanceMetric, attributes, missingIndex);

    /// <summary>
    /// A <see cref="VectorField"/> that uses the <see cref="VectorField.VectorAlgo.HNSW"/> algorithm.
    /// </summary>
    public class HnswVectorField(
        FieldName name,
        VectorType type,
        int dimensions,
        VectorDistanceMetric distanceMetric,
        Dictionary<string, object>? attributes = null,
        bool missingIndex = false)
        : VectorField(name, VectorAlgo.HNSW, type, dimensions, distanceMetric, attributes, missingIndex)
    {
        // optional attributes
        /// <summary>
        /// "M"; Max number of outgoing edges (connections) for each node in a graph layer. On layer zero, the max number of connections will be 2 * M. Higher values increase accuracy, but also increase memory usage and index build time. The default is 16.
        /// </summary>
        public int MaxOutgoingConnections { get; set; } = DEFAULT_M;

        /// <summary>
        /// "EF_CONSTRUCTION"; Max number of connected neighbors to consider during graph building. Higher values increase accuracy, but also increase index build time. The default is 200.
        /// </summary>
        public int MaxConnectedNeighbors { get; set; } = DEFAULT_EF_CONSTRUCTION;

        /// <summary>
        /// "EF_RUNTIME"; Max top candidates during KNN search. Higher values increase accuracy, but also increase search latency. The default is 10.
        /// </summary>
        public int MaxTopCandidates { get; set; } = DEFAULT_EF_RUNTIME;

        /// <summary>
        /// "EPSILON"; Relative factor that sets the boundaries in which a range query may search for candidates. That is, vector candidates whose distance from the query vector is radius * (1 + EPSILON) are potentially scanned, allowing more extensive search and more accurate results, at the expense of run time. The default is 0.01.
        /// </summary>
        public double BoundaryFactor { get; set; } = DEFAULT_EPSILON;

        internal const int DEFAULT_M = 16, DEFAULT_EF_CONSTRUCTION = 200, DEFAULT_EF_RUNTIME = 10;
        internal const double DEFAULT_EPSILON = 0.01;
        internal override int DirectAttributeCount
            => base.DirectAttributeCount
               + (MaxOutgoingConnections == DEFAULT_M ? 0 : 1)
               + (MaxConnectedNeighbors == DEFAULT_EF_CONSTRUCTION ? 0 : 1)
               + (MaxTopCandidates == DEFAULT_EF_RUNTIME ? 0 : 1)
               // ReSharper disable once CompareOfFloatsByEqualityOperator
               + (BoundaryFactor == DEFAULT_EPSILON ? 0 : 1);

        internal override void AddDirectAttributes(List<object> args)
        {
            base.AddDirectAttributes(args);
            if (MaxOutgoingConnections != DEFAULT_M)
            {
                args.Add("M");
                args.Add(MaxOutgoingConnections);
            }
            if (MaxConnectedNeighbors != DEFAULT_EF_CONSTRUCTION)
            {
                args.Add("EF_CONSTRUCTION");
                args.Add(MaxConnectedNeighbors);
            }
            if (MaxTopCandidates != DEFAULT_EF_RUNTIME)
            {
                args.Add("EF_RUNTIME");
                args.Add(MaxTopCandidates);
            }
            // ReSharper disable once CompareOfFloatsByEqualityOperator
            if (BoundaryFactor != DEFAULT_EPSILON)
            {
                args.Add("EPSILON");
                args.Add(BoundaryFactor);
            }
        }
    }

    /// <summary>
    /// A <see cref="VectorField"/> that uses the <see cref="VectorField.VectorAlgo.SVS_VAMANA"/> algorithm.
    /// </summary>
    public class SvsVanamaVectorField(
        FieldName name,
        VectorType type,
        int dimensions,
        VectorDistanceMetric distanceMetric,
        Dictionary<string, object>? attributes = null,
        bool missingIndex = false)
        : VectorField(name, VectorAlgo.SVS_VAMANA, type, dimensions, distanceMetric, attributes, missingIndex)
    {
        public VectorCompressionAlgorithm CompressionAlgorithm { get; set; } = VectorCompressionAlgorithm.NotSpecified;

        internal override int DirectAttributeCount
            => (CompressionAlgorithm == VectorCompressionAlgorithm.NotSpecified ? 0 : 1)
               + base.DirectAttributeCount
               + (ConstructionWindowSize == DEFAULT_CONSTRUCTION_WINDOW_SIZE ? 0 : 1)
               + (GraphMaxDegree == DEFAULT_GRAPH_MAX_DEGREE ? 0 : 1)
               + (SearchWindowSize == DEFAULT_SEARCH_WINDOW_SIZE ? 0 : 1)
               // ReSharper disable once CompareOfFloatsByEqualityOperator
               + (RangeSearchApproximationFactor == DEFAULT_EPSILON ? 0 : 1)
               + (TrainingThreshold == 0 ? 0 : 1)
               + (ReducedDimensions == 0 ? 0 : 1);

        internal override void AddDirectAttributes(List<object> args)
        {
            if (CompressionAlgorithm != VectorCompressionAlgorithm.NotSpecified)
            {
                args.Add("COMPRESSION");
                args.Add(CompressionAlgorithm switch
                {
                    VectorCompressionAlgorithm.LVQ8 => "LVQ8",
                    VectorCompressionAlgorithm.LVQ4 => "LVQ4",
                    VectorCompressionAlgorithm.LVQ4x4 => "LVQ4x4",
                    VectorCompressionAlgorithm.LVQ4x8 => "LVQ4x8",
                    VectorCompressionAlgorithm.LeanVec4x8 => "LeanVec4x8",
                    VectorCompressionAlgorithm.LeanVec8x8 => "LeanVec8x8",
                    _ => CompressionAlgorithm.ToString(), // fallback
                });
            }
            base.AddDirectAttributes(args);
            if (ConstructionWindowSize != DEFAULT_CONSTRUCTION_WINDOW_SIZE)
            {
                args.Add("CONSTRUCTION_WINDOW_SIZE");
                args.Add(ConstructionWindowSize);
            }
            if (GraphMaxDegree != DEFAULT_GRAPH_MAX_DEGREE)
            {
                args.Add("GRAPH_MAX_DEGREE");
                args.Add(GraphMaxDegree);
            }
            if (SearchWindowSize != DEFAULT_SEARCH_WINDOW_SIZE)
            {
                args.Add("SEARCH_WINDOW_SIZE");
                args.Add(SearchWindowSize);
            }
            // ReSharper disable once CompareOfFloatsByEqualityOperator
            if (RangeSearchApproximationFactor != DEFAULT_EPSILON)
            {
                args.Add("EPSILON");
                args.Add(RangeSearchApproximationFactor);
            }
            if (TrainingThreshold != 0)
            {
                args.Add("TRAINING_THRESHOLD");
                args.Add(TrainingThreshold);
            }
            if (ReducedDimensions != 0)
            {
                args.Add("REDUCE");
                args.Add(ReducedDimensions);
            }
        }

        public int ConstructionWindowSize { get; set; } = DEFAULT_CONSTRUCTION_WINDOW_SIZE;
        public int GraphMaxDegree { get; set; } = DEFAULT_GRAPH_MAX_DEGREE;
        public int SearchWindowSize { get; set; } = DEFAULT_SEARCH_WINDOW_SIZE;
        public double RangeSearchApproximationFactor { get; set; } = DEFAULT_EPSILON;
        /// <summary>
        /// Number of vectors after which training is triggered; defaults to 10 * DEFAULT_BLOCK_SIZE (or the provided value, limited to 100 * DEFAULT_BLOCK_SIZE) where DEFAULT_BLOCK_SIZE = 1024
        /// </summary>
        public int TrainingThreshold { get; set; }
        /// <summary>
        /// The dimension used when using LeanVec compression for dimensionality reduction; defaults to dim/2 (applicable only with compression of type LeanVec, should always be &lt; dim)
        /// </summary>
        public int ReducedDimensions { get; set; }

        internal const int DEFAULT_CONSTRUCTION_WINDOW_SIZE = 200, DEFAULT_GRAPH_MAX_DEGREE = 32, DEFAULT_SEARCH_WINDOW_SIZE = 10;
        internal const double DEFAULT_EPSILON = 0.01;
    }

    public List<Field> Fields { get; } = new List<Field>();

    /// <summary>
    /// Add a field to the schema.
    /// </summary>
    /// <param name="field">The <see cref="Field"/> to add.</param>
    /// <returns>The <see cref="Schema"/> object.</returns>
    public Schema AddField(Field field)
    {
        Fields.Add(field ?? throw new ArgumentNullException(nameof(field)));
        return this;
    }

    /// <summary>
    /// Add a Text field to the schema.
    /// </summary>
    /// <param name="name">The field's name.</param>
    /// <param name="weight">Its weight, a positive floating point number.</param>
    /// <param name="sortable">If true, the text field can be sorted.</param>
    /// <param name="noStem"> Disable stemming when indexing its values.</param>
    /// <param name="phonetic">Declaring a text attribute as PHONETIC will perform phonetic matching on it in searches by default.</param>
    /// <param name="noIndex">Attributes can have the NOINDEX option, which means they will not be indexed.</param>
    /// <param name="unf">Set this to true to prevent the indexer from sorting on the normalized form.
    /// Normalied form is the field sent to lower case with all diaretics removed</param>
    /// <param name="withSuffixTrie">Keeps a suffix trie with all terms which match the suffix.</param>
    /// <param name="missingIndex"> search for missing values, that is, documents that do not contain a specific field. 
    /// Note the difference between a field with an empty value and a document with a missing value. 
    /// By default, missing values are not indexed.</param>
    /// <param name="emptyIndex">  allows you to index and search for empty strings. By default, empty strings are not indexed.</param>
    /// <returns>The <see cref="Schema"/> object.</returns>
    public Schema AddTextField(string name, double weight = 1.0, bool sortable = false, bool unf = false, bool noStem = false,
        string? phonetic = null, bool noIndex = false, bool withSuffixTrie = false, bool missingIndex = false, bool emptyIndex = false)
    {
        Fields.Add(new TextField(name, weight, noStem, phonetic, sortable, unf, noIndex, withSuffixTrie, missingIndex, emptyIndex));
        return this;
    }

    /// <summary>
    /// Add a Text field to the schema.
    /// </summary>
    /// <param name="name">The field's name.</param>
    /// <param name="weight">Its weight, a positive floating point number.</param>
    /// <param name="sortable">If true, the text field can be sorted.</param>
    /// <param name="noStem"> Disable stemming when indexing its values.</param>
    /// <param name="phonetic">Declaring a text attribute as PHONETIC will perform phonetic matching on it in searches by default.</param>
    /// <param name="noIndex">Attributes can have the NOINDEX option, which means they will not be indexed.</param>
    /// <param name="unf">Set this to true to prevent the indexer from sorting on the normalized form.
    /// Normalied form is the field sent to lower case with all diaretics removed</param>
    /// <param name="withSuffixTrie">Keeps a suffix trie with all terms which match the suffix.</param>
    /// <param name="missingIndex"> search for missing values, that is, documents that do not contain a specific field. 
    /// Note the difference between a field with an empty value and a document with a missing value. 
    /// By default, missing values are not indexed.</param>
    /// <param name="emptyIndex">  allows you to index and search for empty strings. By default, empty strings are not indexed.</param>
    /// <returns>The <see cref="Schema"/> object.</returns>
    public Schema AddTextField(FieldName name, double weight = 1.0, bool sortable = false, bool unf = false, bool noStem = false,
        string? phonetic = null, bool noIndex = false, bool withSuffixTrie = false, bool missingIndex = false, bool emptyIndex = false)
    {
        Fields.Add(new TextField(name, weight, noStem, phonetic, sortable, unf, noIndex, withSuffixTrie, missingIndex, emptyIndex));
        return this;
    }

    /// <summary>
    /// Add a GeoShape field to the schema.
    /// </summary>
    /// <param name="name">The field's name.</param>
    /// <param name="system">The coordinate system to use.</param>
    /// <param name="missingIndex"> search for missing values, that is, documents that do not contain a specific field. 
    /// Note the difference between a field with an empty value and a document with a missing value. 
    /// By default, missing values are not indexed.</param>
    /// <returns>The <see cref="Schema"/> object.</returns>
    public Schema AddGeoShapeField(string name, CoordinateSystem system, bool missingIndex = false)
    {
        Fields.Add(new GeoShapeField(name, system, missingIndex));
        return this;
    }

    /// <summary>
    /// Add a GeoShape field to the schema.
    /// </summary>
    /// <param name="name">The field's name.</param>
    /// <param name="system">The coordinate system to use.</param>
    /// <param name="missingIndex"> search for missing values, that is, documents that do not contain a specific field. 
    /// Note the difference between a field with an empty value and a document with a missing value. 
    /// By default, missing values are not indexed.</param>
    /// <returns>The <see cref="Schema"/> object.</returns>
    public Schema AddGeoShapeField(FieldName name, CoordinateSystem system, bool missingIndex = false)
    {
        Fields.Add(new GeoShapeField(name, system, missingIndex));
        return this;
    }

    /// <summary>
    /// Add a Geo field to the schema.
    /// </summary>
    /// <param name="name">The field's name.</param>
    /// <param name="sortable">If true, the text field can be sorted.</param>
    /// <param name="noIndex">Attributes can have the NOINDEX option, which means they will not be indexed.</param>
    /// <param name="missingIndex"> search for missing values, that is, documents that do not contain a specific field. 
    /// Note the difference between a field with an empty value and a document with a missing value. 
    /// By default, missing values are not indexed.</param>
    /// <returns>The <see cref="Schema"/> object.</returns>
    public Schema AddGeoField(FieldName name, bool sortable = false, bool noIndex = false, bool missingIndex = false)
    {
        Fields.Add(new GeoField(name, sortable, noIndex, missingIndex));
        return this;
    }

    /// <summary>
    /// Add a Geo field to the schema.
    /// </summary>
    /// <param name="name">The field's name.</param>
    /// <param name="sortable">If true, the text field can be sorted.</param>
    /// <param name="noIndex">Attributes can have the NOINDEX option, which means they will not be indexed.</param>
    /// <param name="missingIndex"> search for missing values, that is, documents that do not contain a specific field. 
    /// Note the difference between a field with an empty value and a document with a missing value. 
    /// By default, missing values are not indexed.</param>
    /// <returns>The <see cref="Schema"/> object.</returns>
    public Schema AddGeoField(string name, bool sortable = false, bool noIndex = false, bool missingIndex = false)
    {
        Fields.Add(new GeoField(name, sortable, noIndex, missingIndex));
        return this;
    }

    /// <summary>
    /// Add a Numeric field to the schema.
    /// </summary>
    /// <param name="name">The field's name.</param>
    /// <param name="sortable">If true, the text field can be sorted.</param>
    /// <param name="noIndex">Attributes can have the NOINDEX option, which means they will not be indexed.</param>
    /// <param name="missingIndex"> search for missing values, that is, documents that do not contain a specific field. 
    /// Note the difference between a field with an empty value and a document with a missing value. 
    /// By default, missing values are not indexed.</param>
    /// <returns>The <see cref="Schema"/> object.</returns>
    public Schema AddNumericField(FieldName name, bool sortable = false, bool noIndex = false, bool missingIndex = false)
    {
        Fields.Add(new NumericField(name, sortable, noIndex, missingIndex));
        return this;
    }

    /// <summary>
    /// Add a Numeric field to the schema.
    /// </summary>
    /// <param name="name">The field's name.</param>
    /// <param name="sortable">If true, the text field can be sorted.</param>
    /// <param name="noIndex">Attributes can have the NOINDEX option, which means they will not be indexed.</param>
    /// <param name="missingIndex"> search for missing values, that is, documents that do not contain a specific field. 
    /// Note the difference between a field with an empty value and a document with a missing value. 
    /// By default, missing values are not indexed.</param>
    /// <returns>The <see cref="Schema"/> object.</returns>
    public Schema AddNumericField(string name, bool sortable = false, bool noIndex = false, bool missingIndex = false)
    {
        Fields.Add(new NumericField(name, sortable, noIndex, missingIndex));
        return this;
    }

    /// <summary>
    /// Add a Tag field to the schema.
    /// </summary>
    /// <param name="name">The field's name.</param>
    /// <param name="sortable">If true, the field can be sorted.</param>
    /// <param name="unf">Set this to true to prevent the indexer from sorting on the normalized form.</param>
    /// <param name="noIndex">Attributes can have the NOINDEX option, which means they will not be indexed.</param>
    /// <param name="separator">The tag separator.</param>
    /// <param name="caseSensitive">If true, Keeps the original letter cases of the tags.
    /// Normalized form is the field sent to lower case with all diaretics removed</param>
    /// <param name="withSuffixTrie">Keeps a suffix trie with all terms which match the suffix.</param>
    /// <param name="missingIndex"> search for missing values, that is, documents that do not contain a specific field. 
    /// Note the difference between a field with an empty value and a document with a missing value. 
    /// By default, missing values are not indexed.</param>
    /// <param name="emptyIndex">  allows you to index and search for empty strings. By default, empty strings are not indexed.</param>
    /// <returns>The <see cref="Schema"/> object.</returns>
    public Schema AddTagField(FieldName name, bool sortable = false, bool unf = false,
        bool noIndex = false, string separator = ",",
        bool caseSensitive = false, bool withSuffixTrie = false, bool missingIndex = false, bool emptyIndex = false)
    {
        Fields.Add(new TagField(name, sortable, unf, noIndex, separator, caseSensitive, withSuffixTrie, missingIndex, emptyIndex));
        return this;
    }

    /// <summary>
    /// Add a Tag field to the schema.
    /// </summary>
    /// <param name="name">The field's name.</param>
    /// <param name="sortable">If true, the field can be sorted.</param>
    /// <param name="unf">Set this to true to prevent the indexer from sorting on the normalized form.
    /// <param name="noIndex">Attributes can have the NOINDEX option, which means they will not be indexed.</param>
    /// <param name="separator">The tag separator.</param>
    /// <param name="caseSensitive">If true, Keeps the original letter cases of the tags.</param>
    /// Normalized form is the field sent to lower case with all diaretics removed</param>
    /// <param name="withSuffixTrie">Keeps a suffix trie with all terms which match the suffix.</param>
    /// <param name="missingIndex"> search for missing values, that is, documents that do not contain a specific field. 
    /// Note the difference between a field with an empty value and a document with a missing value. 
    /// By default, missing values are not indexed.</param>
    /// <param name="emptyIndex">  allows you to index and search for empty strings. By default, empty strings are not indexed.</param>
    /// <returns>The <see cref="Schema"/> object.</returns>
    public Schema AddTagField(string name, bool sortable = false, bool unf = false,
        bool noIndex = false, string separator = ",",
        bool caseSensitive = false, bool withSuffixTrie = false, bool missingIndex = false, bool emptyIndex = false)
    {
        Fields.Add(new TagField(name, sortable, unf, noIndex, separator, caseSensitive, withSuffixTrie, missingIndex, emptyIndex));
        return this;
    }

    /// <summary>
    /// Add a Vector field to the schema.
    /// </summary>
    /// <param name="name">The field's name.</param>
    /// <param name="algorithm">The vector similarity algorithm to use.</param>
    /// <param name="attributes">The algorithm attributes for the creation of the vector index.</param>
    /// <param name="missingIndex"> search for missing values, that is, documents that do not contain a specific field. 
    /// Note the difference between a field with an empty value and a document with a missing value. 
    /// By default, missing values are not indexed.</param>
    /// <returns>The <see cref="Schema"/> object.</returns>
    public Schema AddVectorField(FieldName name, VectorAlgo algorithm, Dictionary<string, object>? attributes = null, bool missingIndex = false)
    {
        Fields.Add(new VectorField(name, algorithm, attributes, missingIndex));
        return this;
    }

    /// <summary>
    /// Add a Vector field to the schema.
    /// </summary>
    /// <param name="name">The field's name.</param>
    /// <param name="algorithm">The vector similarity algorithm to use.</param>
    /// <param name="attributes">The algorithm attributes for the creation of the vector index.</param>
    /// <param name="missingIndex"> search for missing values, that is, documents that do not contain a specific field. 
    /// Note the difference between a field with an empty value and a document with a missing value. 
    /// By default, missing values are not indexed.</param>
    /// <returns>The <see cref="Schema"/> object.</returns>
    public Schema AddVectorField(string name, VectorAlgo algorithm, Dictionary<string, object>? attributes = null, bool missingIndex = false)
    {
        Fields.Add(new VectorField(name, algorithm, attributes, missingIndex));
        return this;
    }

    /// <summary>
    /// Add a <see href="VectorAlgo.FLAT"/> vector to the schema.
    /// </summary>
    public Schema AddFlatVectorField(FieldName name, VectorType type, int dimensions, VectorDistanceMetric distanceMetric, Dictionary<string, object>? attributes = null, bool missingIndex = false)
    {
        Fields.Add(new FlatVectorField(name, type, dimensions, distanceMetric, attributes, missingIndex));
        return this;
    }

    /// <summary>
    /// Add a <see href="VectorAlgo.HNSW"/> vector to the schema.
    /// </summary>
    public Schema AddHnswVectorField(FieldName name, VectorType type, int dimensions, VectorDistanceMetric distanceMetric,
        int maxOutgoingConnections = HnswVectorField.DEFAULT_M,
        int maxConnectedNeighbors = HnswVectorField.DEFAULT_EF_CONSTRUCTION,
        int maxTopCandidates = HnswVectorField.DEFAULT_EF_RUNTIME,
        double boundaryFactor = HnswVectorField.DEFAULT_EPSILON,
        Dictionary<string, object>? attributes = null, bool missingIndex = false)
    {
        Fields.Add(new HnswVectorField(name, type, dimensions, distanceMetric, attributes, missingIndex)
        {
            MaxOutgoingConnections = maxOutgoingConnections,
            MaxConnectedNeighbors = maxConnectedNeighbors,
            MaxTopCandidates = maxTopCandidates,
            BoundaryFactor = boundaryFactor,
        });
        return this;
    }

    /// <summary>
    /// Add a <see href="VectorAlgo.SVS_VAMANA"/> vector to the schema.
    /// </summary>
    /// <remarks>Note that <c>reducedDimensions</c> is only applicable when using LeanVec compression.</remarks>
    public Schema AddSvsVanamaVectorField(FieldName name, VectorType type, int dimensions, VectorDistanceMetric distanceMetric,
        VectorCompressionAlgorithm compressionAlgorithm = VectorCompressionAlgorithm.NotSpecified,
        int constructionWindowSize = SvsVanamaVectorField.DEFAULT_CONSTRUCTION_WINDOW_SIZE,
        int graphMaxDegree = SvsVanamaVectorField.DEFAULT_GRAPH_MAX_DEGREE,
        int searchWindowSize = SvsVanamaVectorField.DEFAULT_SEARCH_WINDOW_SIZE,
        double rangeSearchApproximationFactor = SvsVanamaVectorField.DEFAULT_EPSILON,
        int trainingThreshold = 0,
        int reducedDimensions = 0,
        Dictionary<string, object>? attributes = null, bool missingIndex = false)
    {
        Fields.Add(new SvsVanamaVectorField(name, type, dimensions, distanceMetric, attributes, missingIndex)
        {
            CompressionAlgorithm = compressionAlgorithm,
            ConstructionWindowSize = constructionWindowSize,
            GraphMaxDegree = graphMaxDegree,
            SearchWindowSize = searchWindowSize,
            RangeSearchApproximationFactor = rangeSearchApproximationFactor,
            TrainingThreshold = trainingThreshold,
            ReducedDimensions = reducedDimensions,
        });
        return this;
    }
}