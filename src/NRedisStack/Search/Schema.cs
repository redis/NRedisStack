using NRedisStack.Search.Literals;
using static NRedisStack.Search.Schema.GeoShapeField;
using static NRedisStack.Search.Schema.VectorField;

namespace NRedisStack.Search
{
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
            Vector
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
                if (Sortable) args.Add(FieldOptions.SORTABLE);
                if (Unf) args.Add(SearchArgs.UNF);
                if (MissingIndex) args.Add(FieldOptions.INDEXMISSING);
                if (EmptyIndex) args.Add(FieldOptions.INDEXEMPTY);

            }

            private void AddWeight(List<object> args)
            {
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
                if (Sortable) args.Add(FieldOptions.SORTABLE);
                if (Unf) args.Add(SearchArgs.UNF);
                if (MissingIndex) args.Add(FieldOptions.INDEXMISSING);
                if (EmptyIndex) args.Add(FieldOptions.INDEXEMPTY);
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
                if (Sortable) args.Add(FieldOptions.SORTABLE);
                if (MissingIndex) args.Add(FieldOptions.INDEXMISSING);
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
                if (Sortable) args.Add(FieldOptions.SORTABLE);
                if (MissingIndex) args.Add(FieldOptions.INDEXMISSING);
            }

        }

        public class VectorField : Field
        {
            public enum VectorAlgo
            {
                FLAT,
                HNSW
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

            public VectorField(string name, VectorAlgo algorithm, Dictionary<string, object>? attributes = null, bool missingIndex = false)
                               : this(FieldName.Of(name), algorithm, attributes, missingIndex) { }

            internal override void AddFieldTypeArgs(List<object> args)
            {
                args.Add(Algorithm.ToString());
                if (Attributes != null)
                {
                    args.Add(Attributes.Count() * 2);

                    foreach (var attribute in Attributes)
                    {
                        args.Add(attribute.Key);
                        args.Add(attribute.Value);
                    }
                }
                if (MissingIndex) args.Add(FieldOptions.INDEXMISSING);
            }
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
        /// <param name="unf">Set this to true to prevent the indexer from sorting on the normalized form.
        /// <param name="noIndex">Attributes can have the NOINDEX option, which means they will not be indexed.</param>
        /// <param name="separator">The tag separator.</param>
        /// <param name="caseSensitive">If true, Keeps the original letter cases of the tags.</param>
        /// Normalied form is the field sent to lower case with all diaretics removed</param>
        /// <param name="withSuffixTrie">Keeps a suffix trie with all terms which match the suffix.</param>
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
        /// Normalied form is the field sent to lower case with all diaretics removed</param>
        /// <param name="withSuffixTrie">Keeps a suffix trie with all terms which match the suffix.</param>
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
        /// <param name="attribute">The algorithm attributes for the creation of the vector index.</param>
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
        /// <param name="attribute">The algorithm attributes for the creation of the vector index.</param>
        /// <returns>The <see cref="Schema"/> object.</returns>
        public Schema AddVectorField(string name, VectorAlgo algorithm, Dictionary<string, object>? attributes = null, bool missingIndex = false)
        {
            Fields.Add(new VectorField(name, algorithm, attributes, missingIndex));
            return this;
        }
    }
}
