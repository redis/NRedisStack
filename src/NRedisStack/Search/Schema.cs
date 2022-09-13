// .NET port of https://github.com/RedisLabs/JRediSearch/

using System;
using System.Collections.Generic;
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
        { // TODO: check if this is the correct order
            Text,
            Geo,
            Numeric,
            Tag,
            Vector
        }

        public class Field
        {
            public FieldName FieldName { get; }
            public string Name { get; } //TODO: check if this is needed
            public FieldType Type { get; }
            public bool Sortable { get; }
            public bool Unf { get; }
            public bool NoIndex { get; }

            internal Field(string name, FieldType type, bool sortable, bool noIndex = false, bool unf = false)
            : this(FieldName.Of(name), type, sortable, noIndex, unf)
            {
                Name = name;
            }

            internal Field(FieldName name, FieldType type, bool sortable, bool noIndex = false, bool unf = false)
            {
                FieldName = name;
                Type = type;
                Sortable = sortable;
                NoIndex = noIndex;
                if (unf && !sortable){
                    throw new ArgumentException("UNF can't be applied on a non-sortable field.");
                }
                Unf = unf;
            }

            internal /*virtual*/ void SerializeRedisArgs(List<object> args)
            {
                static object GetForRedis(FieldType type) => type switch
                {
                    FieldType.Text => "TEXT",
                    FieldType.Geo => "GEO",
                    FieldType.Numeric => "NUMERIC",
                    FieldType.Tag => "TAG",
                    FieldType.Vector => "VECTOR",
                    _ => throw new ArgumentOutOfRangeException(nameof(type)),
                };
                FieldName.AddCommandArguments(args);
                args.Add(GetForRedis(Type));
                SerializeTypeArgs(args);
                if (Sortable) { args.Add("SORTABLE"); }
                if (Unf) args.Add("UNF");
                if (NoIndex) { args.Add("NOINDEX"); }
            }
            internal virtual void SerializeTypeArgs(List<object> args) { }
        }

        public class TextField : Field
        {
            public double Weight { get; }
            public bool NoStem { get; }
            public string Phonetic { get; }

            public TextField(string name, double weight, bool sortable, bool noStem, string? phonetic, bool noIndex)
            : this(name, weight, sortable, noStem, phonetic, noIndex, false) { }

            public TextField(string name, double weight = 1.0, bool sortable = false, bool noStem = false, string? phonetic = null, bool noIndex = false, bool unNormalizedForm = false)
            : base(name, FieldType.Text, sortable, noIndex, unNormalizedForm)
            {
                Weight = weight;
                NoStem = noStem;
                Phonetic = phonetic;
            }

            public TextField(FieldName name, double weight, bool sortable, bool noStem, string? phonetic, bool noIndex)
            : this(name, weight, sortable, noStem, phonetic, noIndex, false) { }

            public TextField(FieldName name, double weight = 1.0, bool sortable = false, bool noStem = false, string? phonetic = null, bool noIndex = false, bool unNormalizedForm = false)
            : base(name, FieldType.Text, sortable, noIndex, unNormalizedForm)
            {
                Weight = weight;
                NoStem = noStem;
                Phonetic = phonetic;
            }

            internal override void SerializeTypeArgs(List<object> args)
            {
                // base.SerializeRedisArgs(args);
                if (Weight != 1.0)
                {
                    args.Add("WEIGHT");
                    args.Add(Weight);
                }
                if (NoStem) args.Add("NOSTEM");
                if (Phonetic != null)
                {
                    args.Add("PHONETIC");
                    args.Add(this.Phonetic);
                }

            }
        }

        public class TagField : Field
        {
            public string Separator { get; }
            public bool CaseSensitive { get; }

            internal TagField(string name, string separator = ",", bool caseSensitive = false, bool sortable = false, bool unNormalizedForm = false)
            : base(name, FieldType.Tag, sortable, unf: unNormalizedForm)
            {
                Separator = separator;
                CaseSensitive = caseSensitive;
            }

            internal TagField(FieldName name, string separator = ",", bool caseSensitive = false, bool sortable = false, bool unNormalizedForm = false)
            : base(name, FieldType.Tag, sortable, unf: unNormalizedForm)
            {
                Separator = separator;
                CaseSensitive = caseSensitive;
            }

            internal override void SerializeTypeArgs(List<object> args)
            {
                // base.SerializeRedisArgs(args);
                if (Separator != ",")
                {
                    // if (Sortable) args.Remove("SORTABLE");
                    // if (Unf) args.Remove("UNF");
                    args.Add("SEPARATOR");
                    args.Add(Separator);
                    // if (Sortable) args.Add("SORTABLE");
                    // if (Unf) args.Add("UNF");
                }
                if (CaseSensitive) args.Add("CASESENSITIVE");
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
            public Dictionary<string, object> attributes { get; }
            public VectorField(string name, VectorAlgo algorithm, Dictionary<string, object> attributes,
                               bool sortable = false, bool noIndex = false, bool unNormalizedForm = false)
                               : base(name, FieldType.Vector, sortable, noIndex, unNormalizedForm)
            {
                Algorithm = algorithm;
                this.attributes = attributes;
            }

            public VectorField(FieldName name, VectorAlgo algorithm, Dictionary<string, object> attributes,
                               bool sortable = false, bool noIndex = false, bool unNormalizedForm = false)
                               : base(name, FieldType.Vector, sortable, noIndex, unNormalizedForm)
            {
                Algorithm = algorithm;
                this.attributes = attributes;
            }

            internal override void SerializeTypeArgs(List<object> args)
            {
                args.Add("ALGORITHM");
                args.Add(Algorithm.ToString());
                foreach (var attribute in attributes)
                {
                    args.Add(attribute.Key);
                    args.Add(attribute.Value);
                }
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
        /// Add a text field to the schema.
        /// </summary>
        /// <param name="name">The field's name.</param>
        /// <param name="weight">Its weight, a positive floating point number.</param>
        /// <param name="sortable">If true, the text field can be sorted.</param>
        /// <param name="noStem"> Disable stemming when indexing its values.</param>
        /// <param name="phonetic">Declaring a text attribute as PHONETIC will perform phonetic matching on it in searches by default.</param>
        /// <param name="noIndex">Attributes can have the NOINDEX option, which means they will not be indexed.</param>
        /// <param name="unNormalizedForm">Set this to true to prevent the indexer from sorting on the normalized form.
        /// Normalied form is the field sent to lower case with all diaretics removed</param>
        /// <returns>The <see cref="Schema"/> object.</returns>
        public Schema AddTextField(string name, double weight = 1.0, bool sortable = false, bool noStem = false,
                                   string? phonetic = null, bool noIndex = false, bool unNormalizedForm = false)
        {
            Fields.Add(new TextField(name, weight, sortable, noStem, phonetic, noIndex, unNormalizedForm));
            return this;
        }

        /// <summary>
        /// Add a text field to the schema.
        /// </summary>
        /// <param name="name">The field's name.</param>
        /// <param name="weight">Its weight, a positive floating point number.</param>
        /// <param name="sortable">If true, the text field can be sorted.</param>
        /// <param name="noStem"> Disable stemming when indexing its values.</param>
        /// <param name="phonetic">Declaring a text attribute as PHONETIC will perform phonetic matching on it in searches by default.</param>
        /// <param name="noIndex">Attributes can have the NOINDEX option, which means they will not be indexed.</param>
        /// <param name="unNormalizedForm">Set this to true to prevent the indexer from sorting on the normalized form.
        /// <returns>The <see cref="Schema"/> object.</returns>
        public Schema AddTextField(FieldName name, double weight = 1.0, bool sortable = false, bool noStem = false,
                                   string? phonetic = null, bool noIndex = false, bool unNormalizedForm = false)
        {
            Fields.Add(new TextField(name, weight, sortable, noStem, phonetic, noIndex, unNormalizedForm));
            return this;
        }

        /// <summary>
        /// Add a text field that can be sorted on.
        /// </summary>
        /// <param name="name">The field's name.</param>
        /// <param name="weight">Its weight, a positive floating point number.</param>
        /// <param name="unNormalizedForm">Set this to true to prevent the indexer from sorting on the normalized form.
        /// Normalied form is the field sent to lower case with all diaretics removed</param>
        /// <returns>The <see cref="Schema"/> object.</returns>
        public Schema AddSortableTextField(string name, double weight = 1.0, bool unf = false)
        {
            Fields.Add(new TextField(name, weight, true, unNormalizedForm: unf));
            return this;
        }

        /// <summary>
        /// Add a text field that can be sorted on.
        /// </summary>
        /// <param name="name">The field's name.</param>
        /// <param name="weight">Its weight, a positive floating point number.</param>
        /// <returns>The <see cref="Schema"/> object.</returns>
        public Schema AddSortableTextField(string name, double weight) => AddSortableTextField(name, weight, false);

        /// <summary>
        /// Add a text field that can be sorted on.
        /// </summary>
        /// <param name="name">The field's name.</param>
        /// <param name="weight">Its weight, a positive floating point number.</param>
        /// <param name="unNormalizedForm">Set this to true to prevent the indexer from sorting on the normalized form.
        /// Normalied form is the field sent to lower case with all diaretics removed</param>
        /// <returns>The <see cref="Schema"/> object.</returns>
        public Schema AddSortableTextField(FieldName name, double weight = 1.0, bool unNormalizedForm = false)
        {
            Fields.Add(new TextField(name, weight, true, unNormalizedForm: unNormalizedForm));
            return this;
        }

        /// <summary>
        /// Add a text field that can be sorted on.
        /// </summary>
        /// <param name="name">The field's name.</param>
        /// <param name="weight">Its weight, a positive floating point number.</param>
        /// <returns>The <see cref="Schema"/> object.</returns>
        public Schema AddSortableTextField(FieldName name, double weight) => AddSortableTextField(name, weight, false);

        /// <summary>
        /// Add a numeric field to the schema.
        /// </summary>
        /// <param name="name">The field's name.</param>
        /// <returns>The <see cref="Schema"/> object.</returns>
        public Schema AddGeoField(string name)
        {
            Fields.Add(new Field(name, FieldType.Geo, false));
            return this;
        }

        /// <summary>
        /// Add a numeric field to the schema.
        /// </summary>
        /// <param name="name">The field's name.</param>
        /// <returns>The <see cref="Schema"/> object.</returns>
        public Schema AddGeoField(FieldName name)
        {
            Fields.Add(new Field(name, FieldType.Geo, false));
            return this;
        }

        /// <summary>
        /// Add a numeric field to the schema.
        /// </summary>
        /// <param name="name">The field's name.</param>
        /// <returns>The <see cref="Schema"/> object.</returns>
        public Schema AddNumericField(string name)
        {
            Fields.Add(new Field(name, FieldType.Numeric, false));
            return this;
        }

        /// <summary>
        /// Add a numeric field to the schema.
        /// </summary>
        /// <param name="name">The field's name.</param>
        /// <returns>The <see cref="Schema"/> object.</returns>
        public Schema AddNumericField(FieldName name)
        {
            Fields.Add(new Field(name, FieldType.Numeric, false));
            return this;
        }

        /// <summary>
        /// Add a numeric field that can be sorted on.
        /// </summary>
        /// <param name="name">The field's name.</param>
        /// <returns>The <see cref="Schema"/> object.</returns>
        public Schema AddSortableNumericField(string name)
        {
            Fields.Add(new Field(name, FieldType.Numeric, true));
            return this;
        }

        /// <summary>
        /// Add a numeric field that can be sorted on.
        /// </summary>
        /// <param name="name">The field's name.</param>
        /// <returns>The <see cref="Schema"/> object.</returns>
        public Schema AddSortableNumericField(FieldName name)
        {
            Fields.Add(new Field(name, FieldType.Numeric, true));
            return this;
        }

        /// <summary>
        /// Add a TAG field.
        /// </summary>
        /// <param name="name">The field's name.</param>
        /// <param name="separator">The tag separator.</param>
        /// <param name="caseSensitive">If true, Keeps the original letter cases of the tags.</param>
        /// <param name="sortable">If true, the field can be sorted.</param>
        /// <param name="unNormalizedForm">Set this to true to prevent the indexer from sorting on the normalized form.
        /// Normalied form is the field sent to lower case with all diaretics removed</param>
        /// <returns>The <see cref="Schema"/> object.</returns>
        public Schema AddTagField(string name, string separator = ",", bool caseSensitive = false, bool sortable = false, bool unNormalizedForm = false)
        {
            Fields.Add(new TagField(name, separator, caseSensitive, sortable, unNormalizedForm));
            return this;
        }

        /// <summary>
        /// Add a TAG field.
        /// </summary>
        /// <param name="name">The field's name.</param>
        /// <param name="separator">The tag separator.</param>
        /// <param name="caseSensitive">If true, Keeps the original letter cases of the tags.</param>
        /// <param name="sortable">If true, the field can be sorted.</param>
        /// <param name="unNormalizedForm">Set this to true to prevent the indexer from sorting on the normalized form.
        /// Normalied form is the field sent to lower case with all diaretics removed</param>
        /// <returns>The <see cref="Schema"/> object.</returns>
        public Schema AddTagField(FieldName name, string separator = ",", bool caseSensitive = false, bool sortable = false, bool unNormalizedForm = false)
        {
            Fields.Add(new TagField(name, separator, caseSensitive, sortable, unNormalizedForm));
            return this;
        }

        /// <summary>
        /// Add a sortable TAG field.
        /// </summary>
        /// <param name="name">The field's name.</param>
        /// <param name="separator">The tag separator.</param>
        /// <param name="unNormalizedForm">Set this to true to prevent the indexer from sorting on the normalized form.
        /// Normalied form is the field sent to lower case with all diaretics removed</param>
        /// <returns>The <see cref="Schema"/> object.</returns>
        public Schema AddSortableTagField(string name, string separator = ",", bool unNormalizedForm = false)
        {
            Fields.Add(new TagField(name, separator, sortable: true, unNormalizedForm: unNormalizedForm));
            return this;
        }

        /// <summary>
        /// Add a sortable TAG field.
        /// </summary>
        /// <param name="name">The field's name.</param>
        /// <param name="separator">The tag separator.</param>
        /// <param name="unNormalizedForm">Set this to true to prevent the indexer from sorting on the normalized form.
        /// Normalied form is the field sent to lower case with all diaretics removed</param>
        /// <returns>The <see cref="Schema"/> object.</returns>
        public Schema AddSortableTagField(FieldName name, string separator = ",", bool unNormalizedForm = false)
        {
            Fields.Add(new TagField(name, separator, sortable: true, unNormalizedForm: unNormalizedForm));
            return this;
        }

        /// <summary>
        /// Add a TAG field.
        /// </summary>
        /// <param name="name">The field's name.</param>
        /// <param name="algorithm">The vector similarity algorithm to use.</param>
        /// <param name="attribute">The algorithm attributes for the creation of the vector index.</param>
        /// <param name="sortable">If true, the field can be sorted.</param>
        /// <param name="noIndex">Attributes can have the NOINDEX option, which means they will not be indexed.</param>
        /// <param name="unNormalizedForm">Set this to true to prevent the indexer from sorting on the normalized form.
        /// Normalied form is the field sent to lower case with all diaretics removed</param>
        /// <returns>The <see cref="Schema"/> object.</returns>
        public Schema AddVectorField(string name, VectorAlgo algorithm, Dictionary<string, object> attributes,
                                     bool sortable = false, bool noIndex = false, bool unNormalizedForm = false)
        {
            Fields.Add(new VectorField(name, algorithm, attributes, sortable, noIndex, unNormalizedForm));
            return this;
        }

        /// <summary>
        /// Add a TAG field.
        /// </summary>
        /// <param name="name">The field's name.</param>
        /// <param name="algorithm">The vector similarity algorithm to use.</param>
        /// <param name="attribute">The algorithm attributes for the creation of the vector index.</param>
        /// <param name="sortable">If true, the field can be sorted.</param>
        /// <param name="noIndex">Attributes can have the NOINDEX option, which means they will not be indexed.</param>
        /// <param name="unNormalizedForm">Set this to true to prevent the indexer from sorting on the normalized form.
        /// Normalied form is the field sent to lower case with all diaretics removed</param>
        /// <returns>The <see cref="Schema"/> object.</returns>
        public Schema AddVectorField(FieldName name, VectorAlgo algorithm, Dictionary<string, object> attributes,
                                     bool sortable = false, bool noIndex = false, bool unNormalizedForm = false)
        {
            Fields.Add(new VectorField(name, algorithm, attributes, sortable, noIndex, unNormalizedForm));
            return this;
        }
    }
}
