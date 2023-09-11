using NRedisStack.Search.Literals;
using NRedisStack.Search.Literals.Enums;
namespace NRedisStack.Search
{
    public class FTCreateParams
    {
        private IndexDataType? dataType;
        private List<string>? prefixes;
        private string? filter;
        private string? language;
        private string? languageField;
        private double score;
        private string? scoreField;
        private string? payloadField;
        private bool maxTextFields;
        private bool noOffsets;
        private long temporary;
        private bool noHL;
        private bool noFields;
        private bool noFreqs;
        private List<string>? stopwords;
        private bool skipInitialScan;

        public FTCreateParams()
        {
        }

        public static FTCreateParams CreateParams()
        {
            return new FTCreateParams();
        }

        /// <summary>
        /// Currently supports HASH (default) and JSON. To index JSON, you must have the RedisJSON module
        /// installed.
        /// </summary>
        public FTCreateParams On(IndexDataType dataType)
        {
            this.dataType = dataType;
            return this;
        }

        /// <summary>
        ///  Tells the index which keys it should index. You can add several prefixes to index.
        /// </summary>
        public FTCreateParams Prefix(params string[] prefixes)
        {
            if (this.prefixes == null)
            {
                this.prefixes = new List<string>(prefixes.Length);
            }
            this.prefixes.AddRange(prefixes);
            return this;
        }

        /// <summary>
        ///  This method can be chained to add multiple prefixes.
        ///  @see FTCreateParams#prefix(java.lang.params string[])
        /// </summary>
        public FTCreateParams AddPrefix(string prefix)
        {
            if (this.prefixes == null)
            {
                this.prefixes = new List<string>();
            }
            this.prefixes.Add(prefix);
            return this;
        }

        /// <summary>
        ///  A filter expression with the full RediSearch aggregation expression language.
        /// </summary>
        public FTCreateParams Filter(string filter)
        {
            this.filter = filter;
            return this;
        }

        /// <summary>
        /// default language for documents in the index.
        /// </summary>
        public FTCreateParams Language(string defaultLanguage)
        {
            this.language = defaultLanguage;
            return this;
        }

        /// <summary>
        ///  Document attribute set as the document language.
        /// </summary>
        public FTCreateParams LanguageField(string languageAttribute)
        {
            this.languageField = languageAttribute;
            return this;
        }

        /// <summary>
        ///  Default score for documents in the index.
        /// </summary>
        public FTCreateParams Score(double defaultScore)
        {
            this.score = defaultScore;
            return this;
        }

        /// <summary>
        ///  Document attribute that you use as the document rank based on the user ranking.
        ///  Ranking must be between 0.0 and 1.0.
        /// </summary>
        public FTCreateParams ScoreField(string scoreField)
        {
            this.scoreField = scoreField;
            return this;
        }

        /// <summary>
        ///  Document attribute that you use as a binary safe payload string to the document that can be
        ///  evaluated at query time by a custom scoring function or retrieved to the client.
        /// </summary>
        public FTCreateParams PayloadField(string payloadAttribute)
        {
            //TODO: check if this is correct
            // Array.Copy(this.payloadField, payloadAttribute, payloadAttribute.Length);
            this.payloadField = payloadAttribute;
            return this;
        }


        /// <summary>
        ///  Forces RediSearch to encode indexes as if there were more than 32 text attributes.
        /// </summary>
        public FTCreateParams MaxTextFields()
        {
            this.maxTextFields = true;
            return this;
        }

        /// <summary>
        ///  Does not store term offsets for documents. It saves memory, but does not allow exact searches
        ///  or highlighting.
        /// </summary>
        public FTCreateParams NoOffsets()
        {
            this.noOffsets = true;
            return this;
        }

        /// <summary>
        ///  Creates a lightweight temporary index that expires after a specified period of inactivity.
        /// </summary>
        public FTCreateParams Temporary(long seconds)
        {
            this.temporary = seconds;
            return this;
        }

        /// <summary>
        ///  Conserves storage space and memory by disabling highlighting support.
        /// </summary>
        public FTCreateParams NoHL()
        {
            this.noHL = true;
            return this;
        }

        /// <summary>
        ///  @see FTCreateParams#noHL()
        /// </summary>
        public FTCreateParams NoHighlights()
        {
            return NoHL();
        }

        /// <summary>
        ///  Does not store attribute bits for each term. It saves memory, but it does not allow filtering
        ///  by specific attributes.
        /// </summary>
        public FTCreateParams NoFields()
        {
            this.noFields = true;
            return this;
        }

        /// <summary>
        ///  Avoids saving the term frequencies in the index. It saves memory, but does not allow sorting
        ///  based on the frequencies of a given term within the document.
        /// </summary>
        public FTCreateParams NoFreqs()
        {
            this.noFreqs = true;
            return this;
        }

        /// <summary>
        ///  Sets the index with a custom stopword list, to be ignored during indexing and search time.
        /// </summary>
        public FTCreateParams Stopwords(params string[] stopwords)
        {
            this.stopwords = stopwords.ToList();
            return this;
        }

        /// <summary>
        ///  The index does not have stopwords, not even the default ones.
        /// </summary>
        public FTCreateParams NoStopwords()
        {
            this.stopwords = new List<string> { };
            return this;
        }

        /// <summary>
        ///  Does not scan and index.
        /// </summary>
        public FTCreateParams SkipInitialScan()
        {
            this.skipInitialScan = true;
            return this;
        }

        public void AddParams(List<object> args)
        {

            if (dataType != default(IndexDataType))
            {
                args.Add("ON");
                args.Add(dataType.ToString());
            }

            if (prefixes != null)
            {
                args.Add(SearchArgs.PREFIX);
                args.Add(prefixes.Count);
                foreach (var prefix in prefixes)
                    if (prefix != null)
                        args.Add(prefix);
            }

            if (filter != null)
            {
                args.Add(SearchArgs.FILTER);
                args.Add(filter);
            }

            if (language != null)
            {
                args.Add(SearchArgs.LANGUAGE);
                args.Add(language);
            }
            if (languageField != null)
            {
                args.Add(SearchArgs.LANGUAGE_FIELD);
                args.Add(languageField);
            }

            if (score != default(double))
            {
                args.Add(SearchArgs.SCORE);
                args.Add(score);
            }
            if (scoreField != null)
            {
                args.Add(SearchArgs.SCORE_FIELD);
                args.Add(scoreField);
            }

            if (payloadField != null)
            {
                args.Add(SearchArgs.PAYLOAD_FIELD);
                args.Add(payloadField);
            }

            if (maxTextFields)
            {
                args.Add(SearchArgs.MAXTEXTFIELDS);
            }
            //[TEMPORARY seconds] seposed to be here
            if (noOffsets)
            {
                args.Add(SearchArgs.NOOFFSETS);
            }

            if (temporary != default(long))
            {
                args.Add(SearchArgs.TEMPORARY);
                args.Add(temporary);
            }

            if (noHL)
            {
                args.Add(SearchArgs.NOHL);
            }

            if (noFields)
            {
                args.Add(SearchArgs.NOFIELDS);
            }

            if (noFreqs)
            {
                args.Add(SearchArgs.NOFREQS);
            }

            if (stopwords != null)
            {
                args.Add(SearchArgs.STOPWORDS);
                args.Add(stopwords.Count);
                stopwords.ForEach(w => args.Add(w));
            }

            if (skipInitialScan)
            {
                args.Add(SearchArgs.SKIPINITIALSCAN);
            }
        }
    }
}