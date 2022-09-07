using NRedisStack.Literals.Enums;

namespace NRedisStack.Search.FT.CREATE
{
    public class FTCreateParams
    {
        private IndexDataType dataType;
        private List<string> prefix;
        private string filter;
        private string language;
        private string languageField;
        private double score;
        private string scoreField;
        private byte[] payloadField;
        private bool maxTextFields;
        private bool noOffsets;
        private long temporary;
        private bool noHL;
        private bool noFields;
        private bool noFreqs;
        private List<string> stopwords;
        private bool skipInitialScan;
    }
}