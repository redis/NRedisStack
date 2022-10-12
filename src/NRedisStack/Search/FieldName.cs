using System.Collections.Generic;
using System.Text;

namespace NRedisStack.Search
{
    public class FieldName
    {
        private readonly string fieldName;
        private string alias;

        public FieldName(string name) : this(name, null) { }

        public FieldName(string name, string attribute)
        {
            this.fieldName = name;
            this.alias = attribute;
        }

        public int AddCommandArguments(List<object> args)
        {
            args.Add(fieldName);
            if (alias == null)
            {
                return 1;
            }

            args.Add("AS");
            args.Add(alias);
            return 3;
        }

        public static FieldName Of(string name)
        {
            return new FieldName(name);
        }

        public FieldName As(string attribute)
        {
            this.alias = attribute;
            return this;
        }

        public static FieldName[] convert(params string[] names)
        {
            if (names == null) return null;
            FieldName[] fields = new FieldName[names.Length];
            for (int i = 0; i < names.Length; i++)
                fields[i] = FieldName.Of(names[i]);

            return fields;
        }
    }
}