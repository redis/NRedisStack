namespace NRedisStack.Search
{
    public class FieldName
    {
        public string Name { get; }
        public string? Alias { get; private set; }

        public FieldName(string name) : this(name, null) { }

        public FieldName(string name, string? attribute)
        {
            this.Name = name;
            this.Alias = attribute;
        }

        public int AddCommandArguments(List<object> args)
        {
            args.Add(Name);
            if (Alias is null)
            {
                return 1;
            }

            args.Add("AS");
            args.Add(Alias);
            return 3;
        }

        public static FieldName Of(string name) => new(name);

        public FieldName As(string attribute)
        {
            this.Alias = attribute;
            return this;
        }

        public static implicit operator FieldName(string name) => new(name);
    }
}