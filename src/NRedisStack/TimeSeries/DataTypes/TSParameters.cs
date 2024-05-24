namespace NRedisStack
{
    public class TsBaseParams
    {
        protected object[] parameters;

        internal TsBaseParams(object[] parameters)
        {
            this.parameters = parameters;
        }

        internal object[] GetAsArray()
        {
            return parameters.ToArray();
        }
    }

    public class TsCreateParams : TsBaseParams
    {
        internal TsCreateParams(object[] parameters) : base(parameters) { }
    }
    public class TsAlterParams : TsBaseParams
    {
        internal TsAlterParams(object[] parameters) : base(parameters) { }
    }

    public class TsAddParams : TsBaseParams
    {
        internal TsAddParams(object[] parameters) : base(parameters) { }
    }

    public class TsIncrByParams : TsBaseParams
    {
        internal TsIncrByParams(object[] parameters) : base(parameters) { }
    }

    public class TsDecrByParams : TsBaseParams
    {
        internal TsDecrByParams(object[] parameters) : base(parameters) { }
    }
}
