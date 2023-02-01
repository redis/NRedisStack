using StackExchange.Redis;

namespace NRedisStack
{
    public class Transactions
    {
        public Transactions(IConnectionMultiplexer muxer)
        {
            _transaction = muxer.GetDatabase().CreateTransaction();
        }

        public Transactions(IDatabase db)
        {
            _transaction = db.CreateTransaction();
        }

        private ITransaction _transaction;

        public ConditionResult AddCondition(Condition condition)
        {
           return _transaction.AddCondition(condition);
        }

        public bool Execute()
        {
            return _transaction.Execute();
        }

        public Task<bool> ExecuteAsync()
        {
            return _transaction.ExecuteAsync();
        }


        public IBloomCommandsAsync Bf => new BloomCommandsAsync(_transaction);
        public ICmsCommandsAsync Cms => new CmsCommandsAsync(_transaction);
        public ICuckooCommandsAsync Cf => new CuckooCommandsAsync(_transaction);
        public IGraphCommandsAsync Graph => new GraphCommandsAsync(_transaction);
        public IJsonCommandsAsync Json => new JsonCommandsAsync(_transaction);
        public ISearchCommandsAsync Ft => new SearchCommandsAsync(_transaction);
        public ITdigestCommandsAsync Tdigest => new TdigestCommandsAsync(_transaction);
        public ITimeSeriesCommandsAsync Ts => new TimeSeriesCommandsAsync(_transaction);
        public ITopKCommandsAsync TopK => new TopKCommandsAsync(_transaction);

        public IDatabaseAsync Db => _transaction;

    }
}
