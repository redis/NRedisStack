using StackExchange.Redis;

namespace NRedisStack
{
    public class Transaction
    {
        private ITransaction _transaction;
        public IDatabaseAsync Db => _transaction;

        public Transaction(IDatabase db)
        {
            db.SetInfoInPipeline();
            _transaction = db.CreateTransaction();
        }

        public ConditionResult AddCondition(Condition condition) => _transaction.AddCondition(condition);

        public bool Execute(CommandFlags flags = CommandFlags.None) => _transaction.Execute(flags);

        public Task<bool> ExecuteAsync(CommandFlags flags = CommandFlags.None) => _transaction.ExecuteAsync(flags);

        public BloomCommandsAsync Bf => new BloomCommandsAsync(_transaction);
        public CmsCommandsAsync Cms => new CmsCommandsAsync(_transaction);
        public CuckooCommandsAsync Cf => new CuckooCommandsAsync(_transaction);
        public JsonCommandsAsync Json => new JsonCommandsAsync(_transaction);
        public SearchCommandsAsync Ft => new SearchCommandsAsync(_transaction);
        public TdigestCommandsAsync Tdigest => new TdigestCommandsAsync(_transaction);
        public TimeSeriesCommandsAsync Ts => new TimeSeriesCommandsAsync(_transaction);
        public TopKCommandsAsync TopK => new TopKCommandsAsync(_transaction);
    }
}
