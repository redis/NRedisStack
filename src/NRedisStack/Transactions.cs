using StackExchange.Redis;

namespace NRedisStack;

public class Transaction
{
    private readonly ITransaction _transaction;
    public IDatabaseAsync Db => _transaction;

    public Transaction(IDatabase db)
    {
        db.SetInfoInPipeline();
        _transaction = db.CreateTransaction();
    }

    public ConditionResult AddCondition(Condition condition) => _transaction.AddCondition(condition);

    public bool Execute(CommandFlags flags = CommandFlags.None) => _transaction.Execute(flags);

    public Task<bool> ExecuteAsync(CommandFlags flags = CommandFlags.None) => _transaction.ExecuteAsync(flags);

    public BloomCommandsAsync Bf => new(_transaction);
    public CmsCommandsAsync Cms => new(_transaction);
    public CuckooCommandsAsync Cf => new(_transaction);
    public JsonCommandsAsync Json => new(_transaction);
    public SearchCommandsAsync Ft => new(_transaction);
    public TdigestCommandsAsync Tdigest => new(_transaction);
    public TimeSeriesCommandsAsync Ts => new(_transaction);
    public TopKCommandsAsync TopK => new(_transaction);
}