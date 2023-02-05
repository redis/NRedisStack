using StackExchange.Redis;

namespace NRedisStack;

public class Pipeline
{
    public Pipeline(IDatabase db)
    {
        _batch = db.CreateBatch();
    }

    private IBatch _batch;

    public void Execute()
    {
        _batch.Execute();
    }

    public IBloomCommandsAsync Bf => new BloomCommandsAsync(_batch);
    public ICmsCommandsAsync Cms => new CmsCommandsAsync(_batch);
    public ICuckooCommandsAsync Cf => new CuckooCommandsAsync(_batch);
    public IGraphCommandsAsync Graph => new GraphCommandsAsync(_batch);
    public IJsonCommandsAsync Json => new JsonCommandsAsync(_batch);
    public ISearchCommandsAsync Ft => new SearchCommandsAsync(_batch);
    public ITdigestCommandsAsync Tdigest => new TdigestCommandsAsync(_batch);
    public ITimeSeriesCommandsAsync Ts => new TimeSeriesCommandsAsync(_batch);
    public ITopKCommandsAsync TopK => new TopKCommandsAsync(_batch);

    public IDatabaseAsync Db => _batch;
}