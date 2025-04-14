using StackExchange.Redis;

namespace NRedisStack;

public class Pipeline
{
    public Pipeline(IDatabase db)
    {
        db.SetInfoInPipeline();
        _batch = db.CreateBatch();
    }

    private IBatch _batch;

    public void Execute() => _batch.Execute();


    public BloomCommandsAsync Bf => new BloomCommandsAsync(_batch);
    public CmsCommandsAsync Cms => new CmsCommandsAsync(_batch);
    public CuckooCommandsAsync Cf => new CuckooCommandsAsync(_batch);

    [Obsolete]
    public GraphCommandsAsync Graph => new GraphCommandsAsync(_batch);
    public JsonCommandsAsync Json => new JsonCommandsAsync(_batch);
    public SearchCommandsAsync Ft => new SearchCommandsAsync(_batch);
    public TdigestCommandsAsync Tdigest => new TdigestCommandsAsync(_batch);
    public TimeSeriesCommandsAsync Ts => new TimeSeriesCommandsAsync(_batch);
    public TopKCommandsAsync TopK => new TopKCommandsAsync(_batch);

    public IDatabaseAsync Db => _batch;
}