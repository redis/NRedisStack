using StackExchange.Redis;

namespace NRedisStack;

public class Pipeline
{
    public Pipeline(IDatabase db)
    {
        db.SetInfoInPipeline();
        _batch = db.CreateBatch();
    }

    private readonly IBatch _batch;

    public void Execute() => _batch.Execute();


    public BloomCommandsAsync Bf => new(_batch);
    public CmsCommandsAsync Cms => new(_batch);
    public CuckooCommandsAsync Cf => new(_batch);

    public JsonCommandsAsync Json => new(_batch);
    public SearchCommandsAsync Ft => new(_batch);
    public TdigestCommandsAsync Tdigest => new(_batch);
    public TimeSeriesCommandsAsync Ts => new(_batch);
    public TopKCommandsAsync TopK => new(_batch);

    public IDatabaseAsync Db => _batch;
}