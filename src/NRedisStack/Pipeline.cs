using StackExchange.Redis;

namespace NRedisStack;

public class Pipeline
{
    public Pipeline(IConnectionMultiplexer muxer)
    {
        _batch = muxer.GetDatabase().CreateBatch();
    }

    private IBatch _batch;

    public void Execute()
    {
        _batch.Execute();
    }

    public IJsonCommandsAsync Json => new JsonCommandsAsync(_batch);

    public IDatabaseAsync Db => _batch;
}