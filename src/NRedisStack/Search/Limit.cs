namespace NRedisStack.Search.Aggregation;

internal readonly struct Limit
{
    public static Limit NO_LIMIT = new(0, 0);
    private readonly int _offset, _count;

    public Limit(int offset, int count)
    {
        _offset = offset;
        _count = count;
    }

    //     public void addArgs(List<String> args) {
    //     if (count == 0) {
    //       return;
    //     }
    //     args.add("LIMIT");
    //     args.add(Integer.toString(offset));
    //     args.add(Integer.toString(count));
    //   }

    internal void SerializeRedisArgs(List<object> args)
    {
        if (_count == 0) return;
        args.Add("LIMIT");
        args.Add(_offset);
        args.Add(_count);
    }
}