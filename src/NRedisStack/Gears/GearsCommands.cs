using StackExchange.Redis;
namespace NRedisStack
{

    public class GearsCommands : GearsCommandsAsync, IGearsCommands
    {
        IDatabase _db;
        public GearsCommands(IDatabase db) : base(db)
        {
            _db = db;
        }

        /// <inheritdoc/>
        public bool TFunctionLoad(string libraryCode, string? config = null, bool replace = false)
        {
            return _db.Execute(GearsCommandBuilder.TFunctionLoad(libraryCode, config, replace)).OKtoBoolean();
        }
    }
}
