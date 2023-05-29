using StackExchange.Redis;
namespace NRedisStack
{

    public class GearsCommandsAsync : IGearsCommandsAsync
    {
        IDatabaseAsync _db;
        public GearsCommandsAsync(IDatabaseAsync db)
        {
            _db = db;
        }

        /// <inheritdoc/>
        public async Task<bool> TFunctionLoadAsync(string libraryCode, string? config = null, bool replace = false)
        {
            return (await _db.ExecuteAsync(GearsCommandBuilder.TFunctionLoad(libraryCode, config, replace))).OKtoBoolean();
        }
    }
}
