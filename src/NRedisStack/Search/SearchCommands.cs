using NRedisStack.Literals;
using StackExchange.Redis;
namespace NRedisStack
{
    public class SearchCommands
    {
        IDatabase _db;
        public SearchCommands(IDatabase db)
        {
            _db = db;
        }

        /// <summary>
        /// Returns a list of all existing indexes.
        /// </summary>
        /// <returns>Array with index names.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft._list"/></remarks>
        public RedisResult[] _List()
        {
            return _db.Execute(FT._LIST).ToArray();
        }

        // TODO: Aggregate

        /// <summary>
        /// Add an alias to an index.
        /// </summary>
        /// <param name="alias">Alias to be added to an index.</param>
        /// <param name="index">The index name.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.aliasadd"/></remarks>
        public bool AliasAdd(string alias, string index)
        {
            return _db.Execute(FT.ALIASADD, alias, index).OKtoBoolean();
        }

        /// <summary>
        /// Add an alias to an index.
        /// </summary>
        /// <param name="alias">Alias to be added to an index.</param>
        /// <param name="index">The index name.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.aliasadd"/></remarks>
        public async Task<bool> AliasAddAsync(string alias, string index)
        {
            return (await _db.ExecuteAsync(FT.ALIASADD, alias, index)).OKtoBoolean();
        }

        /// <summary>
        /// Remove an alias to an index.
        /// </summary>
        /// <param name="alias">Alias to be removed.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.aliasdel"/></remarks>
        public bool AliasDel(string alias)
        {
            return _db.Execute(FT.ALIASDEL, alias).OKtoBoolean();
        }

        /// <summary>
        /// Remove an alias to an index.
        /// </summary>
        /// <param name="alias">Alias to be removed.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.aliasdel"/></remarks>
        public async Task<bool> AliasDelAsync(string alias)
        {
            return (await _db.ExecuteAsync(FT.ALIASDEL, alias)).OKtoBoolean();
        }

        /// <summary>
        /// Add an alias to an index. If the alias is already associated with another index,
        /// FT.ALIASUPDATE removes the alias association with the previous index.
        /// </summary>
        /// <param name="alias">Alias to be removed.</param>
        /// <param name="index">The index name.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.aliasdel"/></remarks>
        public bool AliasUpdate(string alias, string index)
        {
            return _db.Execute(FT.ALIASUPDATE, alias, index).OKtoBoolean();
        }

        /// <summary>
        /// Add an alias to an index. If the alias is already associated with another index,
        /// FT.ALIASUPDATE removes the alias association with the previous index.
        /// </summary>
        /// <param name="alias">Alias to be removed.</param>
        /// <param name="index">The index name.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.aliasdel"/></remarks>
        public async Task<bool> AliasUpdateAsync(string alias, string index)
        {
            return (await _db.ExecuteAsync(FT.ALIASUPDATE, alias, index)).OKtoBoolean();
        }

        // TODO: finish this:
        // /// <summary>
        // /// Add a new attribute to the index
        // /// </summary>
        // /// <param name="alias">Alias to be removed.</param>
        // /// <param name="index">The index name.</param>
        // /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        // /// <remarks><seealso href="https://redis.io/commands/ft.aliasdel"/></remarks>
        // public bool Alter(string alias, string index)
        // {
        //     return _db.Execute(FT.ALIASUPDATE, alias, index).OKtoBoolean();
        // }

        // public RedisResult Info(RedisValue index)
        // {
        //     return _db.Execute(FT.INFO, index);
        // }
    }
}