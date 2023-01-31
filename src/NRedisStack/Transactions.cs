using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NRedisStack
{
    public class Transactions
    {
        public Transactions(IConnectionMultiplexer muxer)
        {
            _transaction = muxer.GetDatabase().CreateTransaction();
        }

        public Transactions(IDatabase db)
        {
            _transaction = db.CreateTransaction();
        }

        private ITransaction _transaction;

        public void AddCondition(Condition condition)
        {
            _transaction.AddCondition(condition);
        }

        public bool Execute()
        {
            var result = _transaction.Execute();
            return result;
        }

        public bool ExecuteAsync()
        {
           var result = _transaction.ExecuteAsync();
            return result.Result;
        }


        public IBloomCommandsAsync Bf => new BloomCommandsAsync(_transaction);
        public ICmsCommandsAsync Cms => new CmsCommandsAsync(_transaction);
        public ICuckooCommandsAsync Cf => new CuckooCommandsAsync(_transaction);
        public IGraphCommandsAsync Graph => new GraphCommandsAsync(_transaction);
        public IJsonCommandsAsync Json => new JsonCommandsAsync(_transaction);
        public ISearchCommandsAsync Ft => new SearchCommandsAsync(_transaction);
        public ITdigestCommandsAsync Tdigest => new TdigestCommandsAsync(_transaction);
        public ITimeSeriesCommandsAsync Ts => new TimeSeriesCommandsAsync(_transaction);
        public ITopKCommandsAsync TopK => new TopKCommandsAsync(_transaction);

        public IDatabaseAsync Db => _transaction;

    }
}
