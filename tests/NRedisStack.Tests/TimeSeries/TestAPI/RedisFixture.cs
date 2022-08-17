﻿using System;
using StackExchange.Redis;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class RedisFixture : IDisposable
    {
        public RedisFixture() => Redis = ConnectionMultiplexer.Connect("localhost");

        public void Dispose()
        {
            Redis.Close();
        }

        public ConnectionMultiplexer Redis { get; private set; }
    }
}
