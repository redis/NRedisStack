using System;
using System.Collections.Generic;
using NRedisStack.Literals;
using NRedisStack.Literals.Enums;
using NRedisStack.DataTypes;
using NRedisStack.Extensions;
using StackExchange.Redis;

namespace NRedisStack
{
    public static class BloomAux
    {
        public static List<object> BuildInsertArgs(RedisKey key, RedisValue[] items, int? capacity,
                                     double? error, int? expansion, bool nocreate, bool nonscaling)
        {
            var args = new List<object> { key };
            args.AddCapacity(capacity);
            args.AddError(error);
            args.AddExpansion(expansion);
            args.AddNoCreate(nocreate);
            args.AddNoScaling(nonscaling);
            args.AddItems(items);

            return args;
        }

        private static void AddItems(this List<object> args, RedisValue[] items)
        {
            args.Add(BloomArgs.ITEMS);
            foreach (var item in items)
            {
                args.Add(item);
            }
        }

        private static void AddNoScaling(this List<object> args, bool nonscaling)
        {
            if (nonscaling)
            {
                args.Add(BloomArgs.NONSCALING);
            }
        }

        private static void AddNoCreate(this List<object> args, bool nocreate)
        {
            if (nocreate)
            {
                args.Add(BloomArgs.NOCREATE);
            }
        }

        private static void AddExpansion(this List<object> args, int? expansion)
        {
            if (expansion != null)
            {
                args.Add(BloomArgs.EXPANSION);
                args.Add(expansion);
            }
        }

        private static void AddError(this List<object> args, double? error)
        {
            if (error != null)
            {
                args.Add(BloomArgs.ERROR);
                args.Add(error);
            }
        }

        private static void AddCapacity(this List<object> args, int? capacity)
        {
            if (capacity != null)
            {
                args.Add(BloomArgs.CAPACITY);
                args.Add(capacity);
            }
        }
    }
}
