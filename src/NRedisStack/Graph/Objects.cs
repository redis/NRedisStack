using System.Linq;
using System.Runtime.CompilerServices;
using System.Collections.Generic;

[assembly: InternalsVisibleTo("NRedisStack.Graph.Tests")]

namespace NRedisStack.Graph
{
    internal static class Objects
    {
        public static bool AreEqual(object obj1, object obj2)
        {
            if (obj1 == null && obj2 == null)
            {
                return true;
            }

            if (obj1 == null || obj2 == null)
            {
                return false;
            }

            if (obj1.GetType() != obj2.GetType())
            {
                return false;
            }

            if (obj1 is IEnumerable<object> objArray1 && obj2 is IEnumerable<object> objArray2)
            {
                if (Enumerable.SequenceEqual(objArray1, objArray2))
                {
                    return true;
                }
            }

            switch (obj1)
            {
                case byte o1:
                    return o1 == (byte)obj2;
                case sbyte o1:
                    return o1 == (sbyte)obj2;
                case short o1:
                    return o1 == (short)obj2;
                case ushort o1:
                    return o1 == (ushort)obj2;
                case int o1:
                    return o1 == (int)obj2;
                case uint o1:
                    return o1 == (uint)obj2;
                case long o1:
                    return o1 == (long)obj2;
                case ulong o1:
                    return o1 == (ulong)obj2;
                case float o1:
                    return o1 == (float)obj2;
                case double o1:
                    return o1 == (double)obj2;
                case decimal o1:
                    return o1 == (decimal)obj2;
                case char o1:
                    return o1 == (char)obj2;
                case bool o1:
                    return o1 == (bool)obj2;
                case string o1:
                    return o1 == (string)obj2;
                default:
                    return false;
            }
        }
    }
}