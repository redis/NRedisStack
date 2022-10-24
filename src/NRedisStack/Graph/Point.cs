using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NRedisStack.Graph
{
    public class Point
    {

        private static readonly double EPSILON = 1e-5;

        private double latitude { get; }
        private double longitude { get; }

        /**
         * @param latitude
         * @param longitude
         */
        public Point(double latitude, double longitude)
        {
            this.latitude = latitude;
            this.longitude = longitude;
        }

        /**
         * @param values {@code [latitude, longitude]}
         */
        public Point(List<double> values)
        {
            if (values == null || values.Count != 2)
            {
                throw new ArgumentOutOfRangeException("Point requires two doubles.");
            }
            this.latitude = values[0];
            this.longitude = values[1];
        }

        // public double getLatitude()
        // {
        //     return latitude;
        // }

        // public double getLongitude()
        // {
        //     return longitude;
        // }


        public override bool Equals(object other)
        {
            if (this == other) return true;
            if (!(other.GetType() == typeof(Point))) return false;
            Point o = (Point)other;
            return Math.Abs(latitude - o.latitude) < EPSILON &&
                    Math.Abs(longitude - o.longitude) < EPSILON;
        }

        // TODO: check if needed
        // public override int GetHashCode()
        //     {
        //         return object.Hash(latitude, longitude);
        //     }


        public override string ToString()
        {
            return "Point{latitude=" + latitude + ", longitude=" + longitude + "}";
        }
    }
}