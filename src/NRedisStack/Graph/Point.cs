namespace NRedisStack.Graph
{
    public class Point
    {
        private static readonly double EPSILON = 1e-5;

        private double latitude { get; }
        private double longitude { get; }

        public Point(double latitude, double longitude)
        {
            this.latitude = latitude;
            this.longitude = longitude;
        }

        public Point(List<double> values)
        {
            if (values == null || values.Count != 2)
            {
                throw new ArgumentOutOfRangeException("Point requires two doubles.");
            }
            this.latitude = values[0];
            this.longitude = values[1];
        }

        // TODO: check if this is needed:
        public override bool Equals(object? obj)
        {
            if (obj == null) return this == null;

            if (this == obj) return true;
            if (!(obj.GetType() == typeof(Point))) return false;
            Point o = (Point)obj;
            return Math.Abs(latitude - o.latitude) < EPSILON &&
                    Math.Abs(longitude - o.longitude) < EPSILON;
        }

        public override int GetHashCode()
        {
            return latitude.GetHashCode() ^ longitude.GetHashCode();
        }


        public override string ToString()
        {
            return "Point{latitude=" + latitude + ", longitude=" + longitude + "}";
        }
    }
}