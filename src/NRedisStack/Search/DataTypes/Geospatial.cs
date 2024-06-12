
using NRedisStack.Search.DataTypes.Geo;

namespace NRedisStack.Search.DataTypes
{
    namespace Geo
    {
        public abstract class Shape
        {
            public abstract String SerializeToWKT();
        }

        public class Point : Shape
        {
            public double X { get; }
            public double Y { get; }
            public Point(double x, double y)
            {
                X = x;
                Y = y;
            }

            public override string SerializeToWKT()
            {
                return $"POINT ({X}  {Y})";
            }
        }

        public class Polygon : Shape
        {
            protected (double X, double Y)[] points;
            public Polygon(params Point[] points) => this.points = points.Select(p => (p.X, p.Y)).ToArray();
            public Polygon(params (double X, double Y)[] points) => this.points = points;


            public override string SerializeToWKT()
            {
                string pointsStr = String.Join(", ", points.Select(p => $"{p.X} {p.Y}"));
                return $"POLYGON (({pointsStr}))";
            }
        }
    }

    public class Geospatial
    {
        public enum Functions { INTERSECTS, DISJOINT, WITHIN, CONTAINS }

        public class QuerySerializer
        {
            public static QuerySerializer DEFAULT = new QuerySerializer();
            public virtual string Serialize(Geospatial geo, string paramName)
            {
                // @field:[{WITHIN|CONTAINS|DISJOINT|INTERSECT|} {wkt}]
                return $"@{geo.Property}:[{geo.Function} ${paramName}]";
            }
        }

        private string Property { get; }
        private Functions Function { get; }
        private Shape Shape { get; }

        protected QuerySerializer serializer = QuerySerializer.DEFAULT;

        public Geospatial(string property, Functions function, Shape Shape)
        {
            this.Property = property;
            this.Function = function;
            this.Shape = Shape;
        }

        public string SerializeQuery(string paramName)
        {
            return serializer.Serialize(this, paramName);
        }

        public string SerializeShape() => Shape.SerializeToWKT();

        internal void SetSerializer(QuerySerializer customSerializer)
        {
            serializer = customSerializer;
        }
    }

    public class ORQuerySerializer : Geospatial.QuerySerializer
    {
        public static ORQuerySerializer DEFAULT = new ORQuerySerializer();
        public override string Serialize(Geospatial geo, string paramName)
        {
            string query = base.Serialize(geo, paramName);
            return $" | {query}";
        }
    }
}