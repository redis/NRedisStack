using Xunit;
using StackExchange.Redis;
using NRedisStack.RedisStackCommands;
using Moq;
using NRedisStack.Graph;
using NRedisStack.Graph.DataTypes;

namespace NRedisStack.Tests.Graph;

public class GraphTests : AbstractNRedisStackTest, IDisposable
{
    Mock<IDatabase> _mock = new Mock<IDatabase>();
    private readonly string key = "GRAPH_TESTS";
    public GraphTests(RedisFixture redisFixture) : base(redisFixture) { }

    public void Dispose()
    {
        redisFixture.Redis.GetDatabase().KeyDelete(key);
    }

    #region SyncTests

    [Fact]
    public void TestReserveBasic()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
    }

    [Fact]
    public void TestCreateNode()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        // Create a node
        ResultSet resultSet = graph.Query("social", "CREATE ({name:'roi',age:32})");

        Statistics stats = resultSet.Statistics;
        Assert.Equal(1, stats.NodesCreated);
        Assert.Equal(0, stats.NodesDeleted);
        Assert.Equal(0, stats.RelationshipsCreated);
        Assert.Equal(0, stats.RelationshipsDeleted);
        Assert.Equal(2, stats.PropertiesSet);
        Assert.NotNull(stats.QueryInternalExecutionTime);

        Assert.Equal(0, resultSet.Count);

        // delete
        graph.Delete("social");
    }

    [Fact]
    public void TestCreateLabeledNode()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        // Create a node with a label
        ResultSet resultSet = graph.Query("social", "CREATE (:human{name:'danny',age:12})");

        Statistics stats = resultSet.Statistics;
        //        Assert.Equal("1", stats.getstringValue(Label.NODES_CREATED));
        Assert.Equal(1, stats.NodesCreated);
        //        Assert.Equal("2", stats.getstringValue(Label.PROPERTIES_SET));
        Assert.Equal(2, stats.PropertiesSet);
        //        Assert.NotNull(stats.getstringValue(Label.QUERY_INTERNAL_EXECUTION_TIME));
        Assert.NotNull(stats.QueryInternalExecutionTime);

        Assert.Equal(0, resultSet.Count);
        // Assert.False(resultSet..iterator().MoveNext());
    }

    [Fact]
    public void TestConnectNodes()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        // Create both source and destination nodes
        Assert.NotNull(graph.Query("social", "CREATE (:person{name:'roi',age:32})"));
        Assert.NotNull(graph.Query("social", "CREATE (:person{name:'amit',age:30})"));

        // Connect source and destination nodes.
        ResultSet resultSet = graph.Query("social",
                "MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[:knows]->(b)");

        Statistics stats = resultSet.Statistics;
        //        Assert.Null(stats.getstringValue(Label.NODES_CREATED));
        Assert.Equal(0, stats.NodesCreated);
        Assert.Equal(1, stats.RelationshipsCreated);
        Assert.Equal(0, stats.RelationshipsDeleted);
        //        Assert.Null(stats.getstringValue(Label.PROPERTIES_SET));
        Assert.Equal(0, stats.PropertiesSet);
        //        Assert.NotNull(stats.getstringValue(Label.QUERY_INTERNAL_EXECUTION_TIME));
        Assert.NotNull(stats.QueryInternalExecutionTime);

        Assert.Equal(0, resultSet.Count);
    }

    [Fact]
    public void TestDeleteNodes()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        Assert.NotNull(graph.Query("social", "CREATE (:person{name:'roi',age:32})"));
        Assert.NotNull(graph.Query("social", "CREATE (:person{name:'amit',age:30})"));

        ResultSet deleteResult = graph.Query("social", "MATCH (a:person) WHERE (a.name = 'roi') DELETE a");

        Statistics delStats = deleteResult.Statistics;
        //        Assert.Null(delStats.getstringValue(Label.NODES_CREATED));
        Assert.Equal(0, delStats.NodesCreated);
        Assert.Equal(1, delStats.NodesDeleted);
        //        Assert.Null(delStats.getstringValue(Label.RELATIONSHIPS_CREATED));
        Assert.Equal(0, delStats.RelationshipsCreated);
        //        Assert.Null(delStats.getstringValue(Label.RELATIONSHIPS_DELETED));
        Assert.Equal(0, delStats.RelationshipsDeleted);
        //        Assert.Null(delStats.getstringValue(Label.PROPERTIES_SET));
        Assert.Equal(0, delStats.PropertiesSet);
        //        Assert.NotNull(delStats.getstringValue(Label.QUERY_INTERNAL_EXECUTION_TIME));
        Assert.NotNull(delStats.QueryInternalExecutionTime);
        Assert.Equal(0, deleteResult.Count);
        // Assert.False(deleteResult.iterator().MoveNext());

        Assert.NotNull(graph.Query("social", "CREATE (:person{name:'roi',age:32})"));
        Assert.NotNull(graph.Query("social",
                "MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[:knows]->(a)"));

        deleteResult = graph.Query("social", "MATCH (a:person) WHERE (a.name = 'roi') DELETE a");

        //        Assert.Null(delStats.getstringValue(Label.NODES_CREATED));
        Assert.Equal(0, delStats.NodesCreated);
        Assert.Equal(1, delStats.NodesDeleted);
        //        Assert.Null(delStats.getstringValue(Label.RELATIONSHIPS_CREATED));
        Assert.Equal(0, delStats.RelationshipsCreated);
        // Assert.Equal(1, delStats.RelationshipsDeleted);
        Assert.Equal(0, delStats.RelationshipsDeleted);
        //        Assert.Null(delStats.getstringValue(Label.PROPERTIES_SET));
        Assert.Equal(0, delStats.PropertiesSet);
        //        Assert.NotNull(delStats.getstringValue(Label.QUERY_INTERNAL_EXECUTION_TIME));
        Assert.NotNull(delStats.QueryInternalExecutionTime);
        Assert.Equal(0, deleteResult.Count);
        // Assert.False(deleteResult.iterator().MoveNext());
    }

    [Fact]
    public void TestDeleteRelationship()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        Assert.NotNull(graph.Query("social", "CREATE (:person{name:'roi',age:32})"));
        Assert.NotNull(graph.Query("social", "CREATE (:person{name:'amit',age:30})"));
        Assert.NotNull(graph.Query("social",
                "MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[:knows]->(a)"));

        ResultSet deleteResult = graph.Query("social",
                "MATCH (a:person)-[e]->() WHERE (a.name = 'roi') DELETE e");

        Statistics delStats = deleteResult.Statistics;
        //        Assert.Null(delStats.getstringValue(Label.NODES_CREATED));
        Assert.Equal(0, delStats.NodesCreated);
        //        Assert.Null(delStats.getstringValue(Label.NODES_DELETED));
        Assert.Equal(0, delStats.NodesDeleted);
        //        Assert.Null(delStats.getstringValue(Label.RELATIONSHIPS_CREATED));
        Assert.Equal(0, delStats.RelationshipsCreated);
        Assert.Equal(1, delStats.RelationshipsDeleted);
        //        Assert.Null(delStats.getstringValue(Label.PROPERTIES_SET));
        Assert.Equal(0, delStats.PropertiesSet);
        //        Assert.NotNull(delStats.getstringValue(Label.QUERY_INTERNAL_EXECUTION_TIME));
        Assert.NotNull(delStats.QueryInternalExecutionTime);
        Assert.Equal(0, deleteResult.Count);
        // Assert.False(deleteResult.iterator().MoveNext());
    }

    [Fact]
    public void TestIndex()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        // Create both source and destination nodes
        Assert.NotNull(graph.Query("social", "CREATE (:person{name:'roi',age:32})"));

        ResultSet createIndexResult = graph.Query("social", "CREATE INDEX ON :person(age)");
        Assert.Empty(createIndexResult);
        Assert.Equal(1, createIndexResult.Statistics.IndicesCreated);

        // since RediSearch as index, those action are allowed
        ResultSet createNonExistingIndexResult = graph.Query("social", "CREATE INDEX ON :person(age1)");
        Assert.Empty(createNonExistingIndexResult);
        Assert.Equal(1, createNonExistingIndexResult.Statistics.IndicesCreated);

        ResultSet createExistingIndexResult = graph.Query("social", "CREATE INDEX ON :person(age)");
        Assert.Empty(createExistingIndexResult);
        Assert.Equal(0, createExistingIndexResult.Statistics.IndicesCreated);

        ResultSet deleteExistingIndexResult = graph.Query("social", "DROP INDEX ON :person(age)");
        Assert.Empty(deleteExistingIndexResult);
        Assert.Equal(1, deleteExistingIndexResult.Statistics.IndicesDeleted);
    }

    [Fact]
    public void TestHeader()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();

        Assert.NotNull(graph.Query("social", "CREATE (:person{name:'roi',age:32})"));
        Assert.NotNull(graph.Query("social", "CREATE (:person{name:'amit',age:30})"));
        Assert.NotNull(graph.Query("social",
                "MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[:knows]->(a)"));

        ResultSet queryResult = graph.Query("social", "MATCH (a:person)-[r:knows]->(b:person) RETURN a,r, a.age");

        Header header = queryResult.Header;
        Assert.NotNull(header);
        Assert.Equal("Header{"
                //                + "schemaTypes=[COLUMN_SCALAR, COLUMN_SCALAR, COLUMN_SCALAR], "
                + "schemaTypes=[SCALAR, SCALAR, SCALAR], "
                + "schemaNames=[a, r, a.age]}", header.ToString());
        // Assert.Assert.Equal(-1901778507, header.hashCode());

        List<string> schemaNames = header.SchemaNames;

        Assert.NotNull(schemaNames);
        Assert.Equal(3, schemaNames.Count);
        Assert.Equal("a", schemaNames[0]);
        Assert.Equal("r", schemaNames[1]);
        Assert.Equal("a.age", schemaNames[2]);
    }

    [Fact]
    public void TestRecord()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        string name = "roi";
        int age = 32;
        double doubleValue = 3.14;
        bool boolValue = true;

        string place = "TLV";
        int since = 2000;

        var nameProperty = new KeyValuePair<string, object>("name", name);
        var ageProperty = new KeyValuePair<string, object>("age", age);
        var doubleProperty = new KeyValuePair<string, object>("doubleValue", doubleValue);
        var trueboolProperty = new KeyValuePair<string, object>("boolValue", true);
        var falseboolProperty = new KeyValuePair<string, object>("boolValue", false);
        var placeProperty = new KeyValuePair<string, object>("place", place);
        var sinceProperty = new KeyValuePair<string, object>("since", since);

        Node expectedNode = new Node();
        expectedNode.Id = 0;
        expectedNode.Labels.Add("person");
        expectedNode.PropertyMap.Add(nameProperty);
        expectedNode.PropertyMap.Add(ageProperty);
        expectedNode.PropertyMap.Add(doubleProperty);
        expectedNode.PropertyMap.Add(trueboolProperty);
        Assert.Equal(
                "Node{labels=[person], id=0, "
                        + "propertyMap={name=roi, age=32, doubleValue=3.14, boolValue=True}}",
                expectedNode.ToString());
        Edge expectedEdge = new Edge();
        expectedEdge.Id = 0;
        expectedEdge.Source = 0;
        expectedEdge.Destination = 1;
        expectedEdge.RelationshipType = "knows";
        expectedEdge.PropertyMap.Add(placeProperty);
        expectedEdge.PropertyMap.Add(sinceProperty);
        expectedEdge.PropertyMap.Add(doubleProperty);
        expectedEdge.PropertyMap.Add(falseboolProperty);
        Assert.Equal("Edge{relationshipType='knows', source=0, destination=1, id=0, "
                + "propertyMap={place=TLV, since=2000, doubleValue=3.14, boolValue=False}}", expectedEdge.ToString());

        Dictionary<string, object> parameters = new Dictionary<string, object>();
        parameters.Add("name", name);
        parameters.Add("age", age);
        parameters.Add("boolValue", boolValue);
        parameters.Add("doubleValue", doubleValue);

        Assert.NotNull(graph.Query("social",
                "CREATE (:person{name:$name,age:$age, doubleValue:$doubleValue, boolValue:$boolValue})", parameters));
        Assert.NotNull(graph.Query("social", "CREATE (:person{name:'amit',age:30})"));
        Assert.NotNull(
                graph.Query("social", "MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  " +
                        "CREATE (a)-[:knows{place:'TLV', since:2000,doubleValue:3.14, boolValue:false}]->(b)"));

        ResultSet resultSet = graph.Query("social", "MATCH (a:person)-[r:knows]->(b:person) RETURN a,r, " +
                "a.name, a.age, a.doubleValue, a.boolValue, " +
                "r.place, r.since, r.doubleValue, r.boolValue");
        Assert.NotNull(resultSet);

        Statistics stats = resultSet.Statistics;
        Assert.Equal(0, stats.NodesCreated);
        Assert.Equal(0, stats.NodesDeleted);
        Assert.Equal(0, stats.LabelsAdded);
        Assert.Equal(0, stats.PropertiesSet);
        Assert.Equal(0, stats.RelationshipsCreated);
        Assert.Equal(0, stats.RelationshipsDeleted);
        Assert.NotNull(stats.QueryInternalExecutionTime);
        Assert.NotEmpty(stats.QueryInternalExecutionTime);

        Assert.Equal(1, resultSet.Count);
        // IReadOnlyCollection<Record> iterator = resultSet.GetEnumerator();
        var iterator = resultSet.GetEnumerator();
        Assert.True(iterator.MoveNext());
        var record = iterator.Current;
        Assert.False(iterator.MoveNext());

        Node node = record.GetValue<Node>(0);
        Assert.NotNull(node);

        Assert.Equal(expectedNode.ToString(), node.ToString());
        //Expected: "Node{labels=[person], id=0, propertyMap={name=Property{name='name', value=roi}, age=Property{name='age', value=32}, doubleValue=Property{name='doubleValue', value=3.14}, boolValue=Property{name='boolValue', value=True}}}"
        //Actual   :"Node{labels=[person], id=0, propertyMap={name=Property{name='name', value=roi}, age=Property{name='age', value=32}, doubleValue=Property{name='doubleValue', value=3.14}, boolValue=Property{name='boolValue', value=True}}}"

        node = record.GetValue<Node>("a");
        Assert.Equal(expectedNode.ToString(), node.ToString());

        Edge edge = record.GetValue<Edge>(1);
        Assert.NotNull(edge);
        Assert.Equal(expectedEdge.ToString(), edge.ToString());

        edge = record.GetValue<Edge>("r");
        Assert.Equal(expectedEdge.ToString(), edge.ToString());

        Assert.Equal(new List<string>(){"a", "r", "a.name", "a.age", "a.doubleValue", "a.boolValue",
                "r.place", "r.since", "r.doubleValue", "r.boolValue"}, record.Header);

        List<object> expectedList = new List<object>() {expectedNode, expectedEdge,
                name, (long)age, doubleValue, true,
                place, (long)since, doubleValue, false};


        for (int i = 0; i < expectedList.Count; i++)
        {
            Assert.Equal(expectedList[i].ToString(), record.Values[i].ToString());
        }

        Node a = record.GetValue<Node>("a");
        foreach (string propertyName in expectedNode.PropertyMap.Keys)
        {
            Assert.Equal(expectedNode.PropertyMap[propertyName].ToString(), a.PropertyMap[propertyName].ToString());
        }

        Assert.Equal("roi", record.GetString(2));
        Assert.Equal("32", record.GetString(3));
        Assert.Equal(32L, (record.GetValue<long>(3)));
        Assert.Equal(32L, (record.GetValue<long>("a.age")));
        Assert.Equal("roi", record.GetString("a.name"));
        Assert.Equal("32", record.GetString("a.age"));

    }

    [Fact]
    public void TestAdditionToProcedures()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();

        Assert.NotNull(graph.Query("social", "CREATE (:person{name:'roi',age:32})"));
        Assert.NotNull(graph.Query("social", "CREATE (:person{name:'amit',age:30})"));
        Assert.NotNull(graph.Query("social",
                "MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[:knows]->(b)"));

        // expected objects init
        var nameProperty = new KeyValuePair<string, object>("name", "roi");
        var ageProperty = new KeyValuePair<string, object>("age", 32);
        var lastNameProperty = new KeyValuePair<string, object>("lastName", "a");

        Node expectedNode = new Node();
        expectedNode.Id = 0;
        expectedNode.Labels.Add("person");
        expectedNode.PropertyMap.Add(nameProperty);
        expectedNode.PropertyMap.Add(ageProperty);

        Edge expectedEdge = new Edge();
        expectedEdge.Id = 0;
        expectedEdge.Source = 0;
        expectedEdge.Destination = 1;
        expectedEdge.RelationshipType = "knows";

        ResultSet resultSet = graph.Query("social", "MATCH (a:person)-[r:knows]->(b:person) RETURN a,r");
        Assert.NotNull(resultSet.Header);
        Header header = resultSet.Header;
        List<string> schemaNames = header.SchemaNames;
        Assert.NotNull(schemaNames);
        Assert.Equal(2, schemaNames.Count);
        Assert.Equal("a", schemaNames[0]);
        Assert.Equal("r", schemaNames[1]);
        Assert.Equal(1, resultSet.Count);
        var iterator = resultSet.GetEnumerator();
        Assert.True(iterator.MoveNext());
        var record = iterator.Current;
        Assert.False(iterator.MoveNext());
        Assert.Equal(new List<string>() { "a", "r" }, record.Header);
        Assert.Equal(expectedNode.ToString(), record.Values[0].ToString());
        Assert.Equal(expectedEdge.ToString(), record.Values[1].ToString());

        // test for local cache updates

        expectedNode.PropertyMap.Remove("name");
        expectedNode.PropertyMap.Remove("age");
        expectedNode.PropertyMap.Add(lastNameProperty);
        expectedNode.Labels.Remove("person");
        expectedNode.Labels.Add("worker");
        expectedNode.Id = 2;
        expectedEdge.RelationshipType = "worksWith";
        expectedEdge.Source = 2;
        expectedEdge.Destination = 3;
        expectedEdge.Id = 1;
        Assert.NotNull(graph.Query("social", "CREATE (:worker{lastName:'a'})"));
        Assert.NotNull(graph.Query("social", "CREATE (:worker{lastName:'b'})"));
        Assert.NotNull(graph.Query("social",
                "MATCH (a:worker), (b:worker) WHERE (a.lastName = 'a' AND b.lastName='b')  CREATE (a)-[:worksWith]->(b)"));
        resultSet = graph.Query("social", "MATCH (a:worker)-[r:worksWith]->(b:worker) RETURN a,r");
        Assert.NotNull(resultSet.Header);
        header = resultSet.Header;
        schemaNames = header.SchemaNames;
        Assert.NotNull(schemaNames);
        Assert.Equal(2, schemaNames.Count);
        Assert.Equal("a", schemaNames[0]);
        Assert.Equal("r", schemaNames[1]);
        Assert.Equal(1, resultSet.Count);
        iterator = resultSet.GetEnumerator();
        Assert.True(iterator.MoveNext());
        record = iterator.Current;
        Assert.False(iterator.MoveNext());
        Assert.Equal(new List<string> { "a", "r" }, record.Header);
        Assert.Equal(expectedNode.ToString(), record.Values[0].ToString());
        Assert.Equal(expectedEdge.ToString(), record.Values[1].ToString());
    }

    [Fact]
    public void TestEscapedQuery()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        Dictionary<string, object> params1 = new Dictionary<string, object>();
        params1.Add("s1", "S\"'");
        params1.Add("s2", "S'\"");
        Assert.NotNull(graph.Query("social", "CREATE (:escaped{s1:$s1,s2:$s2})", params1));

        Dictionary<string, object> params2 = new Dictionary<string, object>();
        params2.Add("s1", "S\"'");
        params2.Add("s2", "S'\"");
        Assert.NotNull(graph.Query("social", "MATCH (n) where n.s1=$s1 and n.s2=$s2 RETURN n", params2));

        Assert.NotNull(graph.Query("social", "MATCH (n) where n.s1='S\"' RETURN n"));
    }

    [Fact]
    public void TestArraySupport()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();

        Node expectedANode = new Node();
        expectedANode.Id = 0;
        expectedANode.Labels.Add("person");
        var aNameProperty = new KeyValuePair<string, object>("name", "a");
        var aAgeProperty = new KeyValuePair<string, object>("age", 32L);
        var aListProperty = new KeyValuePair<string, object>("array", new object[] { 0L, 1L, 2L });
        expectedANode.PropertyMap.Add(aNameProperty);
        expectedANode.PropertyMap.Add(aAgeProperty);
        expectedANode.PropertyMap.Add(aListProperty);

        Node expectedBNode = new Node();
        expectedBNode.Id = 1;
        expectedBNode.Labels.Add("person");
        var bNameProperty = new KeyValuePair<string, object>("name", "b");
        var bAgeProperty = new KeyValuePair<string, object>("age", 30L);
        var bListProperty = new KeyValuePair<string, object>("array", new object[] { 3L, 4L, 5L });
        expectedBNode.PropertyMap.Add(bNameProperty);
        expectedBNode.PropertyMap.Add(bAgeProperty);
        expectedBNode.PropertyMap.Add(bListProperty);

        Assert.NotNull(graph.Query("social", "CREATE (:person{name:'a',age:32,array:[0,1,2]})"));
        Assert.NotNull(graph.Query("social", "CREATE (:person{name:'b',age:30,array:[3,4,5]})"));

        // test array

        ResultSet resultSet = graph.Query("social", "WITH [0,1,2] as x return x");

        // check header
        Assert.NotNull(resultSet.Header);
        Header header = resultSet.Header;

        List<string> schemaNames = header.SchemaNames;
        Assert.NotNull(schemaNames);
        Assert.Equal(1, schemaNames.Count);
        Assert.Equal("x", schemaNames[0]);

        // check record
        Assert.Equal(1, resultSet.Count);
        var iterator = resultSet.GetEnumerator();
        Assert.True(iterator.MoveNext());
        NRedisStack.Graph.Record record = iterator.Current;
        Assert.False(iterator.MoveNext());
        Assert.Equal(new List<string>() { "x" }, record.Header);

        var x = record.GetValue<object[]>("x");
        Assert.Equal(new object[] { 0L, 1L, 2L }, x);

        // test collect
        resultSet = graph.Query("social", "MATCH(n) return collect(n) as x");

        Assert.NotNull(resultSet.Header);
        header = resultSet.Header;

        schemaNames = header.SchemaNames;
        Assert.NotNull(schemaNames);
        Assert.Equal(1, schemaNames.Count);
        Assert.Equal("x", schemaNames[0]);

        // check record
        Assert.Equal(1, resultSet.Count);
        iterator = resultSet.GetEnumerator();
        Assert.True(iterator.MoveNext());
        record = iterator.Current;
        Assert.False(iterator.MoveNext());
        Assert.Equal(new List<string>() { "x" }, record.Header);
        var x2 = record.GetValue<object[]>("x");

        Assert.Equal(expectedANode.ToString(), x2[0].ToString());
        Assert.Equal(expectedBNode.ToString(), x2[1].ToString());

        // test unwind
        resultSet = graph.Query("social", "unwind([0,1,2]) as x return x");

        Assert.NotNull(resultSet.Header);
        header = resultSet.Header;

        schemaNames = header.SchemaNames;
        Assert.NotNull(schemaNames);
        Assert.Equal(1, schemaNames.Count);
        Assert.Equal("x", schemaNames[0]);

        // check record
        Assert.Equal(3, resultSet.Count);
        iterator = resultSet.GetEnumerator();
        for (long i = 0; i < 3; i++)
        {
            Assert.True(iterator.MoveNext());
            record = iterator.Current;
            Assert.Equal(new List<string>() { "x" }, record.Header);
            Assert.Equal(i, (long)record.GetValue<long>("x"));
        }
    }

    [Fact]
    public void TestPath()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        List<Node> nodes = new List<Node>(3);
        for (int i = 0; i < 3; i++)
        {
            Node node = new Node();
            node.Id = i;
            node.Labels.Add("L1");
            nodes.Add(node);
        }

        List<Edge> edges = new List<Edge>(2);
        for (int i = 0; i < 2; i++)
        {
            Edge edge = new Edge();
            edge.Id = i;
            edge.RelationshipType = "R1";
            edge.Source = i;
            edge.Destination = i + 1;
            edges.Add(edge);
        }

        var expectedPaths = new HashSet<NRedisStack.Graph.DataTypes.Path>();

        NRedisStack.Graph.DataTypes.Path path01 = new PathBuilder(2).Append(nodes[0]).Append(edges[0]).Append(nodes[1]).Build();
        NRedisStack.Graph.DataTypes.Path path12 = new PathBuilder(2).Append(nodes[1]).Append(edges[1]).Append(nodes[2]).Build();
        NRedisStack.Graph.DataTypes.Path path02 = new PathBuilder(3).Append(nodes[0]).Append(edges[0]).Append(nodes[1])
                .Append(edges[1]).Append(nodes[2]).Build();

        expectedPaths.Add(path01);
        expectedPaths.Add(path12);
        expectedPaths.Add(path02);

        graph.Query("social", "CREATE (:L1)-[:R1]->(:L1)-[:R1]->(:L1)");

        ResultSet resultSet = graph.Query("social", "MATCH p = (:L1)-[:R1*]->(:L1) RETURN p");

        Assert.Equal(expectedPaths.Count, resultSet.Count);
        var iterator = resultSet.GetEnumerator();

        for (int i = 0; i < resultSet.Count; i++)
        {
            NRedisStack.Graph.DataTypes.Path p = resultSet.ElementAt(i).GetValue<NRedisStack.Graph.DataTypes.Path>("p");
            Assert.Contains(p, expectedPaths);
            expectedPaths.Remove(p);
        }
    }

    [Fact]
    public void TestNullGraphEntities()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        // Create two nodes connected by a single outgoing edge.
        Assert.NotNull(graph.Query("social", "CREATE (:L)-[:E]->(:L2)"));
        // Test a query that produces 1 record with 3 null values.
        ResultSet resultSet = graph.Query("social", "OPTIONAL MATCH (a:NONEXISTENT)-[e]->(b) RETURN a, e, b");
        Assert.Equal(1, resultSet.Count);
        IEnumerator<NRedisStack.Graph.Record> iterator = resultSet.GetEnumerator();
        Assert.True(iterator.MoveNext());
        NRedisStack.Graph.Record record = iterator.Current;
        Assert.False(iterator.MoveNext());
        Assert.Equal(new List<object>() { null, null, null }, record.Values);

        // Test a query that produces 2 records, with 2 null values in the second.
        resultSet = graph.Query("social", "MATCH (a) OPTIONAL MATCH (a)-[e]->(b) RETURN a, e, b ORDER BY ID(a)");
        Assert.Equal(2, resultSet.Count);

        // iterator = resultSet.GetEnumerator();
        // record = iterator.Current;
        // Assert.Equal(3, record.Size);
        record = resultSet.First();
        Assert.Equal(3, record.Values.Count);

        Assert.NotNull(record.Values[0]);
        Assert.NotNull(record.Values[1]);
        Assert.NotNull(record.Values[2]);

        // record = iterator.Current;
        record = resultSet.Skip(1).Take(1).First();
        Assert.Equal(3, record.Size);

        Assert.NotNull(record.Values[0]);
        Assert.Null(record.Values[1]);
        Assert.Null(record.Values[2]);

        // Test a query that produces 2 records, the first containing a path and the
        // second containing a null value.
        resultSet = graph.Query("social", "MATCH (a) OPTIONAL MATCH p = (a)-[e]->(b) RETURN p");
        Assert.Equal(2, resultSet.Count);
        iterator = resultSet.GetEnumerator();

        record = resultSet.First();
        Assert.Equal(1, record.Size);
        Assert.NotNull(record.Values[0]);

        record = resultSet.Skip(1).First();
        Assert.Equal(1, record.Size);
        Assert.Null(record.Values[0]);
    }

    [Fact]
    public void Test64BitNumber()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        long value = 1L << 40;
        Dictionary<string, object> parameters = new Dictionary<string, object>();
        parameters.Add("val", value);
        ResultSet resultSet = graph.Query("social", "CREATE (n {val:$val}) RETURN n.val", parameters);
        Assert.Equal(1, resultSet.Count);

        // NRedisStack.Graph.Record r = resultSet.GetEnumerator().Current;
        // Assert.Equal(value, r.Values[0]);
        Assert.Equal(value, resultSet.First().GetValue<long>(0));

    }

    [Fact]
    public void TestCachedExecution()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        graph.Query("social", "CREATE (:N {val:1}), (:N {val:2})");

        // First time should not be loaded from execution cache
        Dictionary<string, object> parameters = new Dictionary<string, object>();
        parameters.Add("val", 1L);
        ResultSet resultSet = graph.Query("social", "MATCH (n:N {val:$val}) RETURN n.val", parameters);
        Assert.Equal(1, resultSet.Count);
        // NRedisStack.Graph.Record r = resultSet.GetEnumerator().Current;
        Assert.Equal(parameters["val"], resultSet.First().Values[0]);
        Assert.False(resultSet.Statistics.CachedExecution);

        // Run in loop many times to make sure the query will be loaded
        // from cache at least once
        for (int i = 0; i < 64; i++)
        {
            resultSet = graph.Query("social", "MATCH (n:N {val:$val}) RETURN n.val", parameters);
        }
        Assert.Equal(1, resultSet.Count);
        // r = resultSet.GetEnumerator().Current;
        // Assert.Equal(parameters["val"], r.Values[0]);
        Assert.Equal(parameters["val"], resultSet.First().Values[0]);

        Assert.True(resultSet.Statistics.CachedExecution);
    }

    [Fact]
    public void TestMapDataType()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        Dictionary<string, object> expected = new Dictionary<string, object>();
        expected.Add("a", (long)1);
        expected.Add("b", "str");
        expected.Add("c", null);
        List<object> d = new List<object>();
        d.Add((long)1);
        d.Add((long)2);
        d.Add((long)3);
        expected.Add("d", d);
        expected.Add("e", true);
        Dictionary<string, object> f = new Dictionary<string, object>();
        f.Add("x", (long)1);
        f.Add("y", (long)2);
        expected.Add("f", f);
        ResultSet res = graph.Query("social", "RETURN {a:1, b:'str', c:NULL, d:[1,2,3], e:True, f:{x:1, y:2}}");
        Assert.Equal(1, res.Count);

        var iterator = res.GetEnumerator();
        iterator.MoveNext();
        NRedisStack.Graph.Record r = iterator.Current;
        var actual = r.Values[0];
        Assert.Equal((object)expected, actual);
    }

    [Fact]
    public void TestGeoPointLatLon()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        ResultSet rs = graph.Query("social", "CREATE (:restaurant"
                + " {location: point({latitude:30.27822306, longitude:-97.75134723})})");
        Assert.Equal(1, rs.Statistics.NodesCreated);
        Assert.Equal(1, rs.Statistics.PropertiesSet);

        AssertTestGeoPoint(graph);
    }

    [Fact]
    public void TestGeoPointLonLat()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        ResultSet rs = graph.Query("social", "CREATE (:restaurant"
                + " {location: point({longitude:-97.75134723, latitude:30.27822306})})");
        Assert.Equal(1, rs.Statistics.NodesCreated);
        Assert.Equal(1, rs.Statistics.PropertiesSet);

        AssertTestGeoPoint(graph);
    }

    private void AssertTestGeoPoint(IGraphCommands graph)
    {
        ResultSet results = graph.Query("social", "MATCH (restaurant) RETURN restaurant");
        Assert.Equal(1, results.Count);
        var record = results.GetEnumerator();
        record.MoveNext();
        Assert.Equal(1, record.Current.Size);
        Assert.Equal(new List<string>() { "restaurant" }, record.Current.Header);
        Node node = record.Current.GetValue<Node>(0);
        var property = node.PropertyMap["location"];

        var point = new Point(30.27822306, -97.75134723);
        Assert.Equal((object)(point), property);
    }

    [Fact]
    public void TestPoint()
    {
        var point = new Point(30.27822306, -97.75134723);

        var pointString = point.ToString();
        Assert.Equal("Point{latitude=30.27822306, longitude=-97.75134723}", pointString);
        var pointHash = point.GetHashCode();
        Assert.Equal(847819990, pointHash);
        Assert.Throws<ArgumentOutOfRangeException>(() => new Point(new List<double> { 1, 2, 3 }));
    }

    [Fact]
    public void timeoutArgument()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        ResultSet rs = graph.Query("social", "UNWIND range(0,100) AS x WITH x AS x WHERE x = 100 RETURN x", 1L);
        Assert.Equal(1, rs.Count);
        var iterator = rs.GetEnumerator();
        iterator.MoveNext();
        var r = iterator.Current;
        Assert.Equal(100l, (long)r.Values[0]);
    }

    [Fact]
    public void TestCachedExecutionReadOnly()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        graph.Query("social", "CREATE (:N {val:1}), (:N {val:2})");

        // First time should not be loaded from execution cache
        Dictionary<string, object> parameters = new Dictionary<string, object>();
        parameters.Add("val", 1L);
        ResultSet resultSet = graph.RO_Query("social", "MATCH (n:N {val:$val}) RETURN n.val", parameters);
        Assert.Equal(1, resultSet.Count);
        var iterator = resultSet.GetEnumerator();
        iterator.MoveNext();
        NRedisStack.Graph.Record r = iterator.Current;
        Assert.Equal(parameters["val"], r.Values[0]);
        Assert.False(resultSet.Statistics.CachedExecution);

        // Run in loop many times to make sure the query will be loaded
        // from cache at least once
        for (int i = 0; i < 64; i++)
        {
            resultSet = graph.RO_Query("social", "MATCH (n:N {val:$val}) RETURN n.val", parameters);
        }
        Assert.Equal(1, resultSet.Count);
        iterator = resultSet.GetEnumerator();
        iterator.MoveNext();
        r = iterator.Current;
        Assert.Equal(parameters["val"], r.Values[0]);
        Assert.True(resultSet.Statistics.CachedExecution);
    }

    [Fact]
    public void TestSimpleReadOnly()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        graph.Query("social", "CREATE (:person{name:'filipe',age:30})");
        ResultSet rsRo = graph.RO_Query("social", "MATCH (a:person) WHERE (a.name = 'filipe') RETURN a.age");
        Assert.Equal(1, rsRo.Count);
        var iterator = rsRo.GetEnumerator();
        iterator.MoveNext();
        var r = iterator.Current;
        Assert.Equal("30", r.Values[0].ToString());
    }

    [Fact]
    public void TestProfile()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        Assert.NotNull(graph.Query("social", "CREATE (:person{name:'roi',age:32})"));
        Assert.NotNull(graph.Query("social", "CREATE (:person{name:'amit',age:30})"));

        var profile = graph.Profile("social",
            "MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[:knows]->(b)");
        Assert.NotEmpty(profile);
        foreach (var p in profile)
        {
            Assert.NotNull(p);
        }
    }

    [Fact]
    public void TestExplain()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        Assert.NotNull(graph.Profile("social", "CREATE (:person{name:'roi',age:32})"));
        Assert.NotNull(graph.Profile("social", "CREATE (:person{name:'amit',age:30})"));

        var explain = graph.Explain("social",
            "MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[:knows]->(b)");
        Assert.NotEmpty(explain);
        foreach (var e in explain)
        {
            Assert.NotNull(e);
        }
    }

    [Fact]
    public void TestSlowlog()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        Assert.NotNull(graph.Profile("social", "CREATE (:person{name:'roi',age:32})"));
        Assert.NotNull(graph.Profile("social", "CREATE (:person{name:'amit',age:30})"));

        List<List<string>> slowlogs = graph.Slowlog("social");
        Assert.Equal(2, slowlogs.Count);
        slowlogs.ForEach(sl => Assert.NotEmpty(sl));
        slowlogs.ForEach(sl => sl.ForEach(s => Assert.NotNull(s)));
    }

    [Fact]
    public void TestList()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        Assert.Empty(graph.List());

        graph.Query("social", "CREATE (:person{name:'filipe',age:30})");

        Assert.Equal(new List<string>() { "social" }, graph.List());
    }

    [Fact]
    public void TestConfig()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        graph.Query("social", "CREATE (:person{name:'filipe',age:30})");

        string name = "RESULTSET_SIZE";
        var existingValue = graph.ConfigGet(name)[name];

        Assert.True(graph.ConfigSet(name, 250L));

        var actual = graph.ConfigGet(name);
        Assert.Equal(actual.Count, 1);
        Assert.Equal("250", actual[name].ToString());

        graph.ConfigSet(name, existingValue != null ? existingValue.ToString() : -1);
    }

    [Fact]
    public void TestModulePrefixs()
    {
        IDatabase db1 = redisFixture.Redis.GetDatabase();
        IDatabase db2 = redisFixture.Redis.GetDatabase();

        var graph1 = db1.GRAPH();
        var graph2 = db2.GRAPH();

        Assert.NotEqual(graph1.GetHashCode(), graph2.GetHashCode());
    }

    [Fact]
    public void TestCallProcedureDbLabels()
    {
        var db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        const string graphName = "social";

        var graph = db.GRAPH();
        // Create empty graph, otherwise call procedure will throw exception
        graph.Query(graphName, "RETURN 1");

        var labels0 = graph.CallProcedure(graphName, "db.labels");
        Assert.Empty(labels0);

        graph.Query(graphName, "CREATE (:Person { name: 'Bob' })");

        var labels1 = graph.CallProcedure(graphName, "db.labels");
        Assert.Single(labels1);
    }

    [Fact]
    public void TestCallProcedureReadOnly()
    {
        var db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        const string graphName = "social";

        var graph = db.GRAPH();
        // throws RedisServerException when executing a ReadOnly procedure against non-existing graph.
        Assert.Throws<RedisServerException>(() => graph.CallProcedure(graphName, "db.labels", ProcedureMode.Read));

        graph.Query(graphName, "CREATE (:Person { name: 'Bob' })");
        var procedureArgs = new List<string>
        {
            "Person",
            "name"
        };
        // throws RedisServerException when executing a Write procedure with Read procedure mode.
        Assert.Throws<RedisServerException>(() => graph.CallProcedure(graphName, "db.idx.fulltext.createNodeIndex", procedureArgs, ProcedureMode.Read));
    }

    #endregion

    #region AsyncTests

    [Fact]
    public async Task TestReserveBasicAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
    }

    [Fact]
    public async Task TestCreateNodeAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        // Create a node
        ResultSet resultSet = await graph.QueryAsync("social", "CREATE ({name:'roi',age:32})");

        Statistics stats = resultSet.Statistics;
        Assert.Equal(1, stats.NodesCreated);
        Assert.Equal(0, stats.NodesDeleted);
        Assert.Equal(0, stats.RelationshipsCreated);
        Assert.Equal(0, stats.RelationshipsDeleted);
        Assert.Equal(2, stats.PropertiesSet);
        Assert.NotNull(stats.QueryInternalExecutionTime);

        Assert.Equal(0, resultSet.Count);

        // delete
        await graph.DeleteAsync("social");
    }

    [Fact]
    public async Task TestCreateLabeledNodeAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        // Create a node with a label
        ResultSet resultSet = await graph.QueryAsync("social", "CREATE (:human{name:'danny',age:12})");

        Statistics stats = resultSet.Statistics;
        //        Assert.Equal("1", stats.getstringValue(Label.NODES_CREATED));
        Assert.Equal(1, stats.NodesCreated);
        //        Assert.Equal("2", stats.getstringValue(Label.PROPERTIES_SET));
        Assert.Equal(2, stats.PropertiesSet);
        //        Assert.NotNull(stats.getstringValue(Label.QUERY_INTERNAL_EXECUTION_TIME));
        Assert.NotNull(stats.QueryInternalExecutionTime);

        Assert.Equal(0, resultSet.Count);
        // Assert.False(resultSet..iterator().MoveNext());
    }

    [Fact]
    public async Task TestConnectNodesAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        // Create both source and destination nodes
        Assert.NotNull(await graph.QueryAsync("social", "CREATE (:person{name:'roi',age:32})"));
        Assert.NotNull(await graph.QueryAsync("social", "CREATE (:person{name:'amit',age:30})"));

        // Connect source and destination nodes.
        ResultSet resultSet = await graph.QueryAsync("social",
                "MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[:knows]->(b)");

        Statistics stats = resultSet.Statistics;
        //        Assert.Null(stats.getstringValue(Label.NODES_CREATED));
        Assert.Equal(0, stats.NodesCreated);
        Assert.Equal(1, stats.RelationshipsCreated);
        Assert.Equal(0, stats.RelationshipsDeleted);
        //        Assert.Null(stats.getstringValue(Label.PROPERTIES_SET));
        Assert.Equal(0, stats.PropertiesSet);
        //        Assert.NotNull(stats.getstringValue(Label.QUERY_INTERNAL_EXECUTION_TIME));
        Assert.NotNull(stats.QueryInternalExecutionTime);

        Assert.Equal(0, resultSet.Count);
        // Assert.False(resultSet.GetEnumerator().MoveNext());
    }

    [Fact]
    public async Task TestDeleteNodesAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        Assert.NotNull(await graph.QueryAsync("social", "CREATE (:person{name:'roi',age:32})"));
        Assert.NotNull(await graph.QueryAsync("social", "CREATE (:person{name:'amit',age:30})"));

        ResultSet deleteResult = await graph.QueryAsync("social", "MATCH (a:person) WHERE (a.name = 'roi') DELETE a");

        Statistics delStats = deleteResult.Statistics;
        //        Assert.Null(delStats.getstringValue(Label.NODES_CREATED));
        Assert.Equal(0, delStats.NodesCreated);
        Assert.Equal(1, delStats.NodesDeleted);
        //        Assert.Null(delStats.getstringValue(Label.RELATIONSHIPS_CREATED));
        Assert.Equal(0, delStats.RelationshipsCreated);
        //        Assert.Null(delStats.getstringValue(Label.RELATIONSHIPS_DELETED));
        Assert.Equal(0, delStats.RelationshipsDeleted);
        //        Assert.Null(delStats.getstringValue(Label.PROPERTIES_SET));
        Assert.Equal(0, delStats.PropertiesSet);
        //        Assert.NotNull(delStats.getstringValue(Label.QUERY_INTERNAL_EXECUTION_TIME));
        Assert.NotNull(delStats.QueryInternalExecutionTime);
        Assert.Equal(0, deleteResult.Count);
        // Assert.False(deleteResult.iterator().MoveNext());

        Assert.NotNull(await graph.QueryAsync("social", "CREATE (:person{name:'roi',age:32})"));
        Assert.NotNull(await graph.QueryAsync("social",
                "MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[:knows]->(a)"));

        deleteResult = await graph.QueryAsync("social", "MATCH (a:person) WHERE (a.name = 'roi') DELETE a");

        //        Assert.Null(delStats.getstringValue(Label.NODES_CREATED));
        Assert.Equal(0, delStats.NodesCreated);
        Assert.Equal(1, delStats.NodesDeleted);
        //        Assert.Null(delStats.getstringValue(Label.RELATIONSHIPS_CREATED));
        Assert.Equal(0, delStats.RelationshipsCreated);
        // Assert.Equal(1, delStats.RelationshipsDeleted);
        Assert.Equal(0, delStats.RelationshipsDeleted);
        //        Assert.Null(delStats.getstringValue(Label.PROPERTIES_SET));
        Assert.Equal(0, delStats.PropertiesSet);
        //        Assert.NotNull(delStats.getstringValue(Label.QUERY_INTERNAL_EXECUTION_TIME));
        Assert.NotNull(delStats.QueryInternalExecutionTime);
        Assert.Equal(0, deleteResult.Count);
        // Assert.False(deleteResult.iterator().MoveNext());
    }

    [Fact]
    public async Task TestDeleteRelationshipAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        Assert.NotNull(await graph.QueryAsync("social", "CREATE (:person{name:'roi',age:32})"));
        Assert.NotNull(await graph.QueryAsync("social", "CREATE (:person{name:'amit',age:30})"));
        Assert.NotNull(await graph.QueryAsync("social",
                "MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[:knows]->(a)"));

        ResultSet deleteResult = await graph.QueryAsync("social",
                "MATCH (a:person)-[e]->() WHERE (a.name = 'roi') DELETE e");

        Statistics delStats = deleteResult.Statistics;
        //        Assert.Null(delStats.getstringValue(Label.NODES_CREATED));
        Assert.Equal(0, delStats.NodesCreated);
        //        Assert.Null(delStats.getstringValue(Label.NODES_DELETED));
        Assert.Equal(0, delStats.NodesDeleted);
        //        Assert.Null(delStats.getstringValue(Label.RELATIONSHIPS_CREATED));
        Assert.Equal(0, delStats.RelationshipsCreated);
        Assert.Equal(1, delStats.RelationshipsDeleted);
        //        Assert.Null(delStats.getstringValue(Label.PROPERTIES_SET));
        Assert.Equal(0, delStats.PropertiesSet);
        //        Assert.NotNull(delStats.getstringValue(Label.QUERY_INTERNAL_EXECUTION_TIME));
        Assert.NotNull(delStats.QueryInternalExecutionTime);
        Assert.Equal(0, deleteResult.Count);
        // Assert.False(deleteResult.iterator().MoveNext());
    }

    [Fact]
    public async Task TestIndexAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        // Create both source and destination nodes
        Assert.NotNull(await graph.QueryAsync("social", "CREATE (:person{name:'roi',age:32})"));

        ResultSet createIndexResult = await graph.QueryAsync("social", "CREATE INDEX ON :person(age)");
        Assert.Empty(createIndexResult);
        Assert.Equal(1, createIndexResult.Statistics.IndicesCreated);

        // since RediSearch as index, those action are allowed
        ResultSet createNonExistingIndexResult = await graph.QueryAsync("social", "CREATE INDEX ON :person(age1)");
        Assert.Empty(createNonExistingIndexResult);
        Assert.Equal(1, createNonExistingIndexResult.Statistics.IndicesCreated);

        ResultSet createExistingIndexResult = await graph.QueryAsync("social", "CREATE INDEX ON :person(age)");
        Assert.Empty(createExistingIndexResult);
        Assert.Equal(0, createExistingIndexResult.Statistics.IndicesCreated);

        ResultSet deleteExistingIndexResult = await graph.QueryAsync("social", "DROP INDEX ON :person(age)");
        Assert.Empty(deleteExistingIndexResult);
        Assert.Equal(1, deleteExistingIndexResult.Statistics.IndicesDeleted);
    }

    [Fact]
    public async Task TestHeaderAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();

        Assert.NotNull(await graph.QueryAsync("social", "CREATE (:person{name:'roi',age:32})"));
        Assert.NotNull(await graph.QueryAsync("social", "CREATE (:person{name:'amit',age:30})"));
        Assert.NotNull(await graph.QueryAsync("social",
                "MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[:knows]->(a)"));

        ResultSet queryResult = await graph.QueryAsync("social", "MATCH (a:person)-[r:knows]->(b:person) RETURN a,r, a.age");

        Header header = queryResult.Header;
        Assert.NotNull(header);
        Assert.Equal("Header{"
                //                + "schemaTypes=[COLUMN_SCALAR, COLUMN_SCALAR, COLUMN_SCALAR], "
                + "schemaTypes=[SCALAR, SCALAR, SCALAR], "
                + "schemaNames=[a, r, a.age]}", header.ToString());
        // Assert.Assert.Equal(-1901778507, header.hashCode());

        List<string> schemaNames = header.SchemaNames;

        Assert.NotNull(schemaNames);
        Assert.Equal(3, schemaNames.Count);
        Assert.Equal("a", schemaNames[0]);
        Assert.Equal("r", schemaNames[1]);
        Assert.Equal("a.age", schemaNames[2]);
    }

    [Fact]
    public async Task TestRecordAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        string name = "roi";
        int age = 32;
        double doubleValue = 3.14;
        bool boolValue = true;

        string place = "TLV";
        int since = 2000;

        var nameProperty = new KeyValuePair<string, object>("name", name);
        var ageProperty = new KeyValuePair<string, object>("age", age);
        var doubleProperty = new KeyValuePair<string, object>("doubleValue", doubleValue);
        var trueboolProperty = new KeyValuePair<string, object>("boolValue", true);
        var falseboolProperty = new KeyValuePair<string, object>("boolValue", false);
        var placeProperty = new KeyValuePair<string, object>("place", place);
        var sinceProperty = new KeyValuePair<string, object>("since", since);

        Node expectedNode = new Node();
        expectedNode.Id = 0;
        expectedNode.Labels.Add("person");
        expectedNode.PropertyMap.Add(nameProperty);
        expectedNode.PropertyMap.Add(ageProperty);
        expectedNode.PropertyMap.Add(doubleProperty);
        expectedNode.PropertyMap.Add(trueboolProperty);
        Assert.Equal(
               "Node{labels=[person], id=0, "
                        + "propertyMap={name=roi, age=32, doubleValue=3.14, boolValue=True}}",
                expectedNode.ToString());
        // "Node{labels=[person], id=0, propertyMap={name=Property{name='name', value=roi}, age=Property{name='age', value=32}, doubleValue=Property{name='doubleValue', value=3.14}, boolValue=Property{name='boolValue', value=True}}}"
        // "Node{labels=[person], id=0, propertyMap={name=Property{name='name', value=roi}, boolValue=Property{name='boolValue', value=true}, doubleValue=Property{name='doubleValue', value=3.14}, age=Property{name='age', value=32}}}"
        Edge expectedEdge = new Edge();
        expectedEdge.Id = 0;
        expectedEdge.Source = 0;
        expectedEdge.Destination = 1;
        expectedEdge.RelationshipType = "knows";
        expectedEdge.PropertyMap.Add(placeProperty);
        expectedEdge.PropertyMap.Add(sinceProperty);
        expectedEdge.PropertyMap.Add(doubleProperty);
        expectedEdge.PropertyMap.Add(falseboolProperty);
        Assert.Equal("Edge{relationshipType='knows', source=0, destination=1, id=0, "
                + "propertyMap={place=TLV, since=2000, doubleValue=3.14, boolValue=False}}", expectedEdge.ToString());

        Dictionary<string, object> parameters = new Dictionary<string, object>();
        parameters.Add("name", name);
        parameters.Add("age", age);
        parameters.Add("boolValue", boolValue);
        parameters.Add("doubleValue", doubleValue);

        Assert.NotNull(await graph.QueryAsync("social",
                "CREATE (:person{name:$name,age:$age, doubleValue:$doubleValue, boolValue:$boolValue})", parameters));
        Assert.NotNull(await graph.QueryAsync("social", "CREATE (:person{name:'amit',age:30})"));
        Assert.NotNull(
                await graph.QueryAsync("social", "MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  " +
                        "CREATE (a)-[:knows{place:'TLV', since:2000,doubleValue:3.14, boolValue:false}]->(b)"));

        ResultSet resultSet = await graph.QueryAsync("social", "MATCH (a:person)-[r:knows]->(b:person) RETURN a,r, " +
                "a.name, a.age, a.doubleValue, a.boolValue, " +
                "r.place, r.since, r.doubleValue, r.boolValue");
        Assert.NotNull(resultSet);

        Statistics stats = resultSet.Statistics;
        Assert.Equal(0, stats.NodesCreated);
        Assert.Equal(0, stats.NodesDeleted);
        Assert.Equal(0, stats.LabelsAdded);
        Assert.Equal(0, stats.PropertiesSet);
        Assert.Equal(0, stats.RelationshipsCreated);
        Assert.Equal(0, stats.RelationshipsDeleted);
        Assert.NotNull(stats.QueryInternalExecutionTime);
        Assert.NotEmpty(stats.QueryInternalExecutionTime);

        Assert.Equal(1, resultSet.Count);
        // IReadOnlyCollection<Record> iterator = resultSet.GetEnumerator();
        var iterator = resultSet.GetEnumerator();
        Assert.True(iterator.MoveNext());
        var record = iterator.Current;
        Assert.False(iterator.MoveNext());

        Node node = record.GetValue<Node>(0);
        Assert.NotNull(node);

        Assert.Equal(expectedNode.ToString(), node.ToString());
        //Expected: "Node{labels=[person], id=0, propertyMap={name=Property{name='name', value=roi}, age=Property{name='age', value=32}, doubleValue=Property{name='doubleValue', value=3.14}, boolValue=Property{name='boolValue', value=True}}}"
        //Actual   :"Node{labels=[person], id=0, propertyMap={name=Property{name='name', value=roi}, age=Property{name='age', value=32}, doubleValue=Property{name='doubleValue', value=3.14}, boolValue=Property{name='boolValue', value=True}}}"

        node = record.GetValue<Node>("a");
        Assert.Equal(expectedNode.ToString(), node.ToString());

        Edge edge = record.GetValue<Edge>(1);
        Assert.NotNull(edge);
        Assert.Equal(expectedEdge.ToString(), edge.ToString());

        edge = record.GetValue<Edge>("r");
        Assert.Equal(expectedEdge.ToString(), edge.ToString());

        Assert.Equal(new List<string>(){"a", "r", "a.name", "a.age", "a.doubleValue", "a.boolValue",
                "r.place", "r.since", "r.doubleValue", "r.boolValue"}, record.Header);

        List<object> expectedList = new List<object>() {expectedNode, expectedEdge,
                name, (long)age, doubleValue, true,
                place, (long)since, doubleValue, false};


        for (int i = 0; i < expectedList.Count; i++)
        {
            Assert.Equal(expectedList[i].ToString(), record.Values[i].ToString());
        }

        Node a = record.GetValue<Node>("a");
        foreach (string propertyName in expectedNode.PropertyMap.Keys)
        {
            Assert.Equal(expectedNode.PropertyMap[propertyName].ToString(), a.PropertyMap[propertyName].ToString());
        }

        Assert.Equal("roi", record.GetString(2));
        Assert.Equal("32", record.GetString(3));
        Assert.Equal(32L, (record.GetValue<long>(3)));
        Assert.Equal(32L, (record.GetValue<long>("a.age")));
        Assert.Equal("roi", record.GetString("a.name"));
        Assert.Equal("32", record.GetString("a.age"));

    }

    [Fact]
    public async Task TestAdditionToProceduresAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();

        Assert.NotNull(await graph.QueryAsync("social", "CREATE (:person{name:'roi',age:32})"));
        Assert.NotNull(await graph.QueryAsync("social", "CREATE (:person{name:'amit',age:30})"));
        Assert.NotNull(await graph.QueryAsync("social",
                "MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[:knows]->(b)"));

        // expected objects init
        var nameProperty = new KeyValuePair<string, object>("name", "roi");
        var ageProperty = new KeyValuePair<string, object>("age", 32);
        var lastNameProperty = new KeyValuePair<string, object>("lastName", "a");

        Node expectedNode = new Node();
        expectedNode.Id = 0;
        expectedNode.Labels.Add("person");
        expectedNode.PropertyMap.Add(nameProperty);
        expectedNode.PropertyMap.Add(ageProperty);

        Edge expectedEdge = new Edge();
        expectedEdge.Id = 0;
        expectedEdge.Source = 0;
        expectedEdge.Destination = 1;
        expectedEdge.RelationshipType = "knows";

        ResultSet resultSet = await graph.QueryAsync("social", "MATCH (a:person)-[r:knows]->(b:person) RETURN a,r");
        Assert.NotNull(resultSet.Header);
        Header header = resultSet.Header;
        List<string> schemaNames = header.SchemaNames;
        Assert.NotNull(schemaNames);
        Assert.Equal(2, schemaNames.Count);
        Assert.Equal("a", schemaNames[0]);
        Assert.Equal("r", schemaNames[1]);
        Assert.Equal(1, resultSet.Count);
        var iterator = resultSet.GetEnumerator();
        Assert.True(iterator.MoveNext());
        var record = iterator.Current;
        Assert.False(iterator.MoveNext());
        Assert.Equal(new List<string>() { "a", "r" }, record.Header);
        Assert.Equal(expectedNode.ToString(), record.Values[0].ToString());
        Assert.Equal(expectedEdge.ToString(), record.Values[1].ToString());

        // test for local cache updates

        expectedNode.PropertyMap.Remove("name");
        expectedNode.PropertyMap.Remove("age");
        expectedNode.PropertyMap.Add(lastNameProperty);
        expectedNode.Labels.Remove("person");
        expectedNode.Labels.Add("worker");
        expectedNode.Id = 2;
        expectedEdge.RelationshipType = "worksWith";
        expectedEdge.Source = 2;
        expectedEdge.Destination = 3;
        expectedEdge.Id = 1;
        Assert.NotNull(await graph.QueryAsync("social", "CREATE (:worker{lastName:'a'})"));
        Assert.NotNull(await graph.QueryAsync("social", "CREATE (:worker{lastName:'b'})"));
        Assert.NotNull(await graph.QueryAsync("social",
                "MATCH (a:worker), (b:worker) WHERE (a.lastName = 'a' AND b.lastName='b')  CREATE (a)-[:worksWith]->(b)"));
        resultSet = await graph.QueryAsync("social", "MATCH (a:worker)-[r:worksWith]->(b:worker) RETURN a,r");
        Assert.NotNull(resultSet.Header);
        header = resultSet.Header;
        schemaNames = header.SchemaNames;
        Assert.NotNull(schemaNames);
        Assert.Equal(2, schemaNames.Count);
        Assert.Equal("a", schemaNames[0]);
        Assert.Equal("r", schemaNames[1]);
        Assert.Equal(1, resultSet.Count);
        iterator = resultSet.GetEnumerator();
        Assert.True(iterator.MoveNext());
        record = iterator.Current;
        Assert.False(iterator.MoveNext());
        Assert.Equal(new List<string> { "a", "r" }, record.Header);
        Assert.Equal(expectedNode.ToString(), record.Values[0].ToString());
        Assert.Equal(expectedEdge.ToString(), record.Values[1].ToString());
    }

    [Fact]
    public async Task TestEscapedQueryAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        Dictionary<string, object> params1 = new Dictionary<string, object>();
        params1.Add("s1", "S\"'");
        params1.Add("s2", "S'\"");
        Assert.NotNull(await graph.QueryAsync("social", "CREATE (:escaped{s1:$s1,s2:$s2})", params1));

        Dictionary<string, object> params2 = new Dictionary<string, object>();
        params2.Add("s1", "S\"'");
        params2.Add("s2", "S'\"");
        Assert.NotNull(await graph.QueryAsync("social", "MATCH (n) where n.s1=$s1 and n.s2=$s2 RETURN n", params2));

        Assert.NotNull(await graph.QueryAsync("social", "MATCH (n) where n.s1='S\"' RETURN n"));
    }

    [Fact]
    public async Task TestArraySupportAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();

        Node expectedANode = new Node();
        expectedANode.Id = 0;
        expectedANode.Labels.Add("person");
        var aNameProperty = new KeyValuePair<string, object>("name", "a");
        var aAgeProperty = new KeyValuePair<string, object>("age", 32L);
        var aListProperty = new KeyValuePair<string, object>("array", new object[] { 0L, 1L, 2L });
        expectedANode.PropertyMap.Add(aNameProperty);
        expectedANode.PropertyMap.Add(aAgeProperty);
        expectedANode.PropertyMap.Add(aListProperty);

        Node expectedBNode = new Node();
        expectedBNode.Id = 1;
        expectedBNode.Labels.Add("person");
        var bNameProperty = new KeyValuePair<string, object>("name", "b");
        var bAgeProperty = new KeyValuePair<string, object>("age", 30L);
        var bListProperty = new KeyValuePair<string, object>("array", new object[] { 3L, 4L, 5L });
        expectedBNode.PropertyMap.Add(bNameProperty);
        expectedBNode.PropertyMap.Add(bAgeProperty);
        expectedBNode.PropertyMap.Add(bListProperty);

        Assert.NotNull(await graph.QueryAsync("social", "CREATE (:person{name:'a',age:32,array:[0,1,2]})"));
        Assert.NotNull(await graph.QueryAsync("social", "CREATE (:person{name:'b',age:30,array:[3,4,5]})"));

        // test array

        ResultSet resultSet = await graph.QueryAsync("social", "WITH [0,1,2] as x return x");

        // check header
        Assert.NotNull(resultSet.Header);
        Header header = resultSet.Header;

        List<string> schemaNames = header.SchemaNames;
        Assert.NotNull(schemaNames);
        Assert.Equal(1, schemaNames.Count);
        Assert.Equal("x", schemaNames[0]);

        // check record
        Assert.Equal(1, resultSet.Count);
        var iterator = resultSet.GetEnumerator();
        Assert.True(iterator.MoveNext());
        NRedisStack.Graph.Record record = iterator.Current;
        Assert.False(iterator.MoveNext());
        Assert.Equal(new List<string>() { "x" }, record.Header);

        var x = record.GetValue<object[]>("x");
        Assert.Equal(new object[] { 0L, 1L, 2L }, x);

        // test collect
        resultSet = await graph.QueryAsync("social", "MATCH(n) return collect(n) as x");

        Assert.NotNull(resultSet.Header);
        header = resultSet.Header;

        schemaNames = header.SchemaNames;
        Assert.NotNull(schemaNames);
        Assert.Equal(1, schemaNames.Count);
        Assert.Equal("x", schemaNames[0]);

        // check record
        Assert.Equal(1, resultSet.Count);
        iterator = resultSet.GetEnumerator();
        Assert.True(iterator.MoveNext());
        record = iterator.Current;
        Assert.False(iterator.MoveNext());
        Assert.Equal(new List<string>() { "x" }, record.Header);
        var x2 = record.GetValue<object[]>("x");

        Assert.Equal(expectedANode.ToString(), x2[0].ToString());
        Assert.Equal(expectedBNode.ToString(), x2[1].ToString());

        // test unwind
        resultSet = await graph.QueryAsync("social", "unwind([0,1,2]) as x return x");

        Assert.NotNull(resultSet.Header);
        header = resultSet.Header;

        schemaNames = header.SchemaNames;
        Assert.NotNull(schemaNames);
        Assert.Equal(1, schemaNames.Count);
        Assert.Equal("x", schemaNames[0]);

        // check record
        Assert.Equal(3, resultSet.Count);
        iterator = resultSet.GetEnumerator();
        for (long i = 0; i < 3; i++)
        {
            Assert.True(iterator.MoveNext());
            record = iterator.Current;
            Assert.Equal(new List<string>() { "x" }, record.Header);
            Assert.Equal(i, (long)record.GetValue<long>("x"));
        }
    }

    [Fact]
    public async Task TestPathAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        List<Node> nodes = new List<Node>(3);
        for (int i = 0; i < 3; i++)
        {
            Node node = new Node();
            node.Id = i;
            node.Labels.Add("L1");
            nodes.Add(node);
        }

        List<Edge> edges = new List<Edge>(2);
        for (int i = 0; i < 2; i++)
        {
            Edge edge = new Edge();
            edge.Id = i;
            edge.RelationshipType = "R1";
            edge.Source = i;
            edge.Destination = i + 1;
            edges.Add(edge);
        }

        var expectedPaths = new HashSet<NRedisStack.Graph.DataTypes.Path>();

        NRedisStack.Graph.DataTypes.Path path01 = new PathBuilder(2).Append(nodes[0]).Append(edges[0]).Append(nodes[1]).Build();
        NRedisStack.Graph.DataTypes.Path path12 = new PathBuilder(2).Append(nodes[1]).Append(edges[1]).Append(nodes[2]).Build();
        NRedisStack.Graph.DataTypes.Path path02 = new PathBuilder(3).Append(nodes[0]).Append(edges[0]).Append(nodes[1])
                .Append(edges[1]).Append(nodes[2]).Build();

        expectedPaths.Add(path01);
        expectedPaths.Add(path12);
        expectedPaths.Add(path02);

        await graph.QueryAsync("social", "CREATE (:L1)-[:R1]->(:L1)-[:R1]->(:L1)");

        ResultSet resultSet = await graph.QueryAsync("social", "MATCH p = (:L1)-[:R1*]->(:L1) RETURN p");

        Assert.Equal(expectedPaths.Count, resultSet.Count);
        var iterator = resultSet.GetEnumerator();

        for (int i = 0; i < resultSet.Count; i++)
        {
            NRedisStack.Graph.DataTypes.Path p = resultSet.ElementAt(i).GetValue<NRedisStack.Graph.DataTypes.Path>("p");
            Assert.Contains(p, expectedPaths);
            expectedPaths.Remove(p);
        }
    }

    [Fact]
    public async Task TestNullGraphEntitiesAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        // Create two nodes connected by a single outgoing edge.
        Assert.NotNull(await graph.QueryAsync("social", "CREATE (:L)-[:E]->(:L2)"));
        // Test a query that produces 1 record with 3 null values.
        ResultSet resultSet = await graph.QueryAsync("social", "OPTIONAL MATCH (a:NONEXISTENT)-[e]->(b) RETURN a, e, b");
        Assert.Equal(1, resultSet.Count);
        IEnumerator<NRedisStack.Graph.Record> iterator = resultSet.GetEnumerator();
        Assert.True(iterator.MoveNext());
        NRedisStack.Graph.Record record = iterator.Current;
        Assert.False(iterator.MoveNext());
        Assert.Equal(new List<object>() { null, null, null }, record.Values);

        // Test a query that produces 2 records, with 2 null values in the second.
        resultSet = await graph.QueryAsync("social", "MATCH (a) OPTIONAL MATCH (a)-[e]->(b) RETURN a, e, b ORDER BY ID(a)");
        Assert.Equal(2, resultSet.Count);

        // iterator = resultSet.GetEnumerator();
        // record = iterator.Current;
        // Assert.Equal(3, record.Size);
        record = resultSet.First();
        Assert.Equal(3, record.Values.Count);

        Assert.NotNull(record.Values[0]);
        Assert.NotNull(record.Values[1]);
        Assert.NotNull(record.Values[2]);

        // record = iterator.Current;
        record = resultSet.Skip(1).Take(1).First();
        Assert.Equal(3, record.Size);

        Assert.NotNull(record.Values[0]);
        Assert.Null(record.Values[1]);
        Assert.Null(record.Values[2]);

        // Test a query that produces 2 records, the first containing a path and the
        // second containing a null value.
        resultSet = await graph.QueryAsync("social", "MATCH (a) OPTIONAL MATCH p = (a)-[e]->(b) RETURN p");
        Assert.Equal(2, resultSet.Count);
        iterator = resultSet.GetEnumerator();

        record = resultSet.First();
        Assert.Equal(1, record.Size);
        Assert.NotNull(record.Values[0]);

        record = resultSet.Skip(1).First();
        Assert.Equal(1, record.Size);
        Assert.Null(record.Values[0]);
    }

    [Fact]
    public async Task Test64bitnumberAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        long value = 1L << 40;
        Dictionary<string, object> parameters = new Dictionary<string, object>();
        parameters.Add("val", value);
        ResultSet resultSet = await graph.QueryAsync("social", "CREATE (n {val:$val}) RETURN n.val", parameters);
        Assert.Equal(1, resultSet.Count);

        // NRedisStack.Graph.Record r = resultSet.GetEnumerator().Current;
        // Assert.Equal(value, r.Values[0]);
        Assert.Equal(value, resultSet.First().GetValue<long>(0));

    }

    [Fact]
    public async Task TestCachedExecutionAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        await graph.QueryAsync("social", "CREATE (:N {val:1}), (:N {val:2})");

        // First time should not be loaded from execution cache
        Dictionary<string, object> parameters = new Dictionary<string, object>();
        parameters.Add("val", 1L);
        ResultSet resultSet = await graph.QueryAsync("social", "MATCH (n:N {val:$val}) RETURN n.val", parameters);
        Assert.Equal(1, resultSet.Count);
        // NRedisStack.Graph.Record r = resultSet.GetEnumerator().Current;
        Assert.Equal(parameters["val"], resultSet.First().Values[0]);
        Assert.False(resultSet.Statistics.CachedExecution);

        // Run in loop many times to make sure the query will be loaded
        // from cache at least once
        for (int i = 0; i < 64; i++)
        {
            resultSet = await graph.QueryAsync("social", "MATCH (n:N {val:$val}) RETURN n.val", parameters);
        }
        Assert.Equal(1, resultSet.Count);
        // r = resultSet.GetEnumerator().Current;
        // Assert.Equal(parameters["val"], r.Values[0]);
        Assert.Equal(parameters["val"], resultSet.First().Values[0]);

        Assert.True(resultSet.Statistics.CachedExecution);
    }

    [Fact]
    public async Task TestMapDataTypeAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        Dictionary<string, object> expected = new Dictionary<string, object>();
        expected.Add("a", (long)1);
        expected.Add("b", "str");
        expected.Add("c", null);
        List<object> d = new List<object>();
        d.Add((long)1);
        d.Add((long)2);
        d.Add((long)3);
        expected.Add("d", d);
        expected.Add("e", true);
        Dictionary<string, object> f = new Dictionary<string, object>();
        f.Add("x", (long)1);
        f.Add("y", (long)2);
        expected.Add("f", f);
        ResultSet res = await graph.QueryAsync("social", "RETURN {a:1, b:'str', c:NULL, d:[1,2,3], e:True, f:{x:1, y:2}}");
        Assert.Equal(1, res.Count);

        var iterator = res.GetEnumerator();
        iterator.MoveNext();
        NRedisStack.Graph.Record r = iterator.Current;
        var actual = r.Values[0];
        Assert.Equal((object)expected, actual);
    }

    [Fact]
    public async Task TestGeoPointLatLonAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        ResultSet rs = await graph.QueryAsync("social", "CREATE (:restaurant"
                + " {location: point({latitude:30.27822306, longitude:-97.75134723})})");
        Assert.Equal(1, rs.Statistics.NodesCreated);
        Assert.Equal(1, rs.Statistics.PropertiesSet);

        AssertTestGeoPoint(graph);
    }

    [Fact]
    public async Task TestGeoPointLonLatAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        ResultSet rs = await graph.QueryAsync("social", "CREATE (:restaurant"
                + " {location: point({longitude:-97.75134723, latitude:30.27822306})})");
        Assert.Equal(1, rs.Statistics.NodesCreated);
        Assert.Equal(1, rs.Statistics.PropertiesSet);

        AssertTestGeoPoint(graph);
    }

    private async Task AssertTestGeoPointAsync(GraphCommands graph)
    {
        ResultSet results = await graph.QueryAsync("social", "MATCH (restaurant) RETURN restaurant");
        Assert.Equal(1, results.Count);
        var record = results.GetEnumerator();
        record.MoveNext();
        Assert.Equal(1, record.Current.Size);
        Assert.Equal(new List<string>() { "restaurant" }, record.Current.Header);
        Node node = record.Current.GetValue<Node>(0);
        var property = node.PropertyMap["location"];

        Assert.Equal((object)(new Point(30.27822306, -97.75134723)), property);
    }

    [Fact]
    public async Task timeoutArgumentAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        ResultSet rs = await graph.QueryAsync("social", "UNWIND range(0,100) AS x WITH x AS x WHERE x = 100 RETURN x", 1L);
        Assert.Equal(1, rs.Count);
        var iterator = rs.GetEnumerator();
        iterator.MoveNext();
        var r = iterator.Current;
        Assert.Equal(100l, (long)r.Values[0]);
    }

    [Fact]
    public async Task TestCachedExecutionReadOnlyAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        await graph.QueryAsync("social", "CREATE (:N {val:1}), (:N {val:2})");

        // First time should not be loaded from execution cache
        Dictionary<string, object> parameters = new Dictionary<string, object>();
        parameters.Add("val", 1L);
        ResultSet resultSet = await graph.RO_QueryAsync("social", "MATCH (n:N {val:$val}) RETURN n.val", parameters);
        Assert.Equal(1, resultSet.Count);
        var iterator = resultSet.GetEnumerator();
        iterator.MoveNext();
        NRedisStack.Graph.Record r = iterator.Current;
        Assert.Equal(parameters["val"], r.Values[0]);
        Assert.False(resultSet.Statistics.CachedExecution);

        // Run in loop many times to make sure the query will be loaded
        // from cache at least once
        for (int i = 0; i < 64; i++)
        {
            resultSet = await graph.RO_QueryAsync("social", "MATCH (n:N {val:$val}) RETURN n.val", parameters);
        }
        Assert.Equal(1, resultSet.Count);
        iterator = resultSet.GetEnumerator();
        iterator.MoveNext();
        r = iterator.Current;
        Assert.Equal(parameters["val"], r.Values[0]);
        Assert.True(resultSet.Statistics.CachedExecution);
    }

    [Fact]
    public async Task TestSimpleReadOnlyAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        await graph.QueryAsync("social", "CREATE (:person{name:'filipe',age:30})");
        ResultSet rsRo = await graph.RO_QueryAsync("social", "MATCH (a:person) WHERE (a.name = 'filipe') RETURN a.age");
        Assert.Equal(1, rsRo.Count);
        var iterator = rsRo.GetEnumerator();
        iterator.MoveNext();
        var r = iterator.Current;
        Assert.Equal("30", r.Values[0].ToString());
    }

    [Fact]
    public async Task TestProfileAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        Assert.NotNull(await graph.QueryAsync("social", "CREATE (:person{name:'roi',age:32})"));
        Assert.NotNull(await graph.QueryAsync("social", "CREATE (:person{name:'amit',age:30})"));

        var profile = await graph.ProfileAsync("social",
            "MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[:knows]->(b)");
        Assert.NotEmpty(profile);
        foreach (var p in profile)
        {
            Assert.NotNull(p);
        }
    }

    [Fact]
    public async Task TestExplainAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        Assert.NotNull(await graph.ProfileAsync("social", "CREATE (:person{name:'roi',age:32})"));
        Assert.NotNull(await graph.ProfileAsync("social", "CREATE (:person{name:'amit',age:30})"));

        var explain = await graph.ExplainAsync("social",
            "MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[:knows]->(b)");
        Assert.NotEmpty(explain);
        foreach (var e in explain)
        {
            Assert.NotNull(e);
        }
    }

    [Fact]
    public async Task TestSlowlogAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        Assert.NotNull(await graph.ProfileAsync("social", "CREATE (:person{name:'roi',age:32})"));
        Assert.NotNull(await graph.ProfileAsync("social", "CREATE (:person{name:'amit',age:30})"));

        List<List<string>> slowlogs = await graph.SlowlogAsync("social");
        Assert.Equal(2, slowlogs.Count);
        slowlogs.ForEach(sl => Assert.NotEmpty(sl));
        slowlogs.ForEach(sl => sl.ForEach(s => Assert.NotNull(s)));
    }

    [Fact]
    public async Task TestListAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        Assert.Empty(await graph.ListAsync());

        await graph.QueryAsync("social", "CREATE (:person{name:'filipe',age:30})");

        Assert.Equal(new List<string>() { "social" }, await graph.ListAsync());
    }

    [Fact]
    public async Task TestConfigAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        await graph.QueryAsync("social", "CREATE (:person{name:'filipe',age:30})");

        string name = "RESULTSET_SIZE";
        var existingValue = (await graph.ConfigGetAsync(name))[name];

        Assert.True(await graph.ConfigSetAsync(name, 250L));

        var actual = await graph.ConfigGetAsync(name);
        Assert.Equal(actual.Count, 1);
        Assert.Equal("250", actual[name].ToString());

        await graph.ConfigSetAsync(name, existingValue != null ? existingValue.ToString() : -1);
    }

    [Fact]
    public async Task TestModulePrefixsAsync()
    {
        IDatabase db1 = redisFixture.Redis.GetDatabase();
        IDatabase db2 = redisFixture.Redis.GetDatabase();

        var graph1 = db1.GRAPH();
        var graph2 = db2.GRAPH();

        Assert.NotEqual(graph1.GetHashCode(), graph2.GetHashCode());
    }

    [Fact]
    public async Task TestCallProcedureDbLabelsAsync()
    {
        var db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        const string graphName = "social";

        var graph = db.GRAPH();
        // Create empty graph, otherwise call procedure will throw exception
        await graph.QueryAsync(graphName, "RETURN 1");

        var labels0 = await graph.CallProcedureAsync(graphName, "db.labels");
        Assert.Empty(labels0);

        await graph.QueryAsync(graphName, "CREATE (:Person { name: 'Bob' })");

        var labels1 = await graph.CallProcedureAsync(graphName, "db.labels");
        Assert.Single(labels1);
    }

    [Fact]
    public async Task TestCallProcedureReadOnlyAsync()
    {
        var db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        const string graphName = "social";

        var graph = db.GRAPH();
        // throws RedisServerException when executing a ReadOnly procedure against non-existing graph.
        await Assert.ThrowsAsync<RedisServerException>(() => graph.CallProcedureAsync(graphName, "db.labels", ProcedureMode.Read));

        await graph.QueryAsync(graphName, "CREATE (:Person { name: 'Bob' })");
        var procedureArgs = new List<string>
        {
            "Person",
            "name"
        };
        // throws RedisServerException when executing a Write procedure with Read procedure mode.
        await Assert.ThrowsAsync<RedisServerException>(() => graph.CallProcedureAsync(graphName, "db.idx.fulltext.createNodeIndex", procedureArgs, ProcedureMode.Read));
    }

    [Fact]
    public void TestParseInfinity()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        ResultSet rs = graph.Query("db", "RETURN 10^100000");
        Assert.Equal(1, rs.Count());
        var iterator = rs.GetEnumerator();
        iterator.MoveNext();
        var r = iterator.Current;
        Assert.Equal(double.PositiveInfinity, r.Values[0]);
    }

    [Fact]
    public void TestEqualsAndToString()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        ResultSet resultSet1 = graph.Query("db", "RETURN 10^100000");
        ResultSet resultSet2 = graph.Query("db", "RETURN 10^1000");
        var iterator1 = resultSet1.GetEnumerator();
        Assert.True(iterator1.MoveNext());
        var record1 = iterator1.Current;
        var iterator2 = resultSet2.GetEnumerator();
        Assert.True(iterator2.MoveNext());
        var record2 = iterator2.Current;

        Assert.True(resultSet1.Header.Equals(resultSet1.Header));
        Assert.False(resultSet1.Header.Equals(resultSet2.Header));
        Assert.False(resultSet1.Header.Equals(new object()));
        Assert.False(resultSet1.Header.Equals(null));

        Assert.True(record1.Equals(record1));
        Assert.False(record1.Equals(record2));
        Assert.False(record1.Equals(new object()));
        Assert.False(record1.Equals(null));

        var edge1 = new Edge();
        var edge1Copy = new Edge();
        var edge2 = new Edge();
        var node1 = new Node();
        var node1Copy = new Node();
        var node2 = new Node();

        edge1.Id = 1;
        edge1Copy.Id = 1;
        edge2.Id = 2;
        node1.Id = 1;
        node1Copy.Id = 1;
        node2.Id = 2;

        Assert.False(edge1.Equals(edge2));
        Assert.False(node1.Equals(node2));
        Assert.True(edge1.Equals(edge1Copy));
        Assert.True(edge1.Equals(edge1));
        Assert.True(node1.Equals(node1Copy));
        Assert.True(node1.Equals(node1));
        Assert.False(node1.Equals(edge1));
        Assert.False(edge1.Equals(node1));
        Assert.False(node1.Equals(null));
        Assert.False(edge1.Equals(null));



        var path = new NRedisStack.Graph.DataTypes.Path(new List<Node>() { node1, node2 },
                                                        new List<Edge>() { edge1, edge2 });
        var pathCopy = new NRedisStack.Graph.DataTypes.Path(new List<Node>() { node1, node2 },
                                                            new List<Edge>() { edge1, edge2 });
        var path2 = new NRedisStack.Graph.DataTypes.Path(new List<Node>() { node1, node2 },
                                                         new List<Edge>() { edge1 });
        Assert.True(path.Equals(pathCopy));
        Assert.True(path.Equals(path));
        Assert.False(path.Equals(path2));
        Assert.False(path.Equals(node1));

        Assert.True(record1.ToString() == "Record{values=Infinity}" || record1.ToString() == "Record{values=∞}");
        Assert.NotEqual(record2.GetHashCode(), record1.GetHashCode());

        var node1String = node1.ToString();
        var edge1String = edge1.ToString();
        var pathString = path.ToString();
        var expectedNode1String = "Node{labels=[], id=1, propertyMap={}}";
        var expectedEdge1String = "Edge{relationshipType='', source=0, destination=0, id=1, propertyMap={}}";
        var expectedPathString = "Path{nodes=System.Collections.ObjectModel.ReadOnlyCollection`1[NRedisStack.Graph.DataTypes.Node], edges=System.Collections.ObjectModel.ReadOnlyCollection`1[NRedisStack.Graph.DataTypes.Edge]}";
        Assert.Equal(expectedNode1String, node1String);
        Assert.Equal(expectedEdge1String, edge1String);
        Assert.Equal(expectedPathString, pathString);
    }

    [Fact]
    public void TestPrepareQuery()
    {
        const string return1Query = "RETURN 1";
        const string return1QueryRecordString = "Record{values=1}";

        var graph = redisFixture.Redis.GetDatabase().GRAPH();

        // handle chars
        var buildCommand = GraphCommandBuilder.Query("graph", return1Query, new Dictionary<string, object> { { "a", (char)'c' }} );
        var expectedPreparedQuery1 = $"CYPHER a=\"c\" {return1Query}";
        Assert.Equal(expectedPreparedQuery1, buildCommand.Args[1].ToString()!);
        var res1 = graph.Query("graph", buildCommand.Args[1].ToString()!);
        Assert.Single(res1);
        Assert.Equal(return1QueryRecordString, res1.Single().ToString());

        // handle null
        var buildCommand2 = GraphCommandBuilder.Query("graph", return1Query, new Dictionary<string, object> { { "a", null } });
        var expectedPreparedQuery2 = $"CYPHER a=null {return1Query}";
        Assert.Equal(expectedPreparedQuery2,buildCommand2.Args[1].ToString()!);
        var res2 = graph.Query("graph",buildCommand2.Args[1].ToString()!);
        Assert.Single(res2);
        Assert.Equal(return1QueryRecordString, res2.Single().ToString());

        // handle arrays
        var buildCommand3 = GraphCommandBuilder.Query("graph", return1Query, new Dictionary<string, object> { { "a", new string[] { "foo", "bar" } } });
        var expectedPreparedQuery3 = $"CYPHER a=[\"foo\", \"bar\"] {return1Query}";
        Assert.Equal(expectedPreparedQuery3,buildCommand3.Args[1].ToString()!);
        var res3 = graph.Query("graph",buildCommand3.Args[1].ToString()!);
        Assert.Single(res3);
        Assert.Equal(return1QueryRecordString, res3.Single().ToString());

        // handle lists
        var buildCommand4 = GraphCommandBuilder.Query("graph", return1Query, new Dictionary<string, object> { { "a", new List<string> { "foo2", "bar2" } } });
        var expectedPreparedQuery4 = $"CYPHER a=[\"foo2\", \"bar2\"] {return1Query}";
        Assert.Equal(expectedPreparedQuery4,buildCommand4.Args[1].ToString()!);
        var res4 = graph.Query("graph",buildCommand4.Args[1].ToString()!);
        Assert.Single(res4);
        Assert.Equal(return1QueryRecordString, res4.Single().ToString());

        // handle bools
        var buildCommand5 = GraphCommandBuilder.Query("graph", return1Query, new Dictionary<string, object> { { "a", true }, { "b", false } });
        var expectedPreparedQuery5 = $"CYPHER a=true b=false {return1Query}";
        Assert.Equal(expectedPreparedQuery5,buildCommand5.Args[1].ToString()!);
        var res5 = graph.Query("graph",buildCommand5.Args[1].ToString()!);
        Assert.Single(res5);
        Assert.Equal(return1QueryRecordString, res4.Single().ToString());

        // handle floats
        var buildCommand6 = GraphCommandBuilder.Query("graph", return1Query, new Dictionary<string, object> { { "a", 1.4d } });
        var expectedPreparedQuery6 = $"CYPHER a=1.4 {return1Query}";
        Assert.Equal(expectedPreparedQuery6,buildCommand6.Args[1].ToString()!);
        var res6 = graph.Query("graph",buildCommand6.Args[1].ToString()!);
        Assert.Single(res6);
        Assert.Equal(return1QueryRecordString, res4.Single().ToString());

        // handle ints
        var buildCommand7 = GraphCommandBuilder.Query("graph", return1Query, new Dictionary<string, object> { { "a", 5 } });
        var expectedPreparedQuery7 = $"CYPHER a=5 {return1Query}";
        Assert.Equal(expectedPreparedQuery7,buildCommand7.Args[1].ToString()!);
        var res7 = graph.Query("graph",buildCommand7.Args[1].ToString()!);
        Assert.Single(res7);
        Assert.Equal(return1QueryRecordString, res4.Single().ToString());

        // handle quotes
        var buildCommand8 = GraphCommandBuilder.Query("graph", return1Query, new Dictionary<string, object> { { "a", "\"abc\"" } });
        var expectedPreparedQuery8 = $"CYPHER a=\"\\\"abc\\\"\" {return1Query}";
        Assert.Equal(expectedPreparedQuery8,buildCommand8.Args[1].ToString()!);
        var res8 = graph.Query("graph",buildCommand8.Args[1].ToString()!);
        Assert.Single(res8);
        Assert.Equal(return1QueryRecordString, res5.Single().ToString());

        // handle backslashes
        var buildCommand9 = GraphCommandBuilder.Query("graph", return1Query, new Dictionary<string, object> { { "a", "abc\\" } });
        var expectedPreparedQuery9 = $"CYPHER a=\"abc\\\\\" {return1Query}";
        Assert.Equal(expectedPreparedQuery9,buildCommand9.Args[1].ToString()!);
        var res9 = graph.Query("graph",buildCommand9.Args[1].ToString()!);
        Assert.Single(res9);
        Assert.Equal(return1QueryRecordString, res6.Single().ToString());
    }
    #endregion

}