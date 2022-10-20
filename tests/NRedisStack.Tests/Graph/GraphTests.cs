using Xunit;
using StackExchange.Redis;
using NRedisStack.RedisStackCommands;
using Moq;
using NRedisStack.Graph;
using System.Drawing;

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

    [Fact]
    public void TestReserveBasic()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();

    }

    [Fact]
    public void testCreateNode()
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

        // Assert.False(resultSet.GetEnumerator().MoveNext());

        // try {
        //     resultSet..iterator().Current;
        //     fail();
        // } catch (NoSuchElementException ignored) {
        // }
    }

    [Fact]
    public void testCreateLabeledNode()
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
    public void testConnectNodes()
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
        // Assert.False(resultSet.GetEnumerator().MoveNext());
    }

    [Fact]
    public void testDeleteNodes()
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
    public void testDeleteRelationship()
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
    public void testIndex()
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
    public void testHeader()
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

    // TODO: finish the tests
    [Fact]
    public void testRecord()
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

        Property nameProperty = new Property("name", name);
        Property ageProperty = new Property("age", age);
        Property doubleProperty = new Property("doubleValue", doubleValue);
        Property trueboolProperty = new Property("boolValue", true);
        Property falseboolProperty = new Property("boolValue", false);

        Property placeProperty = new Property("place", place);
        Property sinceProperty = new Property("since", since);

        Node expectedNode = new Node();
        expectedNode.Id = 0;
        expectedNode.AddLabel("person");
        expectedNode.AddProperty(nameProperty);
        expectedNode.AddProperty(ageProperty);
        expectedNode.AddProperty(doubleProperty);
        expectedNode.AddProperty(trueboolProperty);
        Assert.Equal(
                "Node{labels=[person], id=0, "
                        + "propertyMap={name=Property{name='name', value=roi}, "
                        + "age=Property{name='age', value=32}, "
                        + "doubleValue=Property{name='doubleValue', value=3.14}, "
                        + "boolValue=Property{name='boolValue', value=True}}}",
                expectedNode.ToString());
        // "Node{labels=[person], id=0, propertyMap={name=Property{name='name', value=roi}, age=Property{name='age', value=32}, doubleValue=Property{name='doubleValue', value=3.14}, boolValue=Property{name='boolValue', value=True}}}"
        // "Node{labels=[person], id=0, propertyMap={name=Property{name='name', value=roi}, boolValue=Property{name='boolValue', value=true}, doubleValue=Property{name='doubleValue', value=3.14}, age=Property{name='age', value=32}}}"
        Edge expectedEdge = new Edge();
        expectedEdge.Id = 0;
        expectedEdge.Source = 0;
        expectedEdge.Destination = 1;
        expectedEdge.RelationshipType = "knows";
        expectedEdge.AddProperty(placeProperty);
        expectedEdge.AddProperty(sinceProperty);
        expectedEdge.AddProperty(doubleProperty);
        expectedEdge.AddProperty(falseboolProperty);
        Assert.Equal("Edge{relationshipType='knows', source=0, destination=1, id=0, "
                + "propertyMap={place=Property{name='place', value=TLV}, "
                + "since=Property{name='since', value=2000}, "
                + "doubleValue=Property{name='doubleValue', value=3.14}, "
                + "boolValue=Property{name='boolValue', value=False}}}", expectedEdge.ToString());

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
                "r.place", "r.since", "r.doubleValue", "r.boolValue"}, record.Keys);

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
    public void testAdditionToProcedures()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();

        Assert.NotNull(graph.Query("social", "CREATE (:person{name:'roi',age:32})"));
        Assert.NotNull(graph.Query("social", "CREATE (:person{name:'amit',age:30})"));
        Assert.NotNull(graph.Query("social",
                "MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[:knows]->(b)"));

        // expected objects init
        Property nameProperty = new Property("name", "roi");
        Property ageProperty = new Property("age", 32);
        Property lastNameProperty = new Property("lastName", "a");

        Node expectedNode = new Node();
        expectedNode.Id = 0;
        expectedNode.AddLabel("person");
        expectedNode.AddProperty(nameProperty);
        expectedNode.AddProperty(ageProperty);

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
        Assert.Equal(new List<string>() { "a", "r" }, record.Keys);
        Assert.Equal(expectedNode.ToString(), record.Values[0].ToString());
        Assert.Equal(expectedEdge.ToString(), record.Values[1].ToString());

        // test for local cache updates

        expectedNode.RemoveProperty("name");
        expectedNode.RemoveProperty("age");
        expectedNode.AddProperty(lastNameProperty);
        expectedNode.RemoveProperty("person");
        expectedNode.AddLabel("worker");
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
        Assert.Equal(new List<string> { "a", "r" }, record.Keys);
        Assert.Equal(expectedNode.ToString(), record.Values[0].ToString());
        Assert.Equal(expectedEdge.ToString(), record.Values[1].ToString());
    }

    [Fact]
    public void testEscapedQuery()
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
    public void testArraySupport()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();

        Node expectedANode = new Node();
        expectedANode.Id = 0;
        expectedANode.AddLabel("person");
        Property aNameProperty = new Property("name", "a");
        Property aAgeProperty = new Property("age", 32);
        var aListProperty = new Property("array", new List<long>() { 0L, 1L, 2L });
        expectedANode.AddProperty(aNameProperty);
        expectedANode.AddProperty(aAgeProperty);
        expectedANode.AddProperty(aListProperty);

        Node expectedBNode = new Node();
        expectedBNode.Id = 1;
        expectedBNode.AddLabel("person");
        Property bNameProperty = new Property("name", "b");
        Property bAgeProperty = new Property("age", 30);
        var bListProperty = new Property("array", new List<long>() { 3L, 4L, 5L });
        expectedBNode.AddProperty(bNameProperty);
        expectedBNode.AddProperty(bAgeProperty);
        expectedBNode.AddProperty(bListProperty);

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
        var record = iterator.Current;
        Assert.False(iterator.MoveNext());
        Assert.Equal(new List<string>() { "x" }, record.Keys);

        List<long> x = record.GetValue<List<long>>("x"); // TODO: Check This
        Assert.Equal(new List<long>() { 0L, 1L, 2L }, x);

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
        Assert.Equal(new List<string>() { "x" }, record.Keys);
        x = record.GetValue<List<long>>("x");
        Assert.Equal(expectedANode.ToString(), x[0].ToString());
        Assert.Equal(expectedBNode.ToString(), x[1].ToString());

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
            Assert.Equal(new List<string>() { "x" }, record.Keys);
            Assert.Equal(i, (long)record.GetValue<long>("x"));
        }
    }

    [Fact]
    public void testPath()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var graph = db.GRAPH();
        List<Node> nodes = new List<Node>(3);
        for (int i = 0; i < 3; i++)
        {
            Node node = new Node();
            node.Id = i;
            node.AddLabel("L1");
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

        var expectedPaths = new HashSet<NRedisStack.Graph.Path>();

        NRedisStack.Graph.Path path01 = new PathBuilder(2).Append(nodes[0]).Append(edges[0]).Append(nodes[1]).Build();
        NRedisStack.Graph.Path path12 = new PathBuilder(2).Append(nodes[1]).Append(edges[1]).Append(nodes[2]).Build();
        NRedisStack.Graph.Path path02 = new PathBuilder(3).Append(nodes[0]).Append(edges[0]).Append(nodes[1])
                .Append(edges[1]).Append(nodes[2]).Build();

        expectedPaths.Add(path01);
        expectedPaths.Add(path12);
        expectedPaths.Add(path02);

        graph.Query("social", "CREATE (:L1)-[:R1]->(:L1)-[:R1]->(:L1)");

        ResultSet resultSet = graph.Query("social", "MATCH p = (:L1)-[:R1*]->(:L1) RETURN p");

        Assert.Equal(expectedPaths.Count, resultSet.Count);
        var iterator = resultSet.GetEnumerator();
        // for (int i = 0; i < resultSet.Count; i++) {
        // var p = iterator.Current.GetValue<NRedisStack.Graph.Path>("p");
        //     Assert.True(expectedPaths.Contains(p));
        //     expectedPaths.Remove(p);
        // }
        for (int i = 0; i < resultSet.Count; i++)
        {
            NRedisStack.Graph.Path p = resultSet.ElementAt(i).GetValue<NRedisStack.Graph.Path>("p");
            Assert.Contains(p, expectedPaths);
            expectedPaths.Remove(p);
        }
    }

    [Fact]
    public void testNullGraphEntities()
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
    public void test64bitnumber()
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
        public void testCachedExecution()
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
            for (int i = 0; i < 64; i++) {
                resultSet = graph.Query("social", "MATCH (n:N {val:$val}) RETURN n.val", parameters);
            }
            Assert.Equal(1, resultSet.Count);
            // r = resultSet.GetEnumerator().Current;
            // Assert.Equal(parameters["val"], r.Values[0]);
            Assert.Equal(parameters["val"], resultSet.First().Values[0]);

            Assert.True(resultSet.Statistics.CachedExecution);
        }

    // TODO: fix this test
    //     [Fact]
    //     public void testMapDataType()
    // {
    //         IDatabase db = redisFixture.Redis.GetDatabase();
    //         db.Execute("FLUSHALL");
    //         var graph = db.GRAPH();
    //         Dictionary<string, object> expected = new Dictionary<string, object>();
    //         expected.Add("a", (long) 1);
    //         expected.Add("b", "str");
    //         expected.Add("c", null);
    //         List<object> d = new List<object>();
    //         d.Add((long) 1);
    //         d.Add((long) 2);
    //         d.Add((long) 3);
    //         expected.Add("d", d);
    //         expected.Add("e", true);
    //         Dictionary<string, object> f = new Dictionary<string, object>();
    //         f.Add("x", (long) 1);
    //         f.Add("y", (long) 2);
    //         expected.Add("f", f);
    //         ResultSet res = graph.Query("social", "RETURN {a:1, b:'str', c:NULL, d:[1,2,3], e:True, f:{x:1, y:2}}");
    //         Assert.Equal(1, res.Count);

    //         // var actual = res.First().Values[0];

    //         // Record r = res.iterator().Current;
    //         // var r2 = res.GetEnumerator().Current;
    //         var r = res.GetEnumerator();
    //         var firstCurrent = r.Current;
    //         r.MoveNext();
    //         var secondCurrent = r.Current;
    //         Dictionary<string, object> actual = r.Current.GetValue<Dictionary<string,object>>(0);
    //         // r.MoveNext();
    //         // var thirdCurrent = r.Current;
    //         // var check = r.Current.;
    //         // var current = r.Current;
    //         // var values = current.Values;
    //         // var keys = current.Keys;
    //         //var actual = current.Values[0];
    //         // var actual2 = current.GetValue<Dictionary<string, object>>(0);
    //         // Dictionary<string, object> actual = new Dictionary<string, object>();
    //         // for(int i = 0; i < keys.Count; i++)
    //         // {
    //         //     actual.Add(keys[i], values[i]);
    //         // }
    //         // Dictionary<string, object> actual = r.Values[0];
    //         Assert.Equal(expected, actual);
    //     }

    //     [Fact]
    //     public void testGeoPointLatLon()
    // {
    //         IDatabase db = redisFixture.Redis.GetDatabase();
    //         db.Execute("FLUSHALL");
    //         var graph = db.GRAPH();
    //         ResultSet rs = graph.Query("social", "CREATE (:restaurant"
    //                 + " {location: point({latitude:30.27822306, longitude:-97.75134723})})");
    //         Assert.Equal(1, rs.Statistics.NodesCreated);
    //         Assert.Equal(1, rs.Statistics.PropertiesSet);

    //         AssertTestGeoPoint();
    //     }

    //     [Fact]
    //     public void testGeoPointLonLat()
    // {
    //         IDatabase db = redisFixture.Redis.GetDatabase();
    //         db.Execute("FLUSHALL");
    //         var graph = db.GRAPH();
    //         ResultSet rs = graph.Query("social", "CREATE (:restaurant"
    //                 + " {location: point({longitude:-97.75134723, latitude:30.27822306})})");
    //         Assert.Equal(1, rs.Statistics.NodesCreated);
    //         Assert.Equal(1, rs.Statistics.PropertiesSet);

    //         AssertTestGeoPoint();
    //     }

    //     private void AssertTestGeoPoint()
    // {
    //         IDatabase db = redisFixture.Redis.GetDatabase();
    //         db.Execute("FLUSHALL");
    //         var graph = db.GRAPH();
    //         ResultSet results = graph.Query("social", "MATCH (restaurant) RETURN restaurant");
    //         Assert.Equal(1, results.Count);
    //         var record = results.GetEnumerator();
    //         record.MoveNext();
    //         Assert.Equal(1, record.Current.Size);
    //         Assert.Equal(Collections.singletonList("restaurant"), record.Keys);
    //         Node node = record.Values[0];
    //         Property<?> property = node.getProperty("location");
    //         Assert.Equal(new Point(30.27822306, -97.75134723), property.GetValue());
    //     }

    //     [Fact]
    //     public void timeoutArgument()
    // {
    //         IDatabase db = redisFixture.Redis.GetDatabase();
    //         db.Execute("FLUSHALL");
    //         var graph = db.GRAPH();
    //         ResultSet rs = graph.Query("social", "UNWIND range(0,100) AS x WITH x AS x WHERE x = 100 RETURN x", 1L);
    //         Assert.Equal(1, rs.Count);
    //         Record r = rs.iterator().Current;
    //         Assert.Equal(Long.valueOf(100), r.Values[0]);
    //     }

        [Fact]
        public void testCachedExecutionReadOnly()
    {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var graph = db.GRAPH();
            graph.Query("social", "CREATE (:N {val:1}), (:N {val:2})");

            // First time should not be loaded from execution cache
            Dictionary<string, object> parameters = new Dictionary<string, object>();
            parameters.Add("val", 1L);
            ResultSet resultSet =   graph.RO_Query("social", "MATCH (n:N {val:$val}) RETURN n.val", parameters);
            Assert.Equal(1, resultSet.Count);
            var iterator = resultSet.GetEnumerator();
            iterator.MoveNext();
        NRedisStack.Graph.Record r = iterator.Current;
            Assert.Equal(parameters["val"], r.Values[0]);
            Assert.False(resultSet.Statistics.CachedExecution);

            // Run in loop many times to make sure the query will be loaded
            // from cache at least once
            for (int i = 0; i < 64; i++) {
                resultSet =   graph.RO_Query("social", "MATCH (n:N {val:$val}) RETURN n.val", parameters);
            }
            Assert.Equal(1, resultSet.Count);
            iterator = resultSet.GetEnumerator();
            iterator.MoveNext();
            r = iterator.Current;
            Assert.Equal(parameters["val"], r.Values[0]);
            Assert.True(resultSet.Statistics.CachedExecution);
        }

        [Fact]
        public void testSimpleReadOnly()
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
    // TODO: cpmplete this test after adding support for GRAPH.PROFILE/CONFIG/LIST
    //   [Fact]
    //   public void profile()
    // {
    //         IDatabase db = redisFixture.Redis.GetDatabase();
    //         db.Execute("FLUSHALL");
    //         var graph = db.GRAPH();
    //     Assert.NotNull(graph.Query("social", "CREATE (:person{name:'roi',age:32})"));
    //     Assert.NotNull(graph.Query("social", "CREATE (:person{name:'amit',age:30})"));

    //     List<string> profile = graph.Profile("social",
    //         "MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[:knows]->(b)");
    //     Assert.False(profile.isEmpty());
    //     profile.forEach(Assert::assertNotNull);
    //   }

    //   [Fact]
    //   public void explain()
    // {
    //         IDatabase db = redisFixture.Redis.GetDatabase();
    //         db.Execute("FLUSHALL");
    //         var graph = db.GRAPH();
    //     Assert.NotNull(graph.Profile("social", "CREATE (:person{name:'roi',age:32})"));
    //     Assert.NotNull(graph.Profile("social", "CREATE (:person{name:'amit',age:30})"));

    //     List<string> explain = graph.Explain("social",
    //         "MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[:knows]->(b)");
    //     Assert.False(explain.isEmpty());
    //     explain.forEach(Assert::assertNotNull);
    //   }

    //   [Fact]
    //   public void slowlog()
    // {
    //         IDatabase db = redisFixture.Redis.GetDatabase();
    //         db.Execute("FLUSHALL");
    //         var graph = db.GRAPH();
    //     Assert.NotNull(graph.Profile("social", "CREATE (:person{name:'roi',age:32})"));
    //     Assert.NotNull(graph.Profile("social", "CREATE (:person{name:'amit',age:30})"));

    //     List<List<string>> slowlogs = graph.Slowlog("social");
    //     Assert.Equal(2, slowlogs.Count);
    //     slowlogs.forEach(sl -> Assert.False(sl.isEmpty()));
    //     slowlogs.forEach(sl -> sl.forEach(Assert::assertNotNull));
    //   }

    //   [Fact]
    //   public void list()
    // {
    //         IDatabase db = redisFixture.Redis.GetDatabase();
    //         db.Execute("FLUSHALL");
    //         var graph = db.GRAPH();
    //     Assert.Equal(Collections.emptyList(), graph.List());

    //     graph.Query("social", "CREATE (:person{name:'filipe',age:30})");

    //     Assert.Equal(Collections.singletonList("social"), graph.List());
    //   }

    //   [Fact]
    //   public void config()
    // {
    //         IDatabase db = redisFixture.Redis.GetDatabase();
    //         db.Execute("FLUSHALL");
    //         var graph = db.GRAPH();
    //     graph.Query("social", "CREATE (:person{name:'filipe',age:30})");

    //     final string name = "RESULTSET_SIZE";
    //     final object existingValue = graph.ConfigGet(name).get(name);

    //     Assert.Equal("OK", graph.ConfigSet(name, 250L));
    //     Assert.Equal(Collections.singletonMap(name, 250L), graph.ConfigGet(name));

    //     graph.ConfigSet(name, existingValue != null ? existingValue : -1);
    //   }

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
    public void TestModulePrefixs1()
    {
        {
            var conn = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = conn.GetDatabase();

            var graph = db.GRAPH();
            // ...
            conn.Dispose();
        }

        {
            var conn = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = conn.GetDatabase();

            var graph = db.GRAPH();
            // ...
            conn.Dispose();
        }

    }
}