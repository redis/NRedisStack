using Xunit;
using System;
using System.Collections.Generic;
using StackExchange.Redis;
using System.Linq;
using System.IO;
using RediSharp.Core;
using Moq;

using System.Text.Json;
using System.Text.Json.Serialization;


namespace RediSharp.Tests;

public class UnitTest1
{
    static ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("127.0.0.1");
    public static IDatabase db = redis.GetDatabase();

    Mock<IDatabase> _mock = new Mock<IDatabase>();
    public class Person
    {
        public string Name { get; set; }
        public int Age { get; set; }
    }

    [Fact]
    public void TestJsonSet()
    {
        ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
        IDatabase db = redis.GetDatabase();

        var obj = new Person { Name = "Shachar", Age = 23 };

        db.JsonSet("Person:Shachar", "$", obj);
        //var expected = new[] {"JSON.SET", "Person:Shachar", "$", "{\"Name\":\"Shachar\",\"Age\":23}" };


        //_mock.Setup(x => x.Execute("JSON.SET", It.IsAny<string[]>())).Returns(Redis));
        //_mock.Verify(x => x.Execute("JSON.SET", "Person:Shachar", "$", "{\"Name\":\"Shachar\",\"Age\":23}" ));
        // ADD MOCK and CHECK HOW THE JSON LOOKS AFTER PARSING
        System.Console.WriteLine(JsonSerializer.Serialize(obj).ToString());



    }
}