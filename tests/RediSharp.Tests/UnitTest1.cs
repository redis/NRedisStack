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
    Mock<IDatabase> _mock = new Mock<IDatabase>();
    public class Person
    {
        public string Name { get; set; }
        public int Age { get; set; }
    }

    [Fact]
    public void TestJsonSet()
    {
        var obj = new Person { Name = "Shachar", Age = 23 };
        _mock.Object.JsonSet("Person:Shachar", "$", obj);
        _mock.Verify(x => x.Execute("JSON.SET", "Person:Shachar", "$", "{\"Name\":\"Shachar\",\"Age\":23}"));
    }

    [Fact]
    public void TestJsonSetExist()
    {
        var obj = new Person { Name = "Shachar", Age = 23 };
        _mock.Object.JsonSet("Person:Shachar", "$", obj, When.Exists);
        _mock.Verify(x => x.Execute("JSON.SET", "Person:Shachar", "$", "{\"Name\":\"Shachar\",\"Age\":23}", "XX"));
    }

    [Fact]
    public void TestJsonSetNotExist()
    {
        var obj = new Person { Name = "Shachar", Age = 23 };
        _mock.Object.JsonSet("Person:Shachar", "$", obj, When.NotExists);
        _mock.Verify(x => x.Execute("JSON.SET", "Person:Shachar", "$", "{\"Name\":\"Shachar\",\"Age\":23}", "NX"));
    }
}