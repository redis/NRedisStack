using NRedisStack.Core.DataTypes;
using NRedisStack.RedisStackCommands;
using NRedisStack.Search.Literals.Enums;
using NRedisStack.Search;
using NRedisStack;
using StackExchange.Redis;
using System.Collections.Generic;
using System.Threading;
using System;
using static NRedisStack.Search.Schema;

/// <summary>
/// The goal of this code is to verify that the binary package is working correctly.
/// It should be orchestrated in such a way that the binary package is retrieved from
/// a local NuGet source and then the code is executed.
/// </summary>
public class PackageVerification
{
    public static void Main()
    {
        ConfigurationOptions configurationOptions = new ConfigurationOptions();
        configurationOptions.SyncTimeout = 20000;
        configurationOptions.EndPoints.Add("localhost:6379");
        ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(configurationOptions);

        IDatabase db = redis.GetDatabase();
        db.Execute(new SerializedCommand("FLUSHALL", RequestPolicy.AllShards));

        IJsonCommands json = db.JSON();

        json.Set("product:15970", "$", new
        {
            id = 15970,
            description = "Turtle Navy Blue Shirt",
            price = 34.95,
        });

        json.Set("product:59263", "$", new
        {
            id = 59263,
            description = "Titan Silver Watch",
            price = 129.99,
        });

        ISearchCommands ft = db.FT();

        try
        {
            ft.DropIndex("idx1");
        }
        catch
        {
        }

        ft.Create("idx1", new FTCreateParams().On(IndexDataType.JSON)
                                              .Prefix("product:"),
                                        new Schema().AddNumericField(new FieldName("$.id", "id"))
                                                    .AddTextField(new FieldName("$.description", "description"))
                                                    .AddNumericField(new FieldName("$.price", "price")));

        // wait for index to be created
        Thread.Sleep(2000);

        List<string> results = ft.Search("idx1", new Query("@description:Blue")).ToJson();

        Assert(1 == results.Count);

        var expected = "{\"id\":15970,\"description\":\"Turtle Navy Blue Shirt\",\"price\":34.95}";
        Assert(expected == results[0]);

        Console.WriteLine("All good.");
    }

    /// <summary>
    /// Poor Man's assert, since we don't want to depend on NUnit.
    /// </summary>
    private static void Assert(bool condition)
    {
        if (!condition)
        {
            throw new System.Exception();
        }
    }
}
