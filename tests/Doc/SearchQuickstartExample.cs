// EXAMPLE: search_quickstart
using NRedisStack.RedisStackCommands;
using NRedisStack.Search;
using NRedisStack.Search.Aggregation;
using NRedisStack.Search.Literals.Enums;
using StackExchange.Redis;

// REMOVE_START
namespace NRedisStack.Doc;
// REMOVE_END

public class SearchQuickstartExample
{
    [Fact]
    public void run()
    {
        // STEP_START connect
        var redis = ConnectionMultiplexer.Connect("localhost:6379");
        var db = redis.GetDatabase();
        var ft = db.FT();
        var json = db.JSON();
        // STEP_END

        // REMOVE_START    
        try
        {
            ft.DropIndex("idx:bicycle");
        }
        catch
        {
        }
        // REMOVE_END

        // STEP_START data_sample
        var bike1 = new
        {
            Brand = "Diaz Ltd",
            Model = "Dealer Sl",
            Price = 7315.58M,
            Description = "The Diaz Ltd Dealer Sl is a reliable choice" +
                          " for urban cycling. The Diaz Ltd Dealer Sl " +
                          "is a comfortable choice for urban cycling.",
            Condition = "used"
        };
        // STEP_END

        var bicycles = new[]
        {
            bike1,
            new
            {
                Brand = "Bridges Group",
                Model = "Project Pro",
                Price = 3610.82M,
                Description =
                    "This mountain bike is perfect for mountain biking. " +
                    "The Bridges Group Project Pro is a responsive choice" +
                    " for mountain biking.",
                Condition = "used"
            },
            new
            {
                Brand = "Vega, Cole and Miller",
                Model = "Group Advanced",
                Price = 8961.42M,
                Description =
                    "The Vega, Cole and Miller Group Advanced provides an " +
                    "excellent ride. With its fast carbon frame and 24 gears," +
                    " this bicycle is perfect for any terrain.",
                Condition = "used"
            },
            new
            {
                Brand = "Powell-Montgomery",
                Model = "Angle Race",
                Price = 4050.27M,
                Description =
                    "The Powell-Montgomery Angle Race is a smooth choice for" +
                    " road cycling. The Powell-Montgomery Angle Race" +
                    " provides a durable ride.",
                Condition = "used"
            },
            new
            {
                Brand = "Gill-Lewis",
                Model = "Action Evo",
                Price = 283.68M,
                Description =
                    "The Gill-Lewis Action Evo provides a smooth ride. " +
                    "The Gill-Lewis Action Evo provides an excellent ride.",
                Condition = "used"
            },
            new
            {
                Brand = "Rodriguez-Guerrero",
                Model = "Drama Comp",
                Price = 4462.55M,
                Description =
                    "This kids bike is perfect for young riders. With its " +
                    "excellent aluminum frame and 12 gears, this bicycle " +
                    "is perfect for any terrain.",
                Condition = "new"
            },
            new
            {
                Brand = "Moore PLC",
                Model = "Award Race",
                Price = 3790.76M,
                Description =
                    "This olive folding bike features a carbon frame and" +
                    " 27.5 inch wheels. This folding bike is perfect for" +
                    " compact storage and transportation.",
                Condition = "new"
            },
            new
            {
                Brand = "Hall, Haley and Hayes",
                Model = "Weekend Plus",
                Price = 2008.4M,
                Description =
                    "The Hall, Haley and Hayes Weekend Plus provides a" +
                    " comfortable ride. This blue kids bike features a" +
                    " steel frame and 29.0 inch wheels.",
                Condition = "new"
            },
            new
            {
                Brand = "Peck-Carson",
                Model = "Sun Hybrid",
                Price = 9874.95M,
                Description =
                    "With its comfortable aluminum frame and 25 gears," +
                    " this bicycle is perfect for any terrain. The " +
                    "Peck-Carson Sun Hybrid provides a comfortable ride.",
                Condition = "new"
            },
            new
            {
                Brand = "Fowler Ltd",
                Model = "Weekend Trail",
                Price = 3833.71M,
                Description =
                    "The Fowler Ltd Letter Trail is a comfortable choice" +
                    " for transporting cargo. This cargo bike is " +
                    "perfect for transporting cargo.",
                Condition = "refurbished"
            }
        };

        // STEP_START define_index
        var schema = new Schema()
            .AddTextField(new FieldName("$.Brand", "Brand"))
            .AddTextField(new FieldName("$.Model", "Model"))
            .AddTextField(new FieldName("$.Description", "Description"))
            .AddNumericField(new FieldName("$.Price", "Price"))
            .AddTagField(new FieldName("$.Condition", "Condition"));
        // STEP_END

        // STEP_START create_index
        ft.Create(
            "idx:bicycle",
            new FTCreateParams().On(IndexDataType.JSON).Prefix("bicycle:"),
            schema);
        // STEP_END

        // STEP_START add_documents
        for (int i = 0; i < bicycles.Length; i++)
        {
            json.Set($"bicycle:{i}", "$", bicycles[i]);
        }
        // STEP_END

        // STEP_START query_single_term_and_num_range
        var query = new Query("folding @Price:[1000 4000]");
        var res = ft.Search("idx:bicycle", query).Documents;
        Console.WriteLine(string.Join("\n", res.Select(x => x["json"])));
        // Prints: {"Brand":"Moore PLC","Model":"Award Race","Price":3790.76,
        //          "Description":"This olive folding bike features a carbon frame
        //          and 27.5 inch wheels. This folding bike is perfect for compact
        //          storage and transportation.","Condition":"new"}
        // STEP_END
        // REMOVE_START
        Assert.Single(res);
        Assert.Equal("bicycle:6", res[0].Id);
        // REMOVE_END

        // STEP_START query_single_term_limit_fields
        var cargoQuery = new Query("cargo").ReturnFields("Price");
        var cargoRes = ft.Search("idx:bicycle", cargoQuery).Documents;
        Console.WriteLine(cargoRes.First()["Price"]);
        // Prints: 3833.71
        // STEP_END
        // REMOVE_START
        Assert.Single(cargoRes);
        Assert.Equal("bicycle:9", cargoRes[0].Id);
        // REMOVE_END

        // STEP_START simple_aggregation
        var request = new AggregationRequest("*").GroupBy(
            "@Condition", Reducers.Count().As("Count"));
        var result = ft.Aggregate("idx:bicycle", request);

        for (var i = 0; i < result.TotalResults; i++)
        {
            var row = result.GetRow(i);
            Console.WriteLine($"{row["Condition"]} - {row["Count"]}");
        }

        // Prints:
        // refurbished - 1
        // used - 5
        // new - 4
        // STEP_END
        // REMOVE_START
        Assert.Equal(3, result.TotalResults);
        // REMOVE_END
    }
}