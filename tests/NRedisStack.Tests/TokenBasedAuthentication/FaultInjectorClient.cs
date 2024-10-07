using System.Text;
using System.Text.Json;

public class FaultInjectorClient
{
    private static readonly string BASE_URL;

    static FaultInjectorClient()
    {
        BASE_URL = Environment.GetEnvironmentVariable("FAULT_INJECTION_API_URL") ?? "http://127.0.0.1:20324";
    }

    public class TriggerActionResponse
    {
        public string ActionId { get; }
        private DateTime? LastRequestTime { get; set; }
        private DateTime? CompletedAt { get; set; }
        private DateTime? FirstRequestAt { get; set; }

        public TriggerActionResponse(string actionId)
        {
            ActionId = actionId;
        }

        public async Task<bool> IsCompletedAsync(TimeSpan checkInterval, TimeSpan delayAfter, TimeSpan timeout)
        {
            if (CompletedAt.HasValue)
            {
                return DateTime.UtcNow - CompletedAt.Value >= delayAfter;
            }

            if (FirstRequestAt.HasValue && DateTime.UtcNow - FirstRequestAt.Value >= timeout)
            {
                throw new TimeoutException("Timeout");
            }

            if (!LastRequestTime.HasValue || DateTime.UtcNow - LastRequestTime.Value >= checkInterval)
            {
                LastRequestTime = DateTime.UtcNow;

                if (!FirstRequestAt.HasValue)
                {
                    FirstRequestAt = LastRequestTime;
                }

                using var httpClient = GetHttpClient();
                var request = new HttpRequestMessage(HttpMethod.Get, $"{BASE_URL}/action/{ActionId}");

                try
                {
                    var response = await httpClient.SendAsync(request);
                    var result = await response.Content.ReadAsStringAsync();


                    if (result.Contains("success"))
                    {
                        CompletedAt = DateTime.UtcNow;
                        return DateTime.UtcNow - CompletedAt.Value >= delayAfter;
                    }
                }
                catch (HttpRequestException e)
                {
                    throw new Exception("Fault injection proxy error", e);
                }
            }
            return false;
        }
    }

    private static HttpClient GetHttpClient()
    {
        var httpClient = new HttpClient
        {
            Timeout = TimeSpan.FromMilliseconds(5000)
        };
        return httpClient;
    }

    public async Task<TriggerActionResponse> TriggerActionAsync(string actionType, Dictionary<string, object> parameters)
    {
        var payload = new Dictionary<string, object>
    {
      { "type", actionType },
      { "parameters", parameters }
    };

        var jsonString = JsonSerializer.Serialize(payload, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });

        using var httpClient = GetHttpClient();
        var request = new HttpRequestMessage(HttpMethod.Post, $"{BASE_URL}/action")
        {
            Content = new StringContent(jsonString, Encoding.UTF8, "application/json")
        };

        try
        {
            var response = await httpClient.SendAsync(request);
            var result = await response.Content.ReadAsStringAsync();
            return JsonSerializer.Deserialize<TriggerActionResponse>(result, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            });
        }
        catch (HttpRequestException e)
        {
            throw;
        }
    }
}
