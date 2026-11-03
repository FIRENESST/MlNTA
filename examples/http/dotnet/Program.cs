using System.Net.Http.Json;
using System.Text.Json;

var endpoint = Environment.GetEnvironmentVariable("MINTA_GATEWAY") ?? "http://127.0.0.1:8080";
using var client = new HttpClient(new HttpClientHandler { UseProxy = false }) { Timeout = TimeSpan.FromSeconds(10) };
using var response = await client.PostAsJsonAsync(endpoint + "/v1/call", new { method = "POST", target = "/echo", json = new { hello = "dotnet" } });
response.EnsureSuccessStatusCode();
var reply = await response.Content.ReadFromJsonAsync<JsonElement>();
if (reply.GetProperty("status").GetInt32() != 200 || JsonDocument.Parse(reply.GetProperty("body").GetString()!).RootElement.GetProperty("hello").GetString() != "dotnet") throw new Exception(reply.ToString());
Console.WriteLine(".NET HTTP/JSON: all tests passed");
