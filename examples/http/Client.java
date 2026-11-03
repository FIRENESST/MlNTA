// Java 11+: java Client.java; no MINTA jar or JSON dependency is needed.
import java.net.URI;
import java.net.http.*;
import java.time.Duration;

class Client {
    public static void main(String[] args) throws Exception {
        String endpoint = System.getenv().getOrDefault("MINTA_GATEWAY", "http://127.0.0.1:8080");
        HttpRequest request = HttpRequest.newBuilder(URI.create(endpoint + "/v1/call"))
            .timeout(Duration.ofSeconds(10)).header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString("{\"method\":\"POST\",\"target\":\"/echo\",\"body\":\"hello-java\"}")).build();
        HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200 || !response.body().contains("\"status\":200") || !response.body().contains("\"body\":\"hello-java\"")) throw new Exception(response.body());
        System.out.println("Java HTTP/JSON: all tests passed");
    }
}
