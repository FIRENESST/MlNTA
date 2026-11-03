// go run client.go; uses only the Go standard library.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

func main() {
	endpoint := os.Getenv("MINTA_GATEWAY")
	if endpoint == "" {
		endpoint = "http://127.0.0.1:8080"
	}
	c := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Proxy: nil}}
	r, err := c.Post(endpoint+"/v1/call", "application/json", strings.NewReader(`{"method":"POST","target":"/echo","json":{"hello":"go"}}`))
	if err != nil {
		panic(err)
	}
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}
	var reply struct {
		Status int
		Body   string
	}
	if err := json.Unmarshal(body, &reply); err != nil {
		panic(err)
	}
	if r.StatusCode != 200 || reply.Status != 200 || reply.Body != `{"hello":"go"}` {
		panic(string(body))
	}
	fmt.Println("Go HTTP/JSON: all tests passed")
}
