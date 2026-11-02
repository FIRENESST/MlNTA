package beep

// Run against the Rust echo server:
//   BEEP_ECHO=127.0.0.1:10288 [BEEP_TLS_ECHO=… BEEP_CA=… BEEP_CERT=… BEEP_KEY=…] go test ./...

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func lines(n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "line %d\n", i)
	}
	return b.String()
}

func TestAgainstRustEcho(t *testing.T) {
	addr := os.Getenv("BEEP_ECHO")
	if addr == "" {
		t.Skip("BEEP_ECHO not set")
	}
	ctx := context.Background()
	c := NewClient(addr, &ClientOptions{MaxChannels: 8, Timeout: 5 * time.Second})
	defer c.Close()
	r, err := c.Call(ctx, "POST", "/echo?lang=go", Headers{{"Content-Type", "text/plain"}, {"X-Test", "go"}}, []byte("hello from go"))
	if err != nil || r.Status != 200 || string(r.Body) != "hello from go" || r.Headers.Get("x-test") != "go" || r.Headers.Get("X-Echo-Method") != "POST" {
		t.Fatalf("echo: %v %+v", err, r)
	}
	if r, err = c.Call(ctx, "GET", "/stream/50", nil, nil); err != nil || string(r.Body) != lines(50) {
		t.Errorf("stream: %v", err)
	}
	sr, err := c.Stream(ctx, "GET", "/stream/1000", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	first, _ := sr.Next(ctx)
	if !bytes.HasPrefix(first, []byte("line 0")) {
		t.Errorf("first chunk %q", first)
	}
	sr.Close()
	if r, err = c.Call(ctx, "GET", "/status/418", nil, nil); err != nil || r.Status != 418 || string(r.Body) != "status 418" {
		t.Errorf("status: %v %+v", err, r)
	}
	_, err = c.Call(ctx, "GET", "/err/553", nil, nil)
	var re *RemoteError
	if !errors.As(err, &re) || re.Code != 553 || re.HTTPStatus() != 400 {
		t.Errorf("err: %v", err)
	}
	if r, err = c.Call(ctx, "GET", "/bytes/3000000", nil, nil); err != nil || len(r.Body) != 3000000 || r.Body[0] != 'a' {
		t.Errorf("3MB: %v", err)
	}
	big := make([]byte, 2_500_000)
	_, _ = rand.Read(big)
	if r, err = c.Call(ctx, "POST", "/echo", nil, big); err != nil || !bytes.Equal(r.Body, big) {
		t.Errorf("2.5MB upload: %v", err)
	}
	start := time.Now()
	var wg sync.WaitGroup
	var failed sync.Map
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := c.Call(ctx, "POST", "/sleep/50", nil, []byte(strconv.Itoa(i)))
			if err != nil || r.Status != 200 {
				failed.Store(i, err)
			}
		}(i)
	}
	wg.Wait()
	failed.Range(func(k, v any) bool { t.Errorf("concurrent %v: %v", k, v); return true })
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("concurrency too slow: %v", d)
	}
	if st := c.Status(); len(st) != 1 || st[0] > 8 {
		t.Errorf("pool %v", st)
	}
	short := NewClient(addr, &ClientOptions{Timeout: 200 * time.Millisecond})
	defer short.Close()
	_, err = short.Call(ctx, "GET", "/sleep/2000", nil, nil)
	if !errors.Is(err, ErrTimeout) && !(errors.As(err, &re) && re.Code == 451) {
		t.Errorf("timeout: %v", err)
	}
	if r, err = short.Call(ctx, "POST", "/echo", nil, []byte("after-timeout")); err != nil || string(r.Body) != "after-timeout" {
		t.Errorf("after timeout: %v", err)
	}
}

func testHandler(ctx context.Context, req *Request) (*ServerReply, error) {
	p := req.Path()
	switch {
	case strings.HasPrefix(p, "/stream/"):
		n, _ := strconv.Atoi(p[8:])
		ch := make(chan []byte)
		go func() {
			defer close(ch)
			for i := 0; i < n; i++ {
				ch <- []byte(fmt.Sprintf("line %d\n", i))
			}
		}()
		return &ServerReply{Status: 200, Headers: Headers{{"Content-Type", "text/plain"}}, Stream: ch}, nil
	case p == "/abort":
		ch := make(chan []byte, 1)
		errc := make(chan error, 1)
		ch <- []byte("partial")
		close(ch)
		errc <- errors.New("producer died")
		return &ServerReply{Stream: ch, StreamErr: errc}, nil
	case strings.HasPrefix(p, "/err/"):
		code, _ := strconv.Atoi(p[5:])
		return nil, &Failure{code, "go says no"}
	}
	ct := req.Headers.Get("Content-Type")
	if ct == "" {
		ct = "application/octet-stream"
	}
	return &ServerReply{Status: 200, Headers: Headers{{"Content-Type", ct}, {"X-Lang", "go"}}, Body: req.Body}, nil
}

func TestGoServerGoClient(t *testing.T) {
	cfg := &Config{RecvWindow: 6000, MaxFrameSize: 1999}
	srv, err := NewServer("127.0.0.1:0", testHandler, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	defer srv.Close()
	ctx := context.Background()
	c := NewClient(srv.Addr().String(), &ClientOptions{Config: cfg})
	defer c.Close()
	data := make([]byte, 1_000_000)
	_, _ = rand.Read(data)
	r, err := c.Call(ctx, "POST", "/echo", nil, data)
	if err != nil || !bytes.Equal(r.Body, data) || r.Headers.Get("X-Lang") != "go" {
		t.Fatalf("big echo tiny windows: %v", err)
	}
	if r, err = c.Call(ctx, "GET", "/stream/20", nil, nil); err != nil || string(r.Body) != lines(20) {
		t.Errorf("stream: %v", err)
	}
	_, err = c.Call(ctx, "GET", "/abort", nil, nil)
	var ae *AbortedError
	if !errors.As(err, &ae) || !strings.Contains(ae.Reason, "producer died") {
		t.Errorf("abort: %v", err)
	}
	_, err = c.Call(ctx, "GET", "/err/537", nil, nil)
	var re *RemoteError
	if !errors.As(err, &re) || re.Code != 537 {
		t.Errorf("err: %v", err)
	}

	s, err := Connect(srv.Addr().String(), nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if pp := s.PeerProfiles(); len(pp) != 1 || pp[0] != RPCProfile {
		t.Errorf("greeting %v", pp)
	}
	_, err = s.StartChannel("urn:none")
	var rf *RefusedError
	if !errors.As(err, &rf) || rf.Code != 550 {
		t.Errorf("refused: %v", err)
	}
	ch, err := s.StartChannel(RPCProfile)
	if err != nil {
		t.Fatal(err)
	}
	var streams []*ReplyStream
	for i := 0; i < 10; i++ {
		st, err := ch.Send(EncodeEntity(Headers{{"Rpc-Method", "GET"}, {"Rpc-Target", "/echo"}}, []byte(strconv.Itoa(i))))
		if err != nil {
			t.Fatal(err)
		}
		streams = append(streams, st)
	}
	for i, st := range streams {
		rep, err := st.Collect(ctx, 1<<20)
		if err != nil || !bytes.HasSuffix(rep.Payload, []byte(strconv.Itoa(i))) {
			t.Errorf("pipelined %d: %v", i, err)
		}
	}
	if err := ch.Close(); err != nil {
		t.Error(err)
	}
	if err := s.Close(); err != nil || !s.Closed() {
		t.Errorf("close: %v", err)
	}
}

func TestNetHTTPAdapterAndTransport(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/hello/", func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		w.Header().Set("X-Go-Http", "yes")
		w.WriteHeader(201)
		fmt.Fprintf(w, "%s %s %s %s q=%s", r.Method, r.URL.Path, r.Host, b, r.URL.Query().Get("q"))
	})
	mux.HandleFunc("/sse", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for i := 0; i < 3; i++ {
			fmt.Fprintf(w, "data: %d\n\n", i)
			w.(http.Flusher).Flush()
			time.Sleep(20 * time.Millisecond)
		}
	})
	srv, err := NewServer("127.0.0.1:0", HTTPHandler(mux), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	defer srv.Close()
	bc := NewClient(srv.Addr().String(), nil)
	defer bc.Close()
	hc := &http.Client{Transport: NewTransport(bc)}
	resp, err := hc.Post("http://orders.internal/hello/x?q=7", "text/plain", strings.NewReader("body!"))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 201 || resp.Header.Get("X-Go-Http") != "yes" || string(b) != "POST /hello/x orders.internal body! q=7" {
		t.Errorf("adapter+transport: %d %q %v", resp.StatusCode, b, resp.Header)
	}
	resp, err = hc.Get("http://x/sse")
	if err != nil {
		t.Fatal(err)
	}
	b, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(b) != "data: 0\n\ndata: 1\n\ndata: 2\n\n" || resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Errorf("sse: %q", b)
	}
}

func TestTLS(t *testing.T) {
	tlsAddr, ca, cert, key := os.Getenv("BEEP_TLS_ECHO"), os.Getenv("BEEP_CA"), os.Getenv("BEEP_CERT"), os.Getenv("BEEP_KEY")
	if tlsAddr == "" {
		t.Skip("BEEP_TLS_ECHO not set")
	}
	ctx := context.Background()
	c := NewClient(tlsAddr, &ClientOptions{TLS: &TLSClient{ServerName: "localhost", CAFile: ca}})
	defer c.Close()
	r, err := c.Call(ctx, "POST", "/echo", nil, []byte("go over tls to rust"))
	if err != nil || string(r.Body) != "go over tls to rust" {
		t.Fatalf("go tls -> rust: %v", err)
	}
	srv, err := NewServer("127.0.0.1:0", testHandler, nil, &TLSServer{CertFile: cert, KeyFile: key})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	defer srv.Close()
	c2 := NewClient(srv.Addr().String(), &ClientOptions{TLS: &TLSClient{ServerName: "localhost", CAFile: ca}})
	defer c2.Close()
	if r, err = c2.Call(ctx, "POST", "/echo", nil, []byte("go tls both ends")); err != nil || string(r.Body) != "go tls both ends" {
		t.Fatalf("go tls -> go: %v", err)
	}
}
