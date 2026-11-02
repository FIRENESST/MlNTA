package beep

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
)

var hopByHop = map[string]bool{"connection": true, "keep-alive": true, "proxy-connection": true, "transfer-encoding": true,
	"te": true, "trailer": true, "upgrade": true, "content-length": true, "rpc-id": true, "rpc-timeout": true, "rpc-authority": true}

// HTTPHandler serves any net/http Handler (Gin, Echo, Chi, gorilla/mux, http.ServeMux…) natively
// over BEEP. Responses that call Flush before finishing are streamed as ANS; others go as one RPY.
func HTTPHandler(h http.Handler) Handler {
	return func(ctx context.Context, req *Request) (*ServerReply, error) {
		hr, err := http.NewRequestWithContext(ctx, req.Method, req.Target, bytes.NewReader(req.Body))
		if err != nil {
			return nil, &Failure{553, "bad request target: " + err.Error()}
		}
		hr.RequestURI = req.Target
		hr.Host = req.Headers.Get("Rpc-Authority")
		if hr.Host == "" {
			hr.Host = "beep"
		}
		hr.RemoteAddr = "beep:0"
		hr.ContentLength = int64(len(req.Body))
		for _, x := range req.Headers {
			if !hopByHop[strings.ToLower(x.Name)] {
				hr.Header.Add(x.Name, x.Value)
			}
		}
		w := &beepResponseWriter{header: http.Header{}, headDone: make(chan struct{}), stream: make(chan []byte, 16)}
		finished := make(chan struct{})
		go func() {
			defer close(finished)
			defer func() {
				if p := recover(); p != nil {
					w.abort(fmt.Errorf("handler panic: %v", p))
				}
				w.finish()
			}()
			h.ServeHTTP(w, hr)
		}()
		select {
		case <-finished:
		case <-w.headDone: // Flush() before finishing → stream
		}
		return w.reply(finished), nil
	}
}

type beepResponseWriter struct {
	mu        sync.Mutex
	header    http.Header
	status    int
	buf       bytes.Buffer
	streaming bool
	headDone  chan struct{}
	headOnce  sync.Once
	stream    chan []byte
	errc      chan error
	abortErr  error
	finished  bool
}

func (w *beepResponseWriter) Header() http.Header { return w.header }

func (w *beepResponseWriter) WriteHeader(code int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.status == 0 {
		w.status = code
	}
}

func (w *beepResponseWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	if w.status == 0 {
		w.status = 200
	}
	if !w.streaming {
		n, err := w.buf.Write(p)
		w.mu.Unlock()
		return n, err
	}
	w.mu.Unlock()
	w.stream <- append([]byte(nil), p...)
	return len(p), nil
}

// Flush switches to streaming: what was buffered goes out now, later writes follow as ANS.
func (w *beepResponseWriter) Flush() {
	w.mu.Lock()
	if w.status == 0 {
		w.status = 200
	}
	first := !w.streaming
	w.streaming = true
	pending := append([]byte(nil), w.buf.Bytes()...)
	w.buf.Reset()
	w.mu.Unlock()
	if first {
		w.errc = make(chan error, 1)
		w.headOnce.Do(func() { close(w.headDone) })
	}
	if len(pending) > 0 {
		w.stream <- pending
	}
}

func (w *beepResponseWriter) abort(err error) {
	w.mu.Lock()
	w.abortErr = err
	w.mu.Unlock()
}

func (w *beepResponseWriter) finish() {
	w.mu.Lock()
	streaming := w.streaming
	err := w.abortErr
	w.finished = true
	w.mu.Unlock()
	if streaming {
		close(w.stream)
		w.errc <- err
	}
}

func (w *beepResponseWriter) headers() Headers {
	var h Headers
	for k, vs := range w.header {
		if hopByHop[strings.ToLower(k)] {
			continue
		}
		for _, v := range vs {
			h.Add(k, v)
		}
	}
	return h
}

func (w *beepResponseWriter) reply(finished chan struct{}) *ServerReply {
	w.mu.Lock()
	defer w.mu.Unlock()
	status := w.status
	if status == 0 {
		status = 200
	}
	if !w.streaming {
		if w.abortErr != nil {
			return &ServerReply{Status: 500, Headers: Headers{{"Content-Type", "text/plain"}}, Body: []byte(w.abortErr.Error())}
		}
		return &ServerReply{Status: status, Headers: w.headers(), Body: append([]byte(nil), w.buf.Bytes()...)}
	}
	return &ServerReply{Status: status, Headers: w.headers(), Stream: w.stream, StreamErr: w.errc}
}

// Transport is an http.RoundTripper that sends requests over BEEP, so any Go code using
// http.Client (including third-party SDKs) can talk to a BEEP backend unchanged:
//
//	hc := &http.Client{Transport: beep.NewTransport(beep.NewClient("10.0.0.5:10288", nil))}
//	resp, err := hc.Get("http://orders/api/v1/items")   // host is sent as Rpc-Authority
type Transport struct{ Client *Client }

// NewTransport wraps an RPC client.
func NewTransport(c *Client) *Transport { return &Transport{Client: c} }

// RoundTrip implements http.RoundTripper.
func (t *Transport) RoundTrip(r *http.Request) (*http.Response, error) {
	var body []byte
	if r.Body != nil {
		b, err := io.ReadAll(r.Body)
		_ = r.Body.Close()
		if err != nil {
			return nil, err
		}
		body = b
	}
	var h Headers
	for k, vs := range r.Header {
		if hopByHop[strings.ToLower(k)] {
			continue
		}
		for _, v := range vs {
			h.Add(k, v)
		}
	}
	if r.Host != "" {
		h.Add("Rpc-Authority", r.Host)
	} else if r.URL.Host != "" {
		h.Add("Rpc-Authority", r.URL.Host)
	}
	target := r.URL.RequestURI()
	sr, err := t.Client.Stream(r.Context(), r.Method, target, h, body)
	if err != nil {
		return nil, err
	}
	resp := &http.Response{
		Status:     fmt.Sprintf("%d %s", sr.Status, http.StatusText(sr.Status)),
		StatusCode: sr.Status,
		Proto:      "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header:        http.Header{},
		ContentLength: -1,
		Request:       r,
		Body:          &streamBody{ctx: r.Context(), sr: sr},
	}
	for _, x := range sr.Headers {
		resp.Header.Add(x.Name, x.Value)
	}
	return resp, nil
}

type streamBody struct {
	ctx  context.Context
	sr   *StreamingResponse
	cur  []byte
	done bool
}

func (b *streamBody) Read(p []byte) (int, error) {
	for len(b.cur) == 0 {
		if b.done {
			return 0, io.EOF
		}
		c, err := b.sr.Next(b.ctx)
		if err != nil {
			return 0, err
		}
		if c == nil {
			b.done = true
			return 0, io.EOF
		}
		b.cur = c
	}
	n := copy(p, b.cur)
	b.cur = b.cur[n:]
	return n, nil
}

func (b *streamBody) Close() error {
	b.sr.Close()
	return nil
}
