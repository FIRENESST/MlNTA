package beep

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RPCProfile is the MINTA RPC profile URI.
const RPCProfile = "urn:minta:beep:rpc:1"

var reserved = map[string]bool{"rpc-method": true, "rpc-target": true, "rpc-status": true, "rpc-reason": true, "rpc-abort": true, "content-transfer-encoding": true}

func appHeaders(h Headers) Headers {
	out := Headers{}
	for _, x := range h {
		if !reserved[strings.ToLower(x.Name)] {
			out = append(out, x)
		}
	}
	return out
}

// RemoteError is a BEEP ERR answer: no application-level response exists.
type RemoteError struct {
	Code    int
	Message string
}

func (e *RemoteError) Error() string { return fmt.Sprintf("beep: remote error %d: %s", e.Code, e.Message) }

// HTTPStatus maps the BEEP code to an HTTP status (docs/PROTOCOL.md §2.4).
func (e *RemoteError) HTTPStatus() int { return BeepCodeToHTTP(e.Code) }

// AbortedError means a streamed response was aborted by the responder (Rpc-Abort).
type AbortedError struct{ Reason string }

func (e *AbortedError) Error() string { return "beep: response stream aborted: " + e.Reason }

// ErrTimeout is returned when a request deadline expires.
var ErrTimeout = errors.New("beep: request timed out")

// BeepCodeToHTTP maps RFC 3080 reply codes to HTTP statuses.
func BeepCodeToHTTP(code int) int {
	switch code {
	case 421, 450:
		return 503
	case 451:
		return 504
	case 500, 501, 553:
		return 400
	case 504:
		return 501
	case 530, 534, 535:
		return 401
	case 537, 538:
		return 403
	}
	return 502
}

// Response is a complete RPC response.
type Response struct {
	Status  int
	Headers Headers
	Body    []byte
}

// ---------------------------------------------------------------------------
// reply decoding

type ansBuf struct {
	head   []byte
	parsed bool
	data   [][]byte
	done   bool
}

type decoder struct {
	stream   *ReplyStream
	status   int
	headers  Headers
	haveHead bool
	finished bool
	ready    [][]byte
	rpyBuf   []byte
	rpyDone  bool
	ans      map[uint32]*ansBuf
	nextAns  uint32
}

func newDecoder(st *ReplyStream) *decoder { return &decoder{stream: st, ans: map[uint32]*ansBuf{}} }

func (d *decoder) setHead(h Headers) error {
	d.status = 200
	if v, ok := h.Lookup("Rpc-Status"); ok {
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return fmt.Errorf("beep: bad Rpc-Status %q", v)
		}
		d.status = n
	}
	d.headers = appHeaders(h)
	d.haveHead = true
	return nil
}

func (d *decoder) readHead(ctx context.Context) error {
	for !d.haveHead {
		if d.finished {
			return errors.New("beep: reply ended before a response head")
		}
		if err := d.pump(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (d *decoder) nextBody(ctx context.Context) ([]byte, error) {
	for {
		if len(d.ready) > 0 {
			b := d.ready[0]
			d.ready = d.ready[1:]
			return b, nil
		}
		if d.finished {
			return nil, nil
		}
		if err := d.pump(ctx); err != nil {
			return nil, err
		}
	}
}

func (d *decoder) pump(ctx context.Context) error {
	ev, err := d.stream.Next(ctx)
	if err != nil {
		return err
	}
	if ev == nil {
		d.finished = true
		return nil
	}
	switch ev.Type {
	case "RPY":
		if !d.rpyDone {
			d.rpyBuf = append(d.rpyBuf, ev.Data...)
			if h, off, ok := splitHead(d.rpyBuf, ev.Last); ok {
				d.rpyDone = true
				if err := d.setHead(h); err != nil {
					return err
				}
				if rest := d.rpyBuf[off:]; len(rest) > 0 {
					d.ready = append(d.ready, rest)
				}
				d.rpyBuf = nil
			} else if len(d.rpyBuf) > 65536 {
				return errors.New("beep: response head too large")
			}
		} else if len(ev.Data) > 0 {
			d.ready = append(d.ready, ev.Data)
		}
		if ev.Last {
			d.finished = true
		}
	case "ERR":
		buf := append([]byte(nil), ev.Data...)
		last := ev.Last
		for !last {
			n, err := d.stream.Next(ctx)
			if err != nil {
				return err
			}
			if n == nil || n.Type != "ERR" {
				return errors.New("beep: truncated ERR")
			}
			buf = append(buf, n.Data...)
			last = n.Last
		}
		d.finished = true
		code, msg := parseErrorPayload(buf)
		return &RemoteError{code, msg}
	case "ANS":
		a := d.ans[ev.Ansno]
		if a == nil {
			a = &ansBuf{}
			d.ans[ev.Ansno] = a
		}
		if !a.parsed {
			a.head = append(a.head, ev.Data...)
			if h, off, ok := splitHead(a.head, ev.Last); ok {
				a.parsed = true
				if reason, abort := h.Lookup("Rpc-Abort"); abort {
					return &AbortedError{reason}
				}
				if ev.Ansno == 0 {
					if err := d.setHead(h); err != nil {
						return err
					}
				}
				if rest := a.head[off:]; len(rest) > 0 {
					a.data = append(a.data, rest)
				}
				a.head = nil
			}
		} else if len(ev.Data) > 0 {
			a.data = append(a.data, ev.Data)
		}
		if ev.Last {
			a.done = true
		}
		d.flush(false)
	case "NUL":
		d.flush(true)
		d.finished = true
		if !d.haveHead {
			return errors.New("beep: ANS stream without a head (ansno 0)")
		}
	}
	return nil
}

func (d *decoder) flush(all bool) {
	for len(d.ans) > 0 {
		k := d.nextAns
		if all {
			keys := make([]int, 0, len(d.ans))
			for x := range d.ans {
				keys = append(keys, int(x))
			}
			sort.Ints(keys)
			k = uint32(keys[0])
		}
		a := d.ans[k]
		if a == nil || (k == 0 && !d.haveHead && !all) {
			return
		}
		d.ready = append(d.ready, a.data...)
		a.data = nil
		if a.done || all {
			delete(d.ans, k)
			d.nextAns = k + 1
		} else {
			return
		}
	}
}

// ---------------------------------------------------------------------------
// client

// ClientOptions configures an RPC client.
type ClientOptions struct {
	Sessions    int           // TCP connections (default 1)
	MaxChannels int           // channels per session (default 64)
	Timeout     time.Duration // response-head deadline, also sent as Rpc-Timeout (0 = 30s, <0 = none)
	TLS         *TLSClient
	Config      *Config
	Profile     string
}

// Client is a pooled RPC client: Sessions × MaxChannels channels, one request per channel.
type Client struct {
	addr     string
	opts     ClientOptions
	mu       sync.Mutex
	sessions []*Session
	idle     []*Channel
	permits  chan struct{}
	connMu   sync.Mutex
	closed   bool
}

// NewClient creates a client for addr ("host:port"). opts may be nil.
func NewClient(addr string, opts *ClientOptions) *Client {
	o := ClientOptions{}
	if opts != nil {
		o = *opts
	}
	if o.Sessions <= 0 {
		o.Sessions = 1
	}
	if o.MaxChannels <= 0 {
		o.MaxChannels = 64
	}
	if o.Timeout == 0 {
		o.Timeout = 30 * time.Second
	}
	if o.Profile == "" {
		o.Profile = RPCProfile
	}
	return &Client{addr: addr, opts: o, permits: make(chan struct{}, o.Sessions*o.MaxChannels)}
}

func (c *Client) leastLoadedLocked() *Session {
	best := c.sessions[0]
	for _, s := range c.sessions {
		if s.ChannelCount() < best.ChannelCount() {
			best = s
		}
	}
	return best
}

func (c *Client) session() (*Session, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, &ClosedError{"client closed"}
	}
	live := c.sessions[:0]
	for _, s := range c.sessions {
		if !s.Closed() {
			live = append(live, s)
		}
	}
	c.sessions = live
	if len(c.sessions) >= c.opts.Sessions {
		s := c.leastLoadedLocked()
		c.mu.Unlock()
		return s, nil
	}
	c.mu.Unlock()
	c.connMu.Lock()
	defer c.connMu.Unlock()
	c.mu.Lock()
	n := 0
	for _, s := range c.sessions {
		if !s.Closed() {
			n++
		}
	}
	if n >= c.opts.Sessions {
		s := c.leastLoadedLocked()
		c.mu.Unlock()
		return s, nil
	}
	c.mu.Unlock()
	s, err := Connect(c.addr, c.opts.Config, nil, c.opts.TLS)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.sessions = append(c.sessions, s)
	c.mu.Unlock()
	return s, nil
}

// Connect opens a session now (warm-up / health check).
func (c *Client) Connect() error {
	_, err := c.session()
	return err
}

func (c *Client) acquire() (*Channel, error) {
	c.mu.Lock()
	for len(c.idle) > 0 {
		ch := c.idle[len(c.idle)-1]
		c.idle = c.idle[:len(c.idle)-1]
		if ch.IsOpen() {
			c.mu.Unlock()
			return ch, nil
		}
	}
	c.mu.Unlock()
	s, err := c.session()
	if err != nil {
		return nil, err
	}
	ch, err := s.StartChannel(c.opts.Profile)
	if err != nil && s.Closed() {
		if s, err = c.session(); err != nil {
			return nil, err
		}
		return s.StartChannel(c.opts.Profile)
	}
	return ch, err
}

func (c *Client) giveBack(ch *Channel) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed && ch.IsOpen() && ch.Pending() == 0 {
		c.idle = append(c.idle, ch)
	}
}

func (c *Client) release(ch *Channel, st *ReplyStream) {
	if st.Done() {
		c.giveBack(ch)
		<-c.permits
		return
	}
	st.Discard()
	go func() {
		for {
			ev, err := st.Next(context.Background())
			if ev == nil || err != nil {
				break
			}
		}
		c.giveBack(ch)
		<-c.permits
	}()
}

// StreamingResponse is a response whose body is read incrementally. Always Close it.
type StreamingResponse struct {
	Status  int
	Headers Headers
	c       *Client
	ch      *Channel
	dec     *decoder
	once    sync.Once
}

// Next returns the next body chunk, or (nil, nil) at the end.
func (r *StreamingResponse) Next(ctx context.Context) ([]byte, error) {
	b, err := r.dec.nextBody(ctx)
	if b == nil || err != nil {
		r.Close()
	}
	return b, err
}

// ReadAll reads the remaining body.
func (r *StreamingResponse) ReadAll(ctx context.Context) ([]byte, error) {
	var out []byte
	for {
		b, err := r.Next(ctx)
		if err != nil {
			return nil, err
		}
		if b == nil {
			return out, nil
		}
		out = append(out, b...)
	}
}

// Close returns the channel to the pool (draining in the background if needed).
func (r *StreamingResponse) Close() { r.once.Do(func() { r.c.release(r.ch, r.dec.stream) }) }

// Stream sends a request and returns once the response head arrives.
func (c *Client) Stream(ctx context.Context, method, target string, headers Headers, body []byte) (*StreamingResponse, error) {
	h := appHeaders(headers)
	if c.opts.Timeout > 0 {
		if _, ok := h.Lookup("Rpc-Timeout"); !ok {
			h.Add("Rpc-Timeout", strconv.FormatInt(c.opts.Timeout.Milliseconds(), 10))
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.opts.Timeout)
		defer cancel()
	}
	payload := EncodeEntity(append(Headers{{"Rpc-Method", method}, {"Rpc-Target", target}}, h...), body)
	select {
	case c.permits <- struct{}{}:
	case <-ctx.Done():
		return nil, ErrTimeout
	}
	ch, err := c.acquire()
	if err != nil {
		<-c.permits
		return nil, err
	}
	st, err := ch.Send(payload)
	if err != nil {
		<-c.permits
		return nil, err
	}
	dec := newDecoder(st)
	if err := dec.readHead(ctx); err != nil {
		c.release(ch, st)
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, ErrTimeout
		}
		return nil, err
	}
	return &StreamingResponse{Status: dec.status, Headers: dec.headers, c: c, ch: ch, dec: dec}, nil
}

// Call sends a request and reads the whole response.
func (c *Client) Call(ctx context.Context, method, target string, headers Headers, body []byte) (*Response, error) {
	r, err := c.Stream(ctx, method, target, headers, body)
	if err != nil {
		return nil, err
	}
	data, err := r.ReadAll(ctx)
	if err != nil {
		return nil, err
	}
	return &Response{Status: r.Status, Headers: r.Headers, Body: data}, nil
}

// Raw sends a verbatim payload on a fresh channel of any profile.
func (c *Client) Raw(ctx context.Context, profile string, payload []byte) (*Reply, error) {
	s, err := c.session()
	if err != nil {
		return nil, err
	}
	ch, err := s.StartChannel(profile)
	if err != nil {
		return nil, err
	}
	defer ch.Close()
	return ch.Request(ctx, payload)
}

// Status returns open channel counts of live sessions.
func (c *Client) Status() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []int
	for _, s := range c.sessions {
		if !s.Closed() {
			out = append(out, s.ChannelCount())
		}
	}
	return out
}

// Close releases all sessions.
func (c *Client) Close() error {
	c.mu.Lock()
	c.closed = true
	c.idle = nil
	ss := c.sessions
	c.sessions = nil
	c.mu.Unlock()
	for _, s := range ss {
		if err := s.Close(); err != nil {
			s.Abort()
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// server

// Request is an incoming RPC request.
type Request struct {
	Method  string
	Target  string
	Headers Headers
	Body    []byte
	Channel uint32
}

// Path is Target without the query string.
func (r *Request) Path() string {
	if i := strings.IndexByte(r.Target, '?'); i >= 0 {
		return r.Target[:i]
	}
	return r.Target
}

// Timeout is the Rpc-Timeout hint (0 if absent).
func (r *Request) Timeout() time.Duration {
	ms, err := strconv.ParseInt(strings.TrimSpace(r.Headers.Get("Rpc-Timeout")), 10, 64)
	if err != nil || ms <= 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

// ServerReply is a handler result: Body → one RPY; Stream → ANS chunks until closed.
type ServerReply struct {
	Status  int
	Headers Headers
	Body    []byte
	Stream  <-chan []byte
	// StreamErr, if set, is read after Stream closes; a non-nil error aborts the stream (Rpc-Abort).
	StreamErr <-chan error
}

// Failure makes a handler answer with BEEP ERR.
type Failure struct {
	Code    int
	Message string
}

func (f *Failure) Error() string { return fmt.Sprintf("%d: %s", f.Code, f.Message) }

// Handler serves RPC requests. Return a *Failure error to answer with BEEP ERR.
type Handler func(ctx context.Context, req *Request) (*ServerReply, error)

// RPCProfileHandler adapts a Handler to a ProfileHandler.
func RPCProfileHandler(h Handler) ProfileHandler {
	return HandlerFunc(func(s *Session, msg *IncomingMessage, r *Responder) error {
		payload, err := msg.Collect(context.Background(), s.cfg.MaxMessageSize)
		if err != nil {
			if !s.Closed() {
				return r.Error(554, err.Error())
			}
			return err
		}
		hd, off, _ := splitHead(payload, true)
		method, okM := hd.Lookup("Rpc-Method")
		target, okT := hd.Lookup("Rpc-Target")
		if !okM || method == "" || !okT {
			return r.Error(501, "request lacks Rpc-Method or Rpc-Target")
		}
		req := &Request{Method: method, Target: target, Headers: appHeaders(hd), Body: payload[off:], Channel: msg.Channel}
		ctx := context.Background()
		if t := req.Timeout(); t > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, t)
			defer cancel()
		}
		type result struct {
			r   *ServerReply
			err error
		}
		done := make(chan result, 1)
		go func() {
			defer func() {
				if p := recover(); p != nil {
					done <- result{nil, fmt.Errorf("handler panic: %v", p)}
				}
			}()
			rep, err := h(ctx, req)
			done <- result{rep, err}
		}()
		var res result
		select {
		case res = <-done:
		case <-ctx.Done():
			return r.Error(451, "deadline exceeded")
		}
		if res.err != nil {
			var f *Failure
			if errors.As(res.err, &f) {
				return r.Error(f.Code, f.Message)
			}
			return r.Error(554, "handler failed: "+res.err.Error())
		}
		rep := res.r
		if rep == nil {
			rep = &ServerReply{Status: 204}
		}
		if rep.Status == 0 {
			rep.Status = 200
		}
		rh := Headers{{"Rpc-Status", strconv.Itoa(rep.Status)}}
		rh = append(rh, appHeaders(rep.Headers)...)
		if id, ok := req.Headers.Lookup("Rpc-Id"); ok {
			if _, has := rh.Lookup("Rpc-Id"); !has {
				rh.Add("Rpc-Id", id)
			}
		}
		if rep.Stream == nil {
			return r.RPY(EncodeEntity(rh, rep.Body))
		}
		if _, err := r.ANS(EncodeEntity(rh, nil)); err != nil {
			return err
		}
		for chunk := range rep.Stream {
			if len(chunk) == 0 {
				continue
			}
			if _, err := r.ANS(EncodeEntity(nil, chunk)); err != nil {
				return err
			}
		}
		if rep.StreamErr != nil {
			if err := <-rep.StreamErr; err != nil {
				if _, err := r.ANS(EncodeEntity(Headers{{"Rpc-Abort", err.Error()}}, nil)); err != nil {
					return err
				}
			}
		}
		return r.NUL()
	})
}

// Server is a BEEP listener serving the RPC profile.
type Server struct{ *Listener }

// NewServer binds addr and serves h. tls may be nil.
func NewServer(addr string, h Handler, cfg *Config, tls *TLSServer) (*Server, error) {
	l, err := Listen(addr, cfg, map[string]ProfileHandler{RPCProfile: RPCProfileHandler(h)}, tls)
	if err != nil {
		return nil, err
	}
	return &Server{l}, nil
}
