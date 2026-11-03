package beep

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	initialWindow = 4096
	maxChannel    = 2147483647
)

// Config tunes a session (docs/PROTOCOL.md §1.8).
type Config struct {
	RecvWindow      int
	MaxFrameSize    int
	MaxMessageSize  int
	ConnectTimeout  time.Duration
	GreetingTimeout time.Duration
	CloseTimeout    time.Duration
}

// DefaultConfig returns the protocol defaults.
func DefaultConfig() Config {
	return Config{
		RecvWindow:      262144,
		MaxFrameSize:    65536,
		MaxMessageSize:  64 << 20,
		ConnectTimeout:  5 * time.Second,
		GreetingTimeout: 10 * time.Second,
		CloseTimeout:    10 * time.Second,
	}
}

func (c *Config) normalize() {
	d := DefaultConfig()
	if c.RecvWindow <= 0 {
		c.RecvWindow = d.RecvWindow
	}
	if c.MaxFrameSize <= 0 {
		c.MaxFrameSize = d.MaxFrameSize
	}
	if c.MaxMessageSize <= 0 {
		c.MaxMessageSize = d.MaxMessageSize
	}
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = d.ConnectTimeout
	}
	if c.GreetingTimeout <= 0 {
		c.GreetingTimeout = d.GreetingTimeout
	}
	if c.CloseTimeout <= 0 {
		c.CloseTimeout = d.CloseTimeout
	}
}

// ClosedError reports that the session is gone.
type ClosedError struct{ Reason string }

func (e *ClosedError) Error() string { return "beep: session closed: " + e.Reason }

// RefusedError is a channel-management refusal (<error> reply to start/close).
type RefusedError struct {
	Code    int
	Message string
}

func (e *RefusedError) Error() string { return fmt.Sprintf("beep: refused by peer (%d): %s", e.Code, e.Message) }

// ---------------------------------------------------------------------------
// unbounded single-consumer queue

type queue[T any] struct {
	mu     sync.Mutex
	items  []T
	signal chan struct{}
}

func newQueue[T any]() *queue[T] { return &queue[T]{signal: make(chan struct{}, 1)} }

func (q *queue[T]) push(x T) {
	q.mu.Lock()
	q.items = append(q.items, x)
	q.mu.Unlock()
	select {
	case q.signal <- struct{}{}:
	default:
	}
}

func (q *queue[T]) tryPop() (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var zero T
	if len(q.items) == 0 {
		return zero, false
	}
	x := q.items[0]
	q.items[0] = zero
	q.items = q.items[1:]
	if len(q.items) == 0 {
		q.items = nil
	}
	return x, true
}

func (q *queue[T]) pop(ctx context.Context) (T, error) {
	for {
		if x, ok := q.tryPop(); ok {
			return x, nil
		}
		select {
		case <-q.signal:
		case <-ctx.Done():
			var zero T
			return zero, ctx.Err()
		}
	}
}

// ---------------------------------------------------------------------------
// public message types

type item struct {
	kind  string // MSG, END, CLOSED, RPY, ERR, ANS, NUL
	ansno uint32
	data  []byte
	last  bool
	n     int
}

type sink struct {
	s          *Session
	ch         uint32
	q          *queue[item]
	discarding atomic.Bool
	ansOpen    map[uint32]bool
}

func newSink(s *Session, ch uint32) *sink {
	return &sink{s: s, ch: ch, q: newQueue[item](), ansOpen: map[uint32]bool{}}
}

func (k *sink) deliver(it item) {
	if k.discarding.Load() {
		k.s.credit(k.ch, it.n)
		return
	}
	k.q.push(it)
}

func (k *sink) discard() {
	k.discarding.Store(true)
	for {
		it, ok := k.q.tryPop()
		if !ok {
			return
		}
		k.s.credit(k.ch, it.n)
	}
}

// ReplyEvent is one piece of a reply: Type is RPY, ERR, ANS or NUL.
type ReplyEvent struct {
	Type  string
	Ansno uint32
	Data  []byte
	Last  bool
}

// Answer is one complete ANS message.
type Answer struct {
	Ansno   uint32
	Payload []byte
}

// Reply is a complete reply: Kind is RPY, ERR or ANS.
type Reply struct {
	Kind    string
	Payload []byte
	Answers []Answer
}

// ReplyStream yields the reply events for one MSG; data is acknowledged as consumed.
type ReplyStream struct {
	s    *Session
	sink *sink
	done bool
}

// Done reports whether the terminal event was seen.
func (r *ReplyStream) Done() bool { return r.done }

// Next returns the next event, or (nil, nil) after the terminal one.
func (r *ReplyStream) Next(ctx context.Context) (*ReplyEvent, error) {
	if r.done {
		return nil, nil
	}
	it, err := r.sink.q.pop(ctx)
	if err != nil {
		return nil, err
	}
	r.s.credit(r.sink.ch, it.n)
	if it.kind == "CLOSED" {
		r.done = true
		return nil, r.s.closedError()
	}
	if it.kind == "NUL" || ((it.kind == "RPY" || it.kind == "ERR") && it.last) {
		r.done = true
	}
	return &ReplyEvent{Type: it.kind, Ansno: it.ansno, Data: it.data, Last: it.last}, nil
}

// Collect reads the whole reply.
func (r *ReplyStream) Collect(ctx context.Context, max int) (*Reply, error) {
	var buf []byte
	ans := map[uint32][]byte{}
	kind := "RPY"
	total := 0
	for {
		ev, err := r.Next(ctx)
		if err != nil {
			return nil, err
		}
		if ev == nil {
			break
		}
		switch ev.Type {
		case "RPY", "ERR":
			kind = ev.Type
			buf = append(buf, ev.Data...)
		case "ANS":
			kind = "ANS"
			ans[ev.Ansno] = append(ans[ev.Ansno], ev.Data...)
		case "NUL":
			kind = "ANS"
		}
		total += len(ev.Data)
		if total > max {
			r.Discard()
			return nil, fmt.Errorf("beep: reply exceeds %d bytes", max)
		}
	}
	if kind == "ANS" {
		keys := make([]int, 0, len(ans))
		for k := range ans {
			keys = append(keys, int(k))
		}
		sort.Ints(keys)
		out := &Reply{Kind: "ANS"}
		for _, k := range keys {
			out.Answers = append(out.Answers, Answer{uint32(k), ans[uint32(k)]})
		}
		return out, nil
	}
	return &Reply{Kind: kind, Payload: buf}, nil
}

// Discard stops consuming; remaining data is acknowledged as it arrives.
func (r *ReplyStream) Discard() { r.sink.discard() }

// IncomingMessage is a MSG received from the peer.
type IncomingMessage struct {
	Channel uint32
	Msgno   uint32
	s       *Session
	sink    *sink
	done    bool
}

// Read returns the next body chunk, or (nil, nil) at the end.
func (m *IncomingMessage) Read(ctx context.Context) ([]byte, error) {
	if m.done {
		return nil, nil
	}
	it, err := m.sink.q.pop(ctx)
	if err != nil {
		return nil, err
	}
	m.s.credit(m.Channel, it.n)
	switch it.kind {
	case "END":
		m.done = true
		return nil, nil
	case "CLOSED":
		m.done = true
		return nil, m.s.closedError()
	}
	return it.data, nil
}

// Collect reads the whole body.
func (m *IncomingMessage) Collect(ctx context.Context, max int) ([]byte, error) {
	var buf []byte
	for {
		c, err := m.Read(ctx)
		if err != nil {
			return nil, err
		}
		if c == nil {
			return buf, nil
		}
		buf = append(buf, c...)
		if len(buf) > max {
			m.Discard()
			return nil, fmt.Errorf("beep: message exceeds %d bytes", max)
		}
	}
}

// Discard stops reading; remaining data is acknowledged as it arrives.
func (m *IncomingMessage) Discard() {
	m.done = true
	m.sink.discard()
}

const (
	stIdle = iota
	stAnswering
	stDone
)

// Responder sends the reply to one incoming MSG: RPY, ERR, or ANS…NUL.
type Responder struct {
	s       *Session
	Channel uint32
	Msgno   uint32
	mu      sync.Mutex
	state   int
	nextAns uint32
}

func (r *Responder) final(kind string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != stIdle {
		return fmt.Errorf("beep: %s not allowed after ANS or completion", kind)
	}
	r.state = stDone
	return nil
}

func (r *Responder) getState() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state
}

// RPY sends a positive reply.
func (r *Responder) RPY(payload []byte) error {
	if err := r.final("RPY"); err != nil {
		return err
	}
	defer r.s.finishInbound(r.Channel, r.Msgno)
	return r.s.sendMessage(r.Channel, "RPY", r.Msgno, 0, payload)
}

// ERR sends a negative reply.
func (r *Responder) ERR(payload []byte) error {
	if err := r.final("ERR"); err != nil {
		return err
	}
	defer r.s.finishInbound(r.Channel, r.Msgno)
	return r.s.sendMessage(r.Channel, "ERR", r.Msgno, 0, payload)
}

// Error sends ERR with a standard <error code='…'> element.
func (r *Responder) Error(code int, message string) error {
	return r.ERR(errorPayload(code, message))
}

// ANS sends one answer of a one-to-many reply and returns its ansno.
func (r *Responder) ANS(payload []byte) (uint32, error) {
	r.mu.Lock()
	if r.state == stDone {
		r.mu.Unlock()
		return 0, errors.New("beep: reply already completed")
	}
	r.state = stAnswering
	n := r.nextAns
	r.nextAns++
	r.mu.Unlock()
	return n, r.s.sendMessage(r.Channel, "ANS", r.Msgno, n, payload)
}

// NUL terminates a one-to-many reply.
func (r *Responder) NUL() error {
	r.mu.Lock()
	if r.state == stDone {
		r.mu.Unlock()
		return errors.New("beep: reply already completed")
	}
	r.state = stDone
	r.mu.Unlock()
	defer r.s.finishInbound(r.Channel, r.Msgno)
	return r.s.sendMessage(r.Channel, "NUL", r.Msgno, 0, nil)
}

// ProfileHandler serves MSGs on channels of one profile; MSGs on a channel arrive in order.
// The reply should be complete when OnMessage returns.
type ProfileHandler interface {
	OnMessage(s *Session, msg *IncomingMessage, r *Responder) error
}

// StartHandler may be implemented by a ProfileHandler to vet channel starts.
// Return init-reply data, or a *RefusedError to refuse.
type StartHandler interface {
	OnStart(s *Session, channel uint32, init string) (string, error)
}

// HandlerFunc adapts a function to ProfileHandler.
type HandlerFunc func(s *Session, msg *IncomingMessage, r *Responder) error

func (f HandlerFunc) OnMessage(s *Session, msg *IncomingMessage, r *Responder) error {
	return f(s, msg, r)
}

// ---------------------------------------------------------------------------
// session

type chanState struct {
	number  uint32
	profile string

	mu      sync.Mutex // guards tx window fields
	cond    *sync.Cond
	txSeq   uint32
	txLimit uint32
	dead    bool
	sendMu  sync.Mutex // one outgoing message at a time

	// guarded by Session.mu
	recvSeq, advAck, advWindow, consumed uint32
	nextMsgno                            uint32
	outstanding                          map[uint32]*sink
	rxReplyType                          string
	rxReplyMsgno                         uint32
	rxMsg                                *IncomingMessage
	inbound                              map[uint32]bool
	queue                                *queue[*IncomingMessage]
}

func newChan(number uint32, profile string, nextMsgno uint32) *chanState {
	c := &chanState{number: number, profile: profile, txLimit: initialWindow, advWindow: initialWindow, nextMsgno: nextMsgno,
		outstanding: map[uint32]*sink{}, inbound: map[uint32]bool{}}
	c.cond = sync.NewCond(&c.mu)
	return c
}

func (c *chanState) busy() bool { return len(c.outstanding) > 0 || len(c.inbound) > 0 || c.rxMsg != nil }

func (c *chanState) kill() {
	c.mu.Lock()
	c.dead = true
	c.cond.Broadcast()
	c.mu.Unlock()
}

// Channel is an open BEEP channel.
type Channel struct {
	Session *Session
	Number  uint32
	Profile string
}

// Send sends a MSG and returns the reply stream (transmission continues in the background).
func (c *Channel) Send(payload []byte) (*ReplyStream, error) { return c.Session.sendMsg(c.Number, payload) }

// Request sends a MSG and collects the complete reply.
func (c *Channel) Request(ctx context.Context, payload []byte) (*Reply, error) {
	st, err := c.Send(payload)
	if err != nil {
		return nil, err
	}
	return st.Collect(ctx, c.Session.cfg.MaxMessageSize)
}

// Close closes the channel.
func (c *Channel) Close() error { return c.Session.closeChannel(c.Number, 200) }

// IsOpen reports whether the channel is still open.
func (c *Channel) IsOpen() bool {
	c.Session.mu.Lock()
	defer c.Session.mu.Unlock()
	_, ok := c.Session.chans[c.Number]
	return ok && c.Session.closed == nil
}

// Pending is the number of MSGs awaiting a complete reply.
func (c *Channel) Pending() int {
	c.Session.mu.Lock()
	defer c.Session.mu.Unlock()
	if ch, ok := c.Session.chans[c.Number]; ok {
		return len(ch.outstanding)
	}
	return 0
}

// Session is a BEEP session. Safe for concurrent use.
type Session struct {
	cfg       Config
	profiles  map[string]ProfileHandler
	initiator bool
	conn      net.Conn
	br        *bufio.Reader
	out       *queue[[]byte]

	mu        sync.Mutex
	chans     map[uint32]*chanState
	nextChan  uint32
	closed    error
	closedCh  chan struct{}
	releasing atomic.Bool
	peer      []string
}

// Connect dials a BEEP peer as initiator. tls may be nil.
func Connect(addr string, cfg *Config, profiles map[string]ProfileHandler, tls *TLSClient) (*Session, error) {
	c := DefaultConfig()
	if cfg != nil {
		c = *cfg
	}
	c.normalize()
	conn, err := net.DialTimeout("tcp", addr, c.ConnectTimeout)
	if err != nil {
		return nil, &ClosedError{"connect " + addr + ": " + err.Error()}
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	if tls != nil {
		conn, err = tlsClientBootstrap(conn, tls)
		if err != nil {
			return nil, err
		}
	}
	return Establish(conn, true, &c, profiles)
}

// Establish runs BEEP over an existing connection; returns after greetings.
func Establish(conn net.Conn, initiator bool, cfg *Config, profiles map[string]ProfileHandler) (*Session, error) {
	c := DefaultConfig()
	if cfg != nil {
		c = *cfg
	}
	c.normalize()
	s := &Session{cfg: c, profiles: map[string]ProfileHandler{}, initiator: initiator, conn: conn,
		br: bufio.NewReaderSize(conn, 65536), out: newQueue[[]byte](), chans: map[uint32]*chanState{},
		closedCh: make(chan struct{})}
	for k, v := range profiles {
		s.profiles[k] = v
	}
	if initiator {
		s.nextChan = 1
	} else {
		s.nextChan = 2
	}
	c0 := newChan(0, "", 1)
	greet := newSink(s, 0)
	c0.outstanding[0] = greet
	c0.queue = newQueue[*IncomingMessage]()
	s.chans[0] = c0
	go s.writer()
	go s.reader()

	g := &mgmt{kind: "greeting"}
	uris := make([]string, 0, len(s.profiles))
	for k := range s.profiles {
		uris = append(uris, k)
	}
	sort.Strings(uris)
	for _, u := range uris {
		g.profiles = append(g.profiles, profileEl{uri: u})
	}
	if err := s.sendMessage(0, "RPY", 0, 0, xmlEntity(g.xml())); err != nil {
		return nil, err
	}
	s.openWindow(0)
	ctx, cancel := context.WithTimeout(context.Background(), c.GreetingTimeout)
	defer cancel()
	reply, err := (&ReplyStream{s: s, sink: greet}).Collect(ctx, 1<<20)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			err = &ClosedError{"greeting timed out"}
		}
		s.fail(err)
		return nil, err
	}
	_, body := ParseEntity(reply.Payload)
	m, err := parseMgmt(body)
	if err != nil {
		s.fail(err)
		return nil, err
	}
	if reply.Kind == "ERR" || m.kind == "error" {
		e := &RefusedError{421, "peer refused session"}
		if m.kind == "error" {
			e = &RefusedError{m.code, m.message}
		}
		s.fail(e)
		return nil, e
	}
	if m.kind != "greeting" {
		e := perr("expected greeting, got %s", m.kind)
		s.fail(e)
		return nil, e
	}
	for _, p := range m.profiles {
		s.peer = append(s.peer, p.uri)
	}
	go s.mgmtLoop(c0.queue)
	return s, nil
}

// PeerProfiles returns the profiles the peer advertised.
func (s *Session) PeerProfiles() []string { return append([]string(nil), s.peer...) }

// Config returns the session configuration.
func (s *Session) Config() Config { return s.cfg }

// Closed reports whether the session has ended.
func (s *Session) Closed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed != nil
}

// Done is closed when the session ends; Err then returns the reason.
func (s *Session) Done() <-chan struct{} { return s.closedCh }

// Err returns why the session ended (nil while open).
func (s *Session) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// ChannelCount is the number of open channels excluding channel 0.
func (s *Session) ChannelCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.chans) - 1
}

func nextNumber(n uint32) uint32 {
	if n > maxChannel-2 {
		return 2 - n%2
	}
	return n + 2
}

// StartChannel starts a channel with one profile.
func (s *Session) StartChannel(uri string) (*Channel, error) {
	ch, _, err := s.StartChannelWith(uri, "", false, "")
	return ch, err
}

// StartChannelWith starts a channel with optional init data; returns the peer's init reply.
func (s *Session) StartChannelWith(uri, init string, hasInit bool, serverName string) (*Channel, string, error) {
	s.mu.Lock()
	if s.closed != nil {
		err := s.closed
		s.mu.Unlock()
		return nil, "", err
	}
	n := s.nextChan
	for tries := 0; ; tries++ {
		if _, used := s.chans[n]; !used {
			break
		}
		if tries > 1_000_000 {
			s.mu.Unlock()
			return nil, "", &ClosedError{"no free channel numbers"}
		}
		n = nextNumber(n)
	}
	s.nextChan = nextNumber(n)
	s.chans[n] = newChan(n, "", 0)
	s.mu.Unlock()
	remove := func() {
		s.mu.Lock()
		delete(s.chans, n)
		s.mu.Unlock()
	}
	m, err := s.mgmtRequest(context.Background(), &mgmt{kind: "start", number: n, serverName: serverName,
		profiles: []profileEl{{uri: uri, data: init, has: hasInit}}})
	if err != nil {
		remove()
		return nil, "", err
	}
	if m.kind == "error" {
		remove()
		return nil, "", &RefusedError{m.code, m.message}
	}
	if m.kind != "profile" {
		remove()
		return nil, "", perr("unexpected reply to <start>: %s", m.kind)
	}
	p := m.profiles[0]
	q := newQueue[*IncomingMessage]()
	s.mu.Lock()
	c, ok := s.chans[n]
	if !ok {
		err := s.closedErrorLocked()
		s.mu.Unlock()
		return nil, "", err
	}
	c.profile = p.uri
	c.queue = q
	s.mu.Unlock()
	go s.channelLoop(n, s.profiles[p.uri], q)
	s.openWindow(n)
	return &Channel{Session: s, Number: n, Profile: p.uri}, p.data, nil
}

// Close gracefully releases the session.
func (s *Session) Close() error {
	if s.Closed() {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.CloseTimeout)
	defer cancel()
	m, err := s.mgmtRequest(ctx, &mgmt{kind: "close", number: 0, code: 200})
	if err != nil {
		s.Abort()
		return err
	}
	switch m.kind {
	case "ok":
		s.fail(&ClosedError{"session released"})
		return nil
	case "error":
		return &RefusedError{m.code, m.message}
	}
	return perr("unexpected reply to <close>: %s", m.kind)
}

// Abort drops the connection immediately.
func (s *Session) Abort() { s.fail(&ClosedError{"session aborted"}) }

func (s *Session) closedErrorLocked() error {
	if s.closed != nil {
		return s.closed
	}
	return &ClosedError{"channel closed"}
}

func (s *Session) closedError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closedErrorLocked()
}

func (s *Session) fail(err error) {
	s.mu.Lock()
	if s.closed != nil {
		s.mu.Unlock()
		return
	}
	s.closed = err
	old := s.chans
	s.chans = map[uint32]*chanState{}
	s.mu.Unlock()
	for _, c := range old {
		c.kill()
		for _, k := range c.outstanding {
			k.q.push(item{kind: "CLOSED"})
		}
		if c.rxMsg != nil {
			c.rxMsg.sink.q.push(item{kind: "CLOSED"})
		}
		if c.queue != nil {
			c.queue.push(nil)
		}
	}
	s.out.push(nil)
	close(s.closedCh)
}

func (s *Session) credit(channel uint32, n int) {
	if n <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.chans[channel]
	if !ok {
		return
	}
	c.consumed += uint32(n)
	w := uint32(s.cfg.RecvWindow)
	if c.consumed-c.advAck >= w/2 {
		c.advAck = c.consumed
		c.advWindow = w
		s.out.push(encodeSeq(channel, c.consumed, w))
	}
}

func (s *Session) openWindow(channel uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.chans[channel]
	if !ok {
		return
	}
	c.advAck = c.consumed
	c.advWindow = uint32(s.cfg.RecvWindow)
	s.out.push(encodeSeq(channel, c.consumed, c.advWindow))
}

func (s *Session) finishInbound(channel, msgno uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.chans[channel]; ok {
		delete(c.inbound, msgno)
	}
}

func (s *Session) writer() {
	bw := bufio.NewWriterSize(s.conn, 65536)
	defer func() {
		_ = bw.Flush()
		if tc, ok := s.conn.(interface{ CloseWrite() error }); ok {
			_ = tc.CloseWrite()
		}
		time.AfterFunc(time.Second, func() { _ = s.conn.Close() })
	}()
	for {
		b, err := s.out.pop(context.Background())
		if err != nil || b == nil {
			return
		}
		if _, err := bw.Write(b); err != nil {
			s.fail(&ClosedError{"write failed: " + err.Error()})
			return
		}
		for {
			b, ok := s.out.tryPop()
			if !ok {
				break
			}
			if b == nil {
				return
			}
			if _, err := bw.Write(b); err != nil {
				s.fail(&ClosedError{"write failed: " + err.Error()})
				return
			}
		}
		if err := bw.Flush(); err != nil {
			s.fail(&ClosedError{"write failed: " + err.Error()})
			return
		}
	}
}

func (s *Session) reader() {
	limit := s.cfg.RecvWindow
	if limit < initialWindow {
		limit = initialWindow
	}
	for {
		f, err := readFrame(s.br, limit)
		if err != nil {
			var pe *ProtocolError
			switch {
			case errors.As(err, &pe):
				log.Printf("beep: %v; dropping connection", err)
				s.fail(err)
				_ = s.conn.Close()
			case errors.Is(err, io.EOF):
				reason := "connection closed by peer"
				if s.releasing.Load() {
					reason = "session released"
				}
				s.fail(&ClosedError{reason})
			default:
				s.fail(&ClosedError{"read failed: " + err.Error()})
			}
			return
		}
		if f.typ == "SEQ" {
			s.mu.Lock()
			c := s.chans[f.channel]
			s.mu.Unlock()
			if c != nil {
				c.mu.Lock()
				c.txLimit = f.ackno + f.window
				c.cond.Broadcast()
				c.mu.Unlock()
			}
			continue
		}
		if err := s.onData(f); err != nil {
			log.Printf("beep: %v; dropping connection", err)
			s.fail(err)
			_ = s.conn.Close()
			return
		}
	}
}

type delivery struct {
	k  *sink
	it item
	q  *queue[*IncomingMessage]
	m  *IncomingMessage
}

func (s *Session) onData(f *frame) error {
	var ds []delivery
	err := func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		c, ok := s.chans[f.channel]
		if !ok {
			return perr("frame on unknown channel %d", f.channel)
		}
		if f.seqno != c.recvSeq {
			return perr("channel %d: expected seqno %d, got %d", f.channel, c.recvSeq, f.seqno)
		}
		size := uint32(len(f.payload))
		end := c.recvSeq + size
		if end-c.advAck > c.advWindow {
			return perr("channel %d: frame exceeds receive window", f.channel)
		}
		c.recvSeq = end
		last := !f.more
		switch f.typ {
		case "MSG":
			m := c.rxMsg
			if m != nil && m.Msgno != f.msgno {
				return perr("channel %d: interleaved MSG frames", f.channel)
			}
			if m == nil {
				if c.inbound[f.msgno] {
					return perr("channel %d: msgno %d reused while outstanding", f.channel, f.msgno)
				}
				c.inbound[f.msgno] = true
				m = &IncomingMessage{Channel: f.channel, Msgno: f.msgno, s: s, sink: newSink(s, f.channel)}
				c.rxMsg = m
				if c.queue != nil {
					ds = append(ds, delivery{q: c.queue, m: m})
				}
			}
			ds = append(ds, delivery{k: m.sink, it: item{kind: "MSG", data: f.payload, n: int(size)}})
			if last {
				c.rxMsg = nil
				ds = append(ds, delivery{k: m.sink, it: item{kind: "END"}})
			}
		case "RPY", "ERR":
			if c.rxReplyType != "" && (c.rxReplyType != f.typ || c.rxReplyMsgno != f.msgno) {
				return perr("channel %d: interleaved reply frames", f.channel)
			}
			k, ok := c.outstanding[f.msgno]
			if !ok {
				return perr("channel %d: reply to unknown msgno %d", f.channel, f.msgno)
			}
			if len(k.ansOpen) > 0 {
				return perr("RPY/ERR while ANS in progress")
			}
			ds = append(ds, delivery{k: k, it: item{kind: f.typ, data: f.payload, last: last, n: int(size)}})
			if last {
				c.rxReplyType = ""
				delete(c.outstanding, f.msgno)
			} else {
				c.rxReplyType, c.rxReplyMsgno = f.typ, f.msgno
			}
		case "ANS":
			if c.rxReplyType != "" {
				return perr("ANS while RPY/ERR in progress")
			}
			k, ok := c.outstanding[f.msgno]
			if !ok {
				return perr("channel %d: ANS to unknown msgno %d", f.channel, f.msgno)
			}
			if last {
				delete(k.ansOpen, f.ansno)
			} else {
				k.ansOpen[f.ansno] = true
			}
			ds = append(ds, delivery{k: k, it: item{kind: "ANS", ansno: f.ansno, data: f.payload, last: last, n: int(size)}})
		case "NUL":
			k, ok := c.outstanding[f.msgno]
			if !ok {
				return perr("channel %d: NUL to unknown msgno %d", f.channel, f.msgno)
			}
			if len(k.ansOpen) > 0 {
				return perr("NUL before all ANS messages completed")
			}
			delete(c.outstanding, f.msgno)
			ds = append(ds, delivery{k: k, it: item{kind: "NUL", last: true}})
		}
		return nil
	}()
	// Deliver outside the lock: a discarding sink credits the window, which takes s.mu.
	for _, d := range ds {
		if d.q != nil {
			d.q.push(d.m)
		} else {
			d.k.deliver(d.it)
		}
	}
	return err
}

func (s *Session) sendMessage(channel uint32, typ string, msgno, ansno uint32, payload []byte) error {
	s.mu.Lock()
	if s.closed != nil {
		err := s.closed
		s.mu.Unlock()
		return err
	}
	c, ok := s.chans[channel]
	s.mu.Unlock()
	if !ok {
		return &ClosedError{fmt.Sprintf("channel %d is not open", channel)}
	}
	maxFrame := s.cfg.MaxFrameSize
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	off := 0
	for {
		if c.dead {
			return s.closedError()
		}
		avail := c.txLimit - c.txSeq
		if avail >= 1<<31 {
			avail = 0
		}
		remaining := len(payload) - off
		if remaining == 0 || avail > 0 {
			n := remaining
			if int(avail) < n {
				n = int(avail)
			}
			if maxFrame < n {
				n = maxFrame
			}
			more := off+n < len(payload)
			s.out.push(encodeFrame(typ, channel, msgno, more, c.txSeq, payload[off:off+n], ansno))
			c.txSeq += uint32(n)
			off += n
			if !more {
				return nil
			}
			continue
		}
		c.cond.Wait()
	}
}

func (s *Session) sendMsg(channel uint32, payload []byte) (*ReplyStream, error) {
	k := newSink(s, channel)
	s.mu.Lock()
	if s.closed != nil {
		err := s.closed
		s.mu.Unlock()
		return nil, err
	}
	c, ok := s.chans[channel]
	if !ok {
		s.mu.Unlock()
		return nil, &ClosedError{fmt.Sprintf("channel %d is not open", channel)}
	}
	n := c.nextMsgno
	for c.outstanding[n] != nil {
		if n >= maxChannel {
			n = 0
		} else {
			n++
		}
	}
	if n >= maxChannel {
		c.nextMsgno = 0
	} else {
		c.nextMsgno = n + 1
	}
	c.outstanding[n] = k
	s.mu.Unlock()
	// Transmit in the background so callers can pipeline big MSGs while reading replies.
	go func() {
		if err := s.sendMessage(channel, "MSG", n, 0, payload); err != nil {
			s.mu.Lock()
			removed := false
			if ch, ok := s.chans[channel]; ok && ch.outstanding[n] != nil {
				delete(ch.outstanding, n)
				removed = true
			}
			s.mu.Unlock()
			if removed {
				k.q.push(item{kind: "CLOSED"})
			}
		}
	}()
	return &ReplyStream{s: s, sink: k}, nil
}

func (s *Session) mgmtRequest(ctx context.Context, m *mgmt) (*mgmt, error) {
	st, err := s.sendMsg(0, xmlEntity(m.xml()))
	if err != nil {
		return nil, err
	}
	r, err := st.Collect(ctx, 1<<20)
	if err != nil {
		return nil, err
	}
	if r.Kind == "ANS" {
		return nil, perr("ANS on channel 0")
	}
	_, body := ParseEntity(r.Payload)
	return parseMgmt(body)
}

func (s *Session) closeChannel(number uint32, code int) error {
	s.mu.Lock()
	if c, ok := s.chans[number]; ok && c.busy() {
		s.mu.Unlock()
		return &RefusedError{550, "still working"}
	}
	s.mu.Unlock()
	m, err := s.mgmtRequest(context.Background(), &mgmt{kind: "close", number: number, code: code})
	if err != nil {
		return err
	}
	switch m.kind {
	case "ok":
		s.mu.Lock()
		c := s.chans[number]
		delete(s.chans, number)
		s.mu.Unlock()
		if c != nil {
			c.kill()
			if c.queue != nil {
				c.queue.push(nil)
			}
		}
		return nil
	case "error":
		return &RefusedError{m.code, m.message}
	}
	return perr("unexpected reply to <close>: %s", m.kind)
}

func (s *Session) mgmtLoop(q *queue[*IncomingMessage]) {
	for {
		msg, _ := q.pop(context.Background())
		if msg == nil {
			return
		}
		r := &Responder{s: s, Channel: 0, Msgno: msg.Msgno}
		body, err := msg.Collect(context.Background(), 1<<20)
		if err != nil {
			return
		}
		var reply *mgmt
		var opened *uint32
		_, xmlBody := ParseEntity(body)
		m, err := parseMgmt(xmlBody)
		if err != nil {
			reply = &mgmt{kind: "error", code: 500, message: err.Error()}
		} else {
			switch m.kind {
			case "start":
				reply = s.handleStart(m)
				if reply.kind == "profile" {
					n := m.number
					opened = &n
				}
			case "close":
				reply = s.handleClose(m.number)
			default:
				reply = &mgmt{kind: "error", code: 501, message: "unexpected element <" + m.kind + ">"}
			}
		}
		if reply.kind == "error" {
			err = r.Error(reply.code, reply.message)
		} else {
			err = r.RPY(xmlEntity(reply.xml()))
		}
		if err != nil {
			return
		}
		if opened != nil {
			s.openWindow(*opened) // SEQ must follow the RPY that creates the channel
		}
		if reply.kind == "ok" && s.releasing.Load() {
			s.out.push(nil)
			return
		}
	}
}

func (s *Session) handleStart(m *mgmt) *mgmt {
	if s.releasing.Load() {
		return &mgmt{kind: "error", code: 550, message: "session is being released"}
	}
	peerParity := uint32(1)
	if s.initiator {
		peerParity = 0
	}
	n := m.number
	s.mu.Lock()
	_, used := s.chans[n]
	s.mu.Unlock()
	if n == 0 || n%2 != peerParity || used {
		return &mgmt{kind: "error", code: 553, message: fmt.Sprintf("channel number %d is invalid or in use", n)}
	}
	var chosen *profileEl
	var h ProfileHandler
	for i := range m.profiles {
		if x, ok := s.profiles[m.profiles[i].uri]; ok {
			chosen, h = &m.profiles[i], x
			break
		}
	}
	if chosen == nil {
		return &mgmt{kind: "error", code: 550, message: "no acceptable profile"}
	}
	initReply, hasInit := "", false
	if sh, ok := h.(StartHandler); ok {
		v, err := sh.OnStart(s, n, chosen.data)
		if err != nil {
			var re *RefusedError
			if errors.As(err, &re) {
				return &mgmt{kind: "error", code: re.Code, message: re.Message}
			}
			return &mgmt{kind: "error", code: 550, message: err.Error()}
		}
		initReply, hasInit = v, v != ""
	}
	q := newQueue[*IncomingMessage]()
	s.mu.Lock()
	if _, used := s.chans[n]; used {
		s.mu.Unlock()
		return &mgmt{kind: "error", code: 553, message: fmt.Sprintf("channel number %d is in use", n)}
	}
	c := newChan(n, chosen.uri, 0)
	c.queue = q
	s.chans[n] = c
	s.mu.Unlock()
	go s.channelLoop(n, h, q)
	return &mgmt{kind: "profile", profiles: []profileEl{{uri: chosen.uri, data: initReply, has: hasInit}}}
}

func (s *Session) handleClose(n uint32) *mgmt {
	s.mu.Lock()
	if n == 0 {
		for k, c := range s.chans {
			if k != 0 && c.busy() {
				s.mu.Unlock()
				return &mgmt{kind: "error", code: 550, message: "still working"}
			}
		}
		s.releasing.Store(true)
		s.mu.Unlock()
		return &mgmt{kind: "ok"}
	}
	c, ok := s.chans[n]
	if !ok {
		s.mu.Unlock()
		return &mgmt{kind: "error", code: 553, message: fmt.Sprintf("channel %d is not open", n)}
	}
	if c.busy() {
		s.mu.Unlock()
		return &mgmt{kind: "error", code: 550, message: "still working"}
	}
	delete(s.chans, n)
	s.mu.Unlock()
	c.kill()
	if c.queue != nil {
		c.queue.push(nil)
	}
	return &mgmt{kind: "ok"}
}

func (s *Session) channelLoop(number uint32, h ProfileHandler, q *queue[*IncomingMessage]) {
	for {
		msg, _ := q.pop(context.Background())
		if msg == nil {
			return
		}
		r := &Responder{s: s, Channel: number, Msgno: msg.Msgno}
		if h == nil {
			msg.Discard()
			if r.Error(550, "this peer does not serve the profile") != nil && s.Closed() {
				return
			}
			continue
		}
		err := func() (err error) {
			defer func() {
				if p := recover(); p != nil {
					err = fmt.Errorf("handler panic: %v", p)
				}
			}()
			return h.OnMessage(s, msg, r)
		}()
		if s.Closed() {
			return
		}
		msg.Discard()
		switch r.getState() {
		case stIdle:
			reason := "handler produced no reply"
			if err != nil {
				reason = "handler failed: " + err.Error()
			}
			_ = r.Error(554, reason)
		case stAnswering:
			_ = r.NUL()
		}
	}
}

// ---------------------------------------------------------------------------
// Listener

// Listener accepts BEEP sessions.
type Listener struct {
	ln       net.Listener
	cfg      *Config
	profiles map[string]ProfileHandler
	tls      *TLSServer
	mu       sync.Mutex
	sessions map[*Session]bool
	closed   atomic.Bool
}

// Listen binds addr ("host:port"). tls may be nil.
func Listen(addr string, cfg *Config, profiles map[string]ProfileHandler, tls *TLSServer) (*Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Listener{ln: ln, cfg: cfg, profiles: profiles, tls: tls, sessions: map[*Session]bool{}}, nil
}

// Addr is the bound address.
func (l *Listener) Addr() net.Addr { return l.ln.Addr() }

// Serve accepts connections until Close.
func (l *Listener) Serve() error {
	for {
		conn, err := l.ln.Accept()
		if err != nil {
			if l.closed.Load() {
				return nil
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}
		go l.handle(conn)
	}
}

func (l *Listener) handle(conn net.Conn) {
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	var err error
	if l.tls != nil {
		if conn, err = tlsServerBootstrap(conn, l.tls); err != nil {
			_ = conn.Close()
			return
		}
	}
	s, err := Establish(conn, false, l.cfg, l.profiles)
	if err != nil {
		_ = conn.Close()
		return
	}
	l.mu.Lock()
	l.sessions[s] = true
	l.mu.Unlock()
	<-s.Done()
	l.mu.Lock()
	delete(l.sessions, s)
	l.mu.Unlock()
}

// Close stops accepting and aborts live sessions.
func (l *Listener) Close() error {
	l.closed.Store(true)
	err := l.ln.Close()
	l.mu.Lock()
	for s := range l.sessions {
		s.Abort()
	}
	l.mu.Unlock()
	return err
}
