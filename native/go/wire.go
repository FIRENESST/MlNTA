// Package beep is a pure-Go implementation of RFC 3080/3081 BEEP with the MINTA RPC
// profile (urn:minta:beep:rpc:1). See docs/PROTOCOL.md in the repository.
package beep

import (
	"bufio"
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	MaxHeaderLine = 128
	BeepXML       = "application/beep+xml"
	maxI31        = 2147483647
	maxU32        = 4294967295
)

// ProtocolError means the peer violated RFC 3080/3081; the session is dropped.
type ProtocolError struct{ Msg string }

func (e *ProtocolError) Error() string { return "beep protocol violation: " + e.Msg }

func perr(format string, a ...any) error { return &ProtocolError{fmt.Sprintf(format, a...)} }

type frame struct {
	typ     string
	channel uint32
	msgno   uint32
	more    bool
	seqno   uint32
	ansno   uint32
	payload []byte
	ackno   uint32
	window  uint32
}

func parseNum(s string, max uint64) (uint32, error) {
	if s == "" || len(s) > 10 {
		return 0, perr("bad number %q in frame header", s)
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, perr("bad number %q in frame header", s)
		}
	}
	v, _ := strconv.ParseUint(s, 10, 64)
	if v > max {
		return 0, perr("number %d out of range", v)
	}
	return uint32(v), nil
}

// readFrame reads one frame; io.EOF on a clean end between frames.
func readFrame(r *bufio.Reader, maxPayload int) (*frame, error) {
	line := make([]byte, 0, 64)
	for {
		b, err := r.ReadByte()
		if err != nil {
			if err == io.EOF && len(line) == 0 {
				return nil, io.EOF
			}
			if err == io.EOF {
				return nil, perr("connection closed mid-frame")
			}
			return nil, err
		}
		if len(line) >= MaxHeaderLine {
			return nil, perr("frame header line too long")
		}
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == '\r' && b == '\n' {
			break
		}
	}
	for _, b := range line[:len(line)-2] {
		if b < 0x20 || b > 0x7e {
			return nil, perr("frame header is not ASCII")
		}
	}
	text := string(line[:len(line)-2])
	p := strings.Split(text, " ")
	f := &frame{typ: p[0]}
	var err error
	if f.typ == "SEQ" {
		if len(p) != 4 {
			return nil, perr("malformed SEQ header %q", text)
		}
		if f.channel, err = parseNum(p[1], maxI31); err != nil {
			return nil, err
		}
		if f.ackno, err = parseNum(p[2], maxU32); err != nil {
			return nil, err
		}
		if f.window, err = parseNum(p[3], maxI31); err != nil {
			return nil, err
		}
		return f, nil
	}
	ans := f.typ == "ANS"
	switch f.typ {
	case "MSG", "RPY", "ERR", "ANS", "NUL":
	default:
		return nil, perr("unknown frame keyword %q", f.typ)
	}
	want := 6
	if ans {
		want = 7
	}
	if len(p) != want {
		return nil, perr("malformed frame header %q", text)
	}
	if p[3] != "." && p[3] != "*" {
		return nil, perr("bad continuation indicator in %q", text)
	}
	if f.channel, err = parseNum(p[1], maxI31); err != nil {
		return nil, err
	}
	if f.msgno, err = parseNum(p[2], maxI31); err != nil {
		return nil, err
	}
	f.more = p[3] == "*"
	if f.seqno, err = parseNum(p[4], maxU32); err != nil {
		return nil, err
	}
	size, err := parseNum(p[5], maxI31)
	if err != nil {
		return nil, err
	}
	if ans {
		if f.ansno, err = parseNum(p[6], maxI31); err != nil {
			return nil, err
		}
	}
	if f.typ == "NUL" && (size != 0 || f.more) {
		return nil, perr("NUL frame must have size 0 and no continuation")
	}
	if int(size) > maxPayload {
		return nil, perr("frame payload %d exceeds limit %d", size, maxPayload)
	}
	buf := make([]byte, int(size)+5)
	if _, err := io.ReadFull(r, buf); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, perr("connection closed mid-frame")
		}
		return nil, err
	}
	if string(buf[size:]) != "END\r\n" {
		return nil, perr("frame trailer is not END CRLF")
	}
	f.payload = buf[:size:size]
	return f, nil
}

func encodeFrame(typ string, channel, msgno uint32, more bool, seqno uint32, payload []byte, ansno uint32) []byte {
	m := "."
	if more {
		m = "*"
	}
	var head string
	if typ == "ANS" {
		head = fmt.Sprintf("ANS %d %d %s %d %d %d\r\n", channel, msgno, m, seqno, len(payload), ansno)
	} else {
		head = fmt.Sprintf("%s %d %d %s %d %d\r\n", typ, channel, msgno, m, seqno, len(payload))
	}
	out := make([]byte, 0, len(head)+len(payload)+5)
	out = append(out, head...)
	out = append(out, payload...)
	return append(out, "END\r\n"...)
}

func encodeSeq(channel, ackno, window uint32) []byte {
	return []byte(fmt.Sprintf("SEQ %d %d %d\r\n", channel, ackno, window))
}

// ---------------------------------------------------------------------------
// MIME entities

// Header is one header field.
type Header struct{ Name, Value string }

// Headers is an ordered list of header fields with case-insensitive lookup.
type Headers []Header

// Get returns the first value for name ("" if absent).
func (h Headers) Get(name string) string {
	v, _ := h.Lookup(name)
	return v
}

// Lookup returns the first value for name and whether it exists.
func (h Headers) Lookup(name string) (string, bool) {
	for _, x := range h {
		if strings.EqualFold(x.Name, name) {
			return x.Value, true
		}
	}
	return "", false
}

// Add appends a field.
func (h *Headers) Add(name, value string) { *h = append(*h, Header{name, value}) }

var crlfStripper = strings.NewReplacer("\r", "", "\n", "")

// EncodeEntity builds a MIME entity payload.
func EncodeEntity(h Headers, body []byte) []byte {
	var b bytes.Buffer
	for _, x := range h {
		b.WriteString(crlfStripper.Replace(x.Name))
		b.WriteString(": ")
		b.WriteString(crlfStripper.Replace(x.Value))
		b.WriteString("\r\n")
	}
	b.WriteString("\r\n")
	b.Write(body)
	return b.Bytes()
}

// splitHead returns (headers, bodyOffset, ok); ok=false means "need more bytes".
func splitHead(buf []byte, complete bool) (Headers, int, bool) {
	if bytes.HasPrefix(buf, []byte("\r\n")) {
		return Headers{}, 2, true
	}
	p := bytes.Index(buf, []byte("\r\n"))
	if p < 0 {
		return Headers{}, 0, complete
	}
	if bytes.IndexByte(buf[:p], ':') < 0 {
		return Headers{}, 0, true
	}
	q := bytes.Index(buf, []byte("\r\n\r\n"))
	if q < 0 {
		return Headers{}, 0, complete
	}
	var h Headers
	for _, l := range strings.Split(string(buf[:q+2]), "\r\n") {
		if l == "" {
			continue
		}
		if (l[0] == ' ' || l[0] == '\t') && len(h) > 0 {
			h[len(h)-1].Value += " " + strings.TrimSpace(l)
			continue
		}
		if i := strings.IndexByte(l, ':'); i > 0 {
			h = append(h, Header{strings.TrimSpace(l[:i]), strings.TrimSpace(l[i+1:])})
		}
	}
	return h, q + 4, true
}

// ParseEntity splits a payload into headers and body.
func ParseEntity(payload []byte) (Headers, []byte) {
	h, off, _ := splitHead(payload, true)
	return h, payload[off:]
}

// ---------------------------------------------------------------------------
// Channel-0 management XML

type xmlNode struct {
	XMLName  xml.Name
	Attrs    []xml.Attr `xml:",any,attr"`
	Text     string     `xml:",chardata"`
	Children []xmlNode  `xml:",any"`
}

func (n *xmlNode) attr(name string) (string, bool) {
	for _, a := range n.Attrs {
		if a.Name.Local == name {
			return a.Value, true
		}
	}
	return "", false
}

type profileEl struct {
	uri  string
	data string
	has  bool
}

func xesc(s string) string {
	var b bytes.Buffer
	for _, r := range s {
		switch r {
		case '<':
			b.WriteString("&lt;")
		case '>':
			b.WriteString("&gt;")
		case '&':
			b.WriteString("&amp;")
		case '\'':
			b.WriteString("&apos;")
		case '"':
			b.WriteString("&quot;")
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

func (p profileEl) xml() string {
	if !p.has {
		return "<profile uri='" + xesc(p.uri) + "' />"
	}
	if !strings.Contains(p.data, "]]>") {
		return "<profile uri='" + xesc(p.uri) + "'><![CDATA[" + p.data + "]]></profile>"
	}
	return "<profile uri='" + xesc(p.uri) + "'>" + xesc(p.data) + "</profile>"
}

// mgmt is a channel-0 message: kind ∈ greeting, start, close, profile, ok, error.
type mgmt struct {
	kind       string
	profiles   []profileEl
	number     uint32
	code       int
	message    string
	serverName string
}

func (m *mgmt) xml() string {
	switch m.kind {
	case "greeting":
		if len(m.profiles) == 0 {
			return "<greeting />"
		}
		var b strings.Builder
		b.WriteString("<greeting>")
		for _, p := range m.profiles {
			b.WriteString("<profile uri='" + xesc(p.uri) + "' />")
		}
		b.WriteString("</greeting>")
		return b.String()
	case "start":
		var b strings.Builder
		fmt.Fprintf(&b, "<start number='%d'", m.number)
		if m.serverName != "" {
			b.WriteString(" serverName='" + xesc(m.serverName) + "'")
		}
		b.WriteString(">")
		for _, p := range m.profiles {
			b.WriteString(p.xml())
		}
		b.WriteString("</start>")
		return b.String()
	case "close":
		if m.message != "" {
			return fmt.Sprintf("<close number='%d' code='%d'>%s</close>", m.number, m.code, xesc(m.message))
		}
		return fmt.Sprintf("<close number='%d' code='%d' />", m.number, m.code)
	case "profile":
		return m.profiles[0].xml()
	case "ok":
		return "<ok />"
	case "error":
		return fmt.Sprintf("<error code='%d'>%s</error>", m.code, xesc(m.message))
	}
	panic("unknown mgmt kind " + m.kind)
}

func parseMgmt(body []byte) (*mgmt, error) {
	if bytes.Contains(body, []byte("<!DOCTYPE")) || bytes.Contains(body, []byte("<!ENTITY")) {
		return nil, perr("channel 0: DOCTYPE/ENTITY declarations are not allowed")
	}
	var n xmlNode
	if err := xml.Unmarshal(body, &n); err != nil {
		return nil, perr("channel 0: bad XML: %v", err)
	}
	num := func(name string, dflt int64) (uint32, error) {
		v, ok := n.attr(name)
		if !ok {
			if dflt < 0 {
				return 0, perr("channel 0: <%s> lacks %s", n.XMLName.Local, name)
			}
			return uint32(dflt), nil
		}
		x, err := strconv.ParseUint(strings.TrimSpace(v), 10, 32)
		if err != nil || x > maxI31 {
			return 0, perr("channel 0: bad %s attribute", name)
		}
		return uint32(x), nil
	}
	prof := func(c xmlNode) profileEl {
		uri, _ := c.attr("uri")
		t := strings.TrimSpace(c.Text)
		return profileEl{uri: uri, data: t, has: t != ""}
	}
	m := &mgmt{kind: n.XMLName.Local, code: 200}
	var err error
	switch m.kind {
	case "greeting":
		for _, c := range n.Children {
			if c.XMLName.Local == "profile" {
				uri, _ := c.attr("uri")
				m.profiles = append(m.profiles, profileEl{uri: uri})
			}
		}
	case "start":
		if m.number, err = num("number", -1); err != nil {
			return nil, err
		}
		m.serverName, _ = n.attr("serverName")
		for _, c := range n.Children {
			if c.XMLName.Local == "profile" {
				m.profiles = append(m.profiles, prof(c))
			}
		}
	case "close":
		if m.number, err = num("number", -1); err != nil {
			return nil, err
		}
		c, err := num("code", 200)
		if err != nil {
			return nil, err
		}
		m.code = int(c)
		m.message = strings.TrimSpace(n.Text)
	case "profile":
		m.profiles = []profileEl{prof(n)}
	case "ok":
	case "error":
		c, err := num("code", 554)
		if err != nil {
			return nil, err
		}
		m.code = int(c)
		m.message = strings.TrimSpace(n.Text)
	default:
		return nil, perr("channel 0: unknown element <%s>", m.kind)
	}
	return m, nil
}

func xmlEntity(x string) []byte {
	return EncodeEntity(Headers{{"Content-Type", BeepXML}}, []byte(x))
}

func errorPayload(code int, message string) []byte {
	return xmlEntity((&mgmt{kind: "error", code: code, message: message}).xml())
}

func parseErrorPayload(payload []byte) (int, string) {
	_, body := ParseEntity(payload)
	if m, err := parseMgmt(body); err == nil && m.kind == "error" {
		return m.code, m.message
	}
	return 554, strings.TrimSpace(string(body))
}
