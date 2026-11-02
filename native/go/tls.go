package beep

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"os"
	"strings"
)

// TLSProfile is the BEEP TLS tuning profile (RFC 3080 §3.1).
const TLSProfile = "http://iana.org/beep/TLS"

// TLSClient configures the initiator side. CAFile (PEM) replaces system roots when set.
type TLSClient struct {
	ServerName string
	CAFile     string
	Config     *tls.Config // optional; overrides the fields above
}

// TLSServer configures the listener side.
type TLSServer struct {
	CertFile string
	KeyFile  string
	Config   *tls.Config // optional; overrides the fields above
}

// boot does frame I/O directly on the connection before the real session; reads never
// over-consume, so the TLS handshake sees exactly the bytes that follow.
type boot struct {
	conn net.Conn
	seq  map[uint32]uint32
}

type oneByteReader struct{ c net.Conn }

func (o oneByteReader) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return o.c.Read(p)
}

func (b *boot) send(typ string, ch, msgno uint32, x string) error {
	p := xmlEntity(x)
	s := b.seq[ch]
	b.seq[ch] = s + uint32(len(p))
	_, err := b.conn.Write(encodeFrame(typ, ch, msgno, false, s, p, 0))
	return err
}

func (b *boot) recv() (string, uint32, uint32, []byte, error) {
	// bufio over a one-byte reader: header bytes are consumed exactly.
	r := bufio.NewReaderSize(oneByteReader{b.conn}, 16)
	var acc []byte
	for {
		f, err := readFrame(r, 65536)
		if err != nil {
			return "", 0, 0, nil, err
		}
		if f.typ == "SEQ" {
			continue
		}
		acc = append(acc, f.payload...)
		if !f.more {
			_, body := ParseEntity(acc)
			return f.typ, f.channel, f.msgno, body, nil
		}
	}
}

func tlsClientBootstrap(conn net.Conn, o *TLSClient) (net.Conn, error) {
	b := &boot{conn: conn, seq: map[uint32]uint32{}}
	fail := func(err error) (net.Conn, error) { _ = conn.Close(); return nil, err }
	if err := b.send("RPY", 0, 0, "<greeting />"); err != nil {
		return fail(err)
	}
	t, ch, _, body, err := b.recv()
	if err != nil {
		return fail(err)
	}
	g, err := parseMgmt(body)
	if err != nil || t != "RPY" || ch != 0 || g.kind != "greeting" {
		return fail(perr("expected greeting"))
	}
	offered := false
	for _, p := range g.profiles {
		offered = offered || p.uri == TLSProfile
	}
	if !offered {
		return fail(errors.New("beep: peer does not offer the TLS profile"))
	}
	start := &mgmt{kind: "start", number: 1, serverName: o.ServerName, profiles: []profileEl{{uri: TLSProfile, data: "<ready />", has: true}}}
	if err := b.send("MSG", 0, 1, start.xml()); err != nil {
		return fail(err)
	}
	t, _, _, body, err = b.recv()
	if err != nil {
		return fail(err)
	}
	m, err := parseMgmt(body)
	if err != nil {
		return fail(err)
	}
	if m.kind == "error" {
		return fail(&RefusedError{m.code, m.message})
	}
	if t != "RPY" || m.kind != "profile" || !strings.Contains(m.profiles[0].data, "<proceed") {
		return fail(errors.New("beep: peer did not proceed with TLS"))
	}
	cfg := o.Config
	if cfg == nil {
		cfg = &tls.Config{ServerName: o.ServerName, MinVersion: tls.VersionTLS12}
		if o.CAFile != "" {
			pem, err := os.ReadFile(o.CAFile)
			if err != nil {
				return fail(err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return fail(errors.New("beep: no certificates in " + o.CAFile))
			}
			cfg.RootCAs = pool
		}
	}
	tc := tls.Client(conn, cfg)
	if err := tc.Handshake(); err != nil {
		return fail(err)
	}
	return tc, nil
}

func tlsServerBootstrap(conn net.Conn, o *TLSServer) (net.Conn, error) {
	b := &boot{conn: conn, seq: map[uint32]uint32{}}
	g := &mgmt{kind: "greeting", profiles: []profileEl{{uri: TLSProfile}}}
	if err := b.send("RPY", 0, 0, g.xml()); err != nil {
		return conn, err
	}
	t, ch, msgno, _, err := b.recv()
	if err != nil {
		return conn, err
	}
	if t != "RPY" || ch != 0 || msgno != 0 {
		return conn, perr("expected greeting")
	}
	tlsCh := uint32(0)
	proceeded := false
	for i := 0; i < 16 && !proceeded; i++ {
		t, ch, msgno, body, err := b.recv()
		if err != nil {
			return conn, err
		}
		if t != "MSG" {
			return conn, perr("expected MSG during TLS negotiation")
		}
		if tlsCh != 0 && ch == tlsCh {
			if bytes.Contains(body, []byte("<ready")) {
				err = b.send("RPY", ch, msgno, "<proceed />")
				proceeded = true
			} else {
				err = b.send("ERR", ch, msgno, (&mgmt{kind: "error", code: 500, message: "expected <ready />"}).xml())
			}
			if err != nil {
				return conn, err
			}
			continue
		}
		m, err := parseMgmt(body)
		if err != nil {
			return conn, err
		}
		var p *profileEl
		if m.kind == "start" && m.number%2 == 1 {
			for i := range m.profiles {
				if m.profiles[i].uri == TLSProfile {
					p = &m.profiles[i]
				}
			}
		}
		switch {
		case p != nil && strings.Contains(p.data, "<ready"):
			err = b.send("RPY", 0, msgno, (&mgmt{kind: "profile", profiles: []profileEl{{uri: TLSProfile, data: "<proceed />", has: true}}}).xml())
			proceeded = true
		case p != nil:
			err = b.send("RPY", 0, msgno, (&mgmt{kind: "profile", profiles: []profileEl{{uri: TLSProfile}}}).xml())
			tlsCh = m.number
		case m.kind == "close" && m.number == 0:
			_ = b.send("RPY", 0, msgno, "<ok />")
			return conn, &ClosedError{"peer released session before TLS"}
		default:
			err = b.send("ERR", 0, msgno, (&mgmt{kind: "error", code: 538, message: "TLS required"}).xml())
		}
		if err != nil {
			return conn, err
		}
	}
	if !proceeded {
		return conn, errors.New("beep: peer never requested TLS")
	}
	cfg := o.Config
	if cfg == nil {
		cert, err := tls.LoadX509KeyPair(o.CertFile, o.KeyFile)
		if err != nil {
			return conn, err
		}
		cfg = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	}
	tc := tls.Server(conn, cfg)
	if err := tc.Handshake(); err != nil {
		return conn, err
	}
	return tc, nil
}
