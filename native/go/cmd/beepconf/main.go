// beepconf: cross-language conformance helper.
//
//	beepconf serve PORT     serve /echo, /stream/N, /err/N natively in Go
//	beepconf client ADDR    check a conformance server (exit 1 on failure)
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	beep "github.com/minta/beep-bridge/native/go"
)

func handler(ctx context.Context, req *beep.Request) (*beep.ServerReply, error) {
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
		return &beep.ServerReply{Status: 200, Headers: beep.Headers{{Name: "Content-Type", Value: "text/plain"}}, Stream: ch}, nil
	case strings.HasPrefix(p, "/err/"):
		code, _ := strconv.Atoi(p[5:])
		return nil, &beep.Failure{Code: code, Message: "go says no"}
	}
	ct := req.Headers.Get("Content-Type")
	if ct == "" {
		ct = "application/octet-stream"
	}
	return &beep.ServerReply{Status: 200, Headers: beep.Headers{{Name: "Content-Type", Value: ct}, {Name: "X-Lang", Value: "go"}}, Body: req.Body}, nil
}

func client(addr string) error {
	ctx := context.Background()
	c := beep.NewClient(addr, nil)
	defer c.Close()
	r, err := c.Call(ctx, "POST", "/echo", beep.Headers{{Name: "Content-Type", Value: "text/plain"}}, []byte("conformance"))
	if err != nil || r.Status != 200 || string(r.Body) != "conformance" {
		return fmt.Errorf("echo: %v %+v", err, r)
	}
	if r, err = c.Call(ctx, "GET", "/stream/5", nil, nil); err != nil || string(r.Body) != "line 0\nline 1\nline 2\nline 3\nline 4\n" {
		return fmt.Errorf("stream: %v", err)
	}
	big := bytes.Repeat([]byte("0123456789"), 200_000)
	if r, err = c.Call(ctx, "POST", "/echo", nil, big); err != nil || !bytes.Equal(r.Body, big) {
		return fmt.Errorf("2MB echo: %v", err)
	}
	_, err = c.Call(ctx, "GET", "/err/537", nil, nil)
	var re *beep.RemoteError
	if !errors.As(err, &re) || re.Code != 537 {
		return fmt.Errorf("err: %v", err)
	}
	return nil
}

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: beepconf serve PORT | beepconf client ADDR")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "serve":
		srv, err := beep.NewServer("127.0.0.1:"+os.Args[2], handler, nil, nil)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println("READY", srv.Addr())
		_ = srv.Serve()
	case "client":
		if err := client(os.Args[2]); err != nil {
			fmt.Println("FAIL:", err)
			os.Exit(1)
		}
		fmt.Println("ok")
	}
}
