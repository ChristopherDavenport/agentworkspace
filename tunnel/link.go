package tunnel

// What both ends share: the WebSocket as a net.Conn, the HTTP/2 it
// carries, and the options that shape both.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/coder/websocket"
)

// The defaults of the options both ends take.
const (
	// DefaultMaxMessageBytes bounds one WebSocket message either end
	// reads, and so each write it sends (WithMaxMessageBytes).
	DefaultMaxMessageBytes = 64 << 10
	// DefaultPingInterval is how long a tunnel may go without a frame
	// from the other end before this end pings it (WithPing).
	DefaultPingInterval = 15 * time.Second
	// DefaultPingTimeout is how long a ping goes unanswered before the
	// tunnel is taken as dead and closed (WithPing).
	DefaultPingTimeout = 15 * time.Second
)

// The bounds of the HTTP/2 a tunnel carries, which are not options.
//
// They are chosen so that nothing the workspace end's handler allows
// can stop a session's renewal, a Wait, a Signal, a Close or a new
// Start from getting through. The connection's window is never the one
// that binds: ConnWindow is MaxStreams streams' windows, so each stream
// can be stalled only by its own reader. And the streams the handler
// lets the server hold open stay well under MaxStreams: with Serve's
// defaults for remote's bounds, DefaultSessions sessions of
// DefaultProcesses processes, each holding at most four streams (its
// input, its output, its standard error and the client's one wait), are
// 4 × 32 × 4 = 512 streams, and DefaultTransfers Execs and file reads
// and writes are 128 more: 640 of 1024, which leaves 384 for the
// requests answered at once (renewals, starts, signals, closes, the
// file system's). Raising remote's bounds through WithHandlerOptions
// past that arithmetic gives the guarantee up.
const (
	// MaxStreams bounds the requests open at once on one tunnel: every
	// stream, wait and Exec of remote's is one while it lasts. Past it, a
	// request waits for one to end.
	MaxStreams = 1024
	// StreamWindow is how much of one request's or reply's body either
	// end holds that its reader has not taken; a reader that takes no
	// more stalls that stream alone.
	StreamWindow = 128 << 10
	// ConnWindow is how much of all a tunnel's bodies either end holds
	// that its readers have not taken: MaxStreams × StreamWindow, so that
	// the connection's window never runs out before a stream's does.
	// It is the most one tunnel buffers unread at the hub (128 MiB).
	ConnWindow = MaxStreams * StreamWindow
	// HandshakeTimeout bounds a WebSocket upgrade at the workspace end,
	// the wait for HTTP/2's preface there, and at the hub the wait for a
	// new connection to answer a request before it is taken.
	HandshakeTimeout = 30 * time.Second
)

// linkConfig is what both ends share.
type linkConfig struct {
	maxMessage   int
	pingInterval time.Duration
	pingTimeout  time.Duration
	handshake    time.Duration // HandshakeTimeout but in tests
	log          *slog.Logger
}

func defaultLink() linkConfig {
	return linkConfig{
		maxMessage:   DefaultMaxMessageBytes,
		pingInterval: DefaultPingInterval,
		pingTimeout:  DefaultPingTimeout,
		handshake:    HandshakeTimeout,
		log:          slog.New(slog.DiscardHandler),
	}
}

func (l linkConfig) valid() error {
	switch {
	case l.maxMessage <= 0:
		return errors.New("tunnel: a message bound that is not positive")
	case l.pingInterval <= 0 || l.pingTimeout <= 0:
		return errors.New("tunnel: a ping interval or timeout that is not positive")
	}
	return nil
}

// An Option configures Serve, the workspace end.
type Option interface{ applyServe(*serveConfig) }

// A HubOption configures NewHub, the server end.
type HubOption interface{ applyHub(*hubConfig) }

// A LinkOption configures either end: it is an Option and a HubOption.
type LinkOption func(*linkConfig)

func (o LinkOption) applyServe(c *serveConfig) { o(&c.link) }
func (o LinkOption) applyHub(c *hubConfig)     { o(&c.link) }

// WithMaxMessageBytes bounds one WebSocket message this end reads: a
// longer one ends the tunnel. This end sends its writes in messages of
// at most n bytes, so the two ends' bounds should agree, or the
// workspace end's be the smaller. The default is
// DefaultMaxMessageBytes.
func WithMaxMessageBytes(n int) LinkOption { return func(c *linkConfig) { c.maxMessage = n } }

// WithPing is how a dead tunnel is noticed: after interval without a
// frame from the other end, this end sends an HTTP/2 ping, and closes
// the tunnel when timeout passes without its answer. A tunnel that has
// died is noticed within interval and timeout together. The defaults
// are DefaultPingInterval and DefaultPingTimeout.
func WithPing(interval, timeout time.Duration) LinkOption {
	return func(c *linkConfig) { c.pingInterval, c.pingTimeout = interval, timeout }
}

// WithLogger logs the tunnel's comings and goings at this end: a
// connection made, ended or refused, and a redial. Nothing logged holds
// a credential or a request's headers. The default logs nothing.
func WithLogger(l *slog.Logger) LinkOption {
	return func(c *linkConfig) {
		if l != nil {
			c.log = l
		}
	}
}

// http2 is the HTTP/2 configuration of either end of a tunnel.
func (l linkConfig) http2() *http.HTTP2Config {
	return &http.HTTP2Config{
		MaxConcurrentStreams:          MaxStreams,
		MaxReceiveBufferPerConnection: ConnWindow,
		MaxReceiveBufferPerStream:     StreamWindow,
		SendPingTimeout:               l.pingInterval,
		PingTimeout:                   l.pingTimeout,
	}
}

// h2c is unencrypted HTTP/2 alone, spoken with prior knowledge: the
// WebSocket is the only transport, and TLS, when there is any, is the
// WebSocket's.
func h2c() *http.Protocols {
	var p http.Protocols
	p.SetUnencryptedHTTP2(true)
	return &p
}

// conn is a WebSocket as the byte stream HTTP/2 runs on: each write is
// sent in binary messages of at most the bound, and a message read
// over it ends the connection. ctx bounds its life.
func conn(ctx context.Context, c *websocket.Conn, maxMessage int) net.Conn {
	nc := websocket.NetConn(ctx, c, websocket.MessageBinary)
	// NetConn lifts the read limit; it is set again after.
	c.SetReadLimit(int64(maxMessage))
	return &chunked{Conn: nc, ws: c, max: maxMessage}
}

type chunked struct {
	net.Conn
	ws  *websocket.Conn
	max int
}

func (c *chunked) Write(p []byte) (int, error) {
	n := 0
	for len(p) > 0 {
		k := min(len(p), c.max)
		m, err := c.Conn.Write(p[:k])
		n += m
		if err != nil {
			return n, err
		}
		p = p[k:]
	}
	return n, nil
}

// Close ends the WebSocket at once, without waiting for the other
// end's answer to a close: by then HTTP/2 has said all it will.
func (c *chunked) Close() error {
	err := c.Conn.Close()
	c.ws.CloseNow()
	return err
}

// oneConn is a listener that gives one connection and then waits for
// its Close: what http.Server serves a tunnel on.
type oneConn struct {
	c    chan net.Conn
	done chan struct{}
	addr net.Addr
}

func listenOn(c net.Conn) *oneConn {
	l := &oneConn{c: make(chan net.Conn, 1), done: make(chan struct{}), addr: c.LocalAddr()}
	l.c <- c
	return l
}

func (l *oneConn) Accept() (net.Conn, error) {
	select {
	case c := <-l.c:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *oneConn) Close() error {
	select {
	case <-l.done:
	default:
		close(l.done)
	}
	return nil
}

func (l *oneConn) Addr() net.Addr { return l.addr }

// errorLog is l as an http.Server's or a Transport's error log, at
// debug: what HTTP/2 says of a tunnel that ended is not news.
func errorLog(l *slog.Logger) *slogWriter { return &slogWriter{l} }

type slogWriter struct{ l *slog.Logger }

func (w *slogWriter) Write(p []byte) (int, error) {
	w.l.Debug("tunnel: http", "message", string(p))
	return len(p), nil
}

var _ io.Writer = (*slogWriter)(nil)
