package mcpworkspace

// Refusals on the wire: every kind of RFC 0001 §2 both ways, and a
// message that names no path of the serving side's machine.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/internal/wireerr"
	"github.com/ChristopherDavenport/agentworkspace/mcpworkspace/wire"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// kinds are the sentinels each kind stands for, in wire.Kinds' order:
// a served error is matched in it, ErrOutside first.
var kinds = []struct {
	kind string
	err  error
}{
	{wire.KindOutside, agentworkspace.ErrOutside},
	{wire.KindNotExist, fs.ErrNotExist},
	{wire.KindExist, fs.ErrExist},
	{wire.KindPermission, fs.ErrPermission},
	{wire.KindInvalid, fs.ErrInvalid},
	{wire.KindUnsupported, errors.ErrUnsupported},
	{wire.KindProcessDone, os.ErrProcessDone},
	{wire.KindClosed, fs.ErrClosed},
	{wire.KindNoServer, agentworkspace.ErrNoServer},
	{wire.KindNoProcess, ErrNoProcess},
}

// kindOf is err's kind and the sentinel it stands for; KindOther and
// nil for an error of none.
func kindOf(err error) (string, error) {
	for _, k := range kinds {
		if errors.Is(err, k.err) {
			return k.kind, k.err
		}
	}
	return wire.KindOther, nil
}

// sentinelOf is the sentinel kind stands for, nil for KindOther or a
// kind this side does not know.
func sentinelOf(kind string) error {
	for _, k := range kinds {
		if k.kind == kind {
			return k.err
		}
	}
	return nil
}

// A ServeOption configures ServeFiles and NewProcesses.
type ServeOption func(*serveConfig)

type serveConfig struct {
	errorText bool
}

// WithErrorText sends a refusal's own text, as the served workspace
// wrote it, rather than its kind's words or a text with every path but
// a name in the workspace cut: for debugging a server, never for one a
// client that should not learn the serving side's paths (a home
// directory, a mount's layout) is given. RFC 0001 §15.
func WithErrorText() ServeOption { return func(c *serveConfig) { c.errorText = true } }

func configOf(opts []ServeOption) serveConfig {
	var c serveConfig
	for _, o := range opts {
		if o != nil {
			o(&c)
		}
	}
	return c
}

// ownRefusal is a refusal of the server's own, of the kind sentinel
// stands for (nil for none): its words are the server's, and cross as
// they are.
type ownRefusal struct {
	sentinel error
	msg      string
}

func (e *ownRefusal) Error() string { return e.msg }
func (e *ownRefusal) Unwrap() error { return e.sentinel }

// refuse is a refusal of the server's own, of sentinel's kind.
func refuse(sentinel error, format string, args ...any) error {
	return &ownRefusal{sentinel: sentinel, msg: fmt.Sprintf(format, args...)}
}

// message is err's message as it crosses, err being of the kind
// sentinel stands for (nil for none): an allow-list (wireerr.Message),
// the sentinel's words, the system's for an errno, a context's error's
// own, and wireerr.Other for anything else; never the served error's
// own text, which may name a path of this machine's in any form. A
// refusal of the server's own (ownRefusal) is its words, made of the
// request and nothing served. WithErrorText sends the text as it is.
func (c serveConfig) message(err, sentinel error) string {
	var own *ownRefusal
	if c.errorText || errors.As(err, &own) && own == err {
		return wireerr.Bounded(err.Error(), wire.MaxErrorBytes)
	}
	return wireerr.Message(err, sentinel, sentinel != agentworkspace.ErrOutside)
}

// fileError is err as a reply's error.
func (c serveConfig) fileError(err error) *wire.FileError {
	kind, sentinel := kindOf(err)
	return &wire.FileError{Kind: kind, Message: c.message(err, sentinel)}
}

// refused is err, a process or server method's, as the wire carries
// it: a JSON-RPC error with its kind as a wire.Refusal, CodeNoServer
// for a server not offered and CodeRefused for any other, a context's
// error among them (its words alone, never what wraps it).
func (c serveConfig) refused(err error) error {
	kind, sentinel := kindOf(err)
	msg := c.message(err, sentinel)
	if errors.Is(err, agentworkspace.ErrOutputFilter) {
		// Its words alone: the panic's value it wraps may hold a
		// secret the filter was given.
		kind, msg = wire.KindOther, agentworkspace.ErrOutputFilter.Error()
	}
	data, _ := json.Marshal(wire.Refusal{Kind: kind})
	code := int64(wire.CodeRefused)
	if kind == wire.KindNoServer {
		code = wire.CodeNoServer
	}
	return &jsonrpc.Error{Code: code, Message: msg, Data: data}
}

// answering is h with its error as the wire carries it.
func answering[P, R any](c serveConfig, h func(context.Context, *mcp.ServerSession, P) (R, error)) func(context.Context, *mcp.ServerSession, P) (R, error) {
	return func(ctx context.Context, ss *mcp.ServerSession, p P) (R, error) {
		r, err := h(ctx, ss, p)
		if err != nil {
			var zero R
			return zero, c.refused(err)
		}
		return r, nil
	}
}

// refusedError is a refusal the server sent: its words, and the
// sentinel its kind stands for.
type refusedError struct {
	sentinel error
	msg      string
}

func (e *refusedError) Error() string { return e.msg }
func (e *refusedError) Unwrap() error { return e.sentinel }

// fromWire is err, a method's, with the kind the server gave it: an
// error that errors.Is its sentinel. Any other error is as it is.
func fromWire(err error) error {
	var we *jsonrpc.Error
	if !errors.As(err, &we) {
		return err
	}
	var sentinel error
	switch we.Code {
	case wire.CodeNoServer:
		sentinel = agentworkspace.ErrNoServer
	case wire.CodeRefused:
		var r wire.Refusal
		if json.Unmarshal(we.Data, &r) == nil {
			sentinel = sentinelOf(r.Kind)
		}
	default:
		return err
	}
	msg := wireerr.Bounded(we.Message, wire.MaxErrorBytes)
	if sentinel == nil {
		return errors.New(msg)
	}
	return &refusedError{sentinel: sentinel, msg: msg}
}
