package remote

import (
	"context"
	"errors"
	"io/fs"
	"net/http"
	"os"
	"strings"
	"syscall"

	"github.com/ChristopherDavenport/agentworkspace"
)

// The errors of the wire's own, which the client returns as they are
// and a served workspace's error is never mapped to.
var (
	// ErrUnauthenticated is a request no principal was resolved for: no
	// authenticator, no token, or one the authenticator refused.
	ErrUnauthenticated = errors.New("remote: unauthenticated")
	// ErrForbidden is a request whose principal lacks the scope its
	// route needs.
	ErrForbidden = errors.New("remote: the principal lacks the scope this needs")
	// ErrLimit is a request over one of the server's bounds: a body too
	// large, too many sessions, too many processes in one, a listing too
	// long.
	ErrLimit = errors.New("remote: over a bound of the server's")
	// ErrNoSession is a session that lapsed, was closed, or is another
	// principal's.
	ErrNoSession = errors.New("remote: no such session")
	// ErrNoProcess is a process id the session did not start, or closed.
	ErrNoProcess = errors.New("remote: no such process in this session")
)

// A code is an error's kind on the wire, which the client maps back to
// the sentinel it came from, so errors.Is holds on the client as it
// does on the served workspace.
const (
	codeOutside         = "outside"
	codeNotExist        = "not_exist"
	codeExist           = "exist"
	codePermission      = "permission"
	codeInvalid         = "invalid"
	codeUnsupported     = "unsupported"
	codeProcessDone     = "process_done"
	codeClosed          = "closed"
	codeUnauthenticated = "unauthenticated"
	codeForbidden       = "forbidden"
	codeLimit           = "limit"
	codeNoSession       = "no_session"
	codeNoProcess       = "no_process"
	codeOther           = "other"
)

// codes are the sentinels a code stands for and the status it is sent
// with, in the order a served workspace's error is matched: ErrOutside
// before anything a link's refusal may also be.
var codes = []struct {
	code   string
	err    error
	status int
}{
	{codeOutside, agentworkspace.ErrOutside, http.StatusForbidden},
	{codeNotExist, fs.ErrNotExist, http.StatusNotFound},
	{codeExist, fs.ErrExist, http.StatusConflict},
	{codePermission, fs.ErrPermission, http.StatusForbidden},
	{codeInvalid, fs.ErrInvalid, http.StatusBadRequest},
	{codeUnsupported, errors.ErrUnsupported, http.StatusNotImplemented},
	{codeProcessDone, os.ErrProcessDone, http.StatusConflict},
	{codeClosed, fs.ErrClosed, http.StatusGone},
	{codeUnauthenticated, ErrUnauthenticated, http.StatusUnauthorized},
	{codeForbidden, ErrForbidden, http.StatusForbidden},
	{codeLimit, ErrLimit, http.StatusRequestEntityTooLarge},
	{codeNoSession, ErrNoSession, http.StatusNotFound},
	{codeNoProcess, ErrNoProcess, http.StatusNotFound},
}

// wireError is an error on the wire: its code and the server's words.
type wireError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// errorReply is the body of every refusal.
type errorReply struct {
	Error wireError `json:"error"`
}

// statusOf is the HTTP status code is sent with.
func statusOf(code string) int {
	for _, c := range codes {
		if c.code == code {
			return c.status
		}
	}
	return http.StatusInternalServerError
}

// errorOf is err, a served workspace's, as the wire carries it. A
// mapped error's message is the system's words for it (an errno's
// text) or the sentinel's, never the error's own text, which may name
// a path on the server's machine; the request's name is the client's
// to add. Any other error's message is its text, bounded: the served
// workspace's words, such as "x is not a regular file".
func errorOf(err error) wireError {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return wireError{Code: codeOther, Message: "the request ended"}
	}
	for _, c := range codes {
		if !errors.Is(err, c.err) {
			continue
		}
		msg := c.err.Error()
		var errno syscall.Errno
		if c.code != codeOutside && errors.As(err, &errno) {
			msg = errno.Error()
		}
		return wireError{Code: c.code, Message: bounded(msg)}
	}
	return wireError{Code: codeOther, Message: bounded(err.Error())}
}

// bounded is s cut to MaxErrorBytes and made valid UTF-8.
func bounded(s string) string {
	if len(s) > MaxErrorBytes {
		s = s[:MaxErrorBytes]
	}
	return strings.ToValidUTF8(s, "?")
}

// err is e as the client's error: its code's sentinel, wrapped with
// the server's words, or those words alone for a code it does not
// know.
func (e wireError) err() error {
	msg := bounded(e.Message)
	for _, c := range codes {
		if c.code == e.Code {
			return &remoteError{sentinel: c.err, msg: msg}
		}
	}
	return &remoteError{msg: msg}
}

// remoteError is an error the server sent: its words, and the sentinel
// its code maps to, nil for an error of no kind the wire names.
type remoteError struct {
	sentinel error
	msg      string
}

func (e *remoteError) Error() string { return e.msg }
func (e *remoteError) Unwrap() error { return e.sentinel }
