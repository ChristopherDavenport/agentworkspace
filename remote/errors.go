package remote

import (
	"context"
	"errors"
	"io/fs"
	"net/http"
	"os"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/internal/wireerr"
)

// The errors of the wire's own, which the client returns as they are
// and a served workspace's error is never mapped to.
var (
	// ErrUnauthenticated is a request no principal was resolved for: no
	// authenticator, or one the authenticator refused.
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
	codeNoServer        = "no_server"
	codeMediaType       = "media_type"
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
	{codeNoServer, agentworkspace.ErrNoServer, http.StatusNotFound},
	// A body of another type than the route's: the client's mistake,
	// as an invalid name is.
	{codeMediaType, fs.ErrInvalid, http.StatusUnsupportedMediaType},
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

// errorOf is err, a served workspace's, as the wire carries it: its
// code and an allow-listed message (wireerr.Message), the sentinel's
// words or the system's for an errno, and wireerr.Other for an error of
// no kind; never the error's own text, which may name a path on the
// server's machine in any form. The request's name is the client's to
// add. An offered server's OutputFilter that failed is
// agentworkspace.ErrOutputFilter's words alone, never the panic's value
// its error holds.
func errorOf(err error) wireError {
	switch {
	case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
		return wireError{Code: codeOther, Message: "the request ended"}
	case errors.Is(err, agentworkspace.ErrOutputFilter):
		return wireError{Code: codeOther, Message: agentworkspace.ErrOutputFilter.Error()}
	}
	for _, c := range codes {
		if errors.Is(err, c.err) {
			return wireError{Code: c.code, Message: bounded(wireerr.Message(err, c.err, c.code != codeOutside))}
		}
	}
	return wireError{Code: codeOther, Message: wireerr.Message(err, nil, false)}
}

// bounded is s cut to MaxErrorBytes and made valid UTF-8.
func bounded(s string) string { return wireerr.Bounded(s, MaxErrorBytes) }

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
