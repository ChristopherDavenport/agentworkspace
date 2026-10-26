package remote

import (
	"context"
	"net/http"
	"slices"
)

// Scope is what a principal may do through a Handler.
type Scope string

const (
	// ScopeRead reads: the info route and every read of the files
	// (stat, lstat, readlink, readdir, a file's content).
	ScopeRead Scope = "read"
	// ScopeWrite writes and removes files.
	ScopeWrite Scope = "write"
	// ScopeExec runs anything: Exec, every session and process route,
	// and the workspace's environment, which the info route gives only
	// to a principal holding it. It is never implied by read or write.
	ScopeExec Scope = "exec"
)

// allScopes are every scope a Handler checks, the insecure principal's.
func allScopes() []Scope { return []Scope{ScopeRead, ScopeWrite, ScopeExec} }

// Principal is who a request is from, as an Authenticator resolved it,
// and what they may do. A session, and every process started in it,
// belongs to the principal's Name: another principal's request names
// no such session.
type Principal struct {
	Name   string
	Scopes []Scope
}

// Has reports whether p holds s.
func (p Principal) Has(s Scope) bool { return slices.Contains(p.Scopes, s) }

// Authenticator resolves the principal a request is from, and so what
// it may do: it is the program's authentication, which this package
// does not implement (no tokens, no certificates, no sessions of a
// login), only asks for. An error refuses the request as
// ErrUnauthenticated; its text is not sent.
type Authenticator func(r *http.Request) (Principal, error)

type principalKey struct{}

// PrincipalFrom is the principal a Handler resolved for the request
// whose context ctx is.
func PrincipalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}

// principalOf is the principal ServeHTTP put on r; the zero Principal,
// which holds no scope, if none.
func principalOf(r *http.Request) Principal {
	p, _ := PrincipalFrom(r.Context())
	return p
}
