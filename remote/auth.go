package remote

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"slices"
	"strings"
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

// Authenticator resolves the principal a request is from. An error
// refuses the request as ErrUnauthenticated; its text is not sent.
type Authenticator func(r *http.Request) (Principal, error)

// BearerTokens is an Authenticator over static bearer tokens: a request
// whose Authorization header is "Bearer <token>" for a token in tokens
// is that token's principal. Tokens are compared in constant time,
// through their SHA-256, so neither their contents nor their lengths
// leak through timing. An empty token is never matched.
func BearerTokens(tokens map[string]Principal) Authenticator {
	type entry struct {
		sum [sha256.Size]byte
		p   Principal
	}
	entries := make([]entry, 0, len(tokens))
	for tok, p := range tokens {
		if tok == "" {
			continue
		}
		p.Scopes = slices.Clone(p.Scopes)
		entries = append(entries, entry{sha256.Sum256([]byte(tok)), p})
	}
	return func(r *http.Request) (Principal, error) {
		tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || tok == "" {
			return Principal{}, ErrUnauthenticated
		}
		sum := sha256.Sum256([]byte(tok))
		var found Principal
		matched := 0
		for _, e := range entries {
			if subtle.ConstantTimeCompare(sum[:], e.sum[:]) == 1 {
				found, matched = e.p, 1
			}
		}
		if matched == 0 {
			return Principal{}, ErrUnauthenticated
		}
		found.Scopes = slices.Clone(found.Scopes)
		return found, nil
	}
}

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
