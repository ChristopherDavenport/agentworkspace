// Package wireerr holds what every wire's server sends of a refusal
// (RFC 0001 §15), so that remote and mcpworkspace cannot drift. It is
// an allow-list: a refusal crosses as its kind and that kind's fixed
// words, the system's fixed words for an error number, or, for a
// refusal of no kind, Other; never the served error's own text, which
// may name a path on the serving side's machine in any form (after a
// newline, as key=/path, as a file:// URL, with spaces in it), so no
// filter over that text could be trusted to catch it.
package wireerr

import (
	"context"
	"errors"
	"strings"
	"syscall"
)

// Other is the message of a refusal of no kind.
const Other = "the workspace refused the operation"

// Message is what of err crosses a wire, err being of the kind sentinel
// stands for (nil for none): sentinel's words, the system's words for
// the error number err holds (but for the kinds whose words must stay
// as they are, where errnoOK is false), a context's error's own words,
// and Other for anything else.
func Message(err, sentinel error, errnoOK bool) string {
	var errno syscall.Errno
	hasErrno := errors.As(err, &errno)
	switch {
	case sentinel != nil && hasErrno && errnoOK:
		return errno.Error()
	case sentinel != nil:
		return sentinel.Error()
	case errors.Is(err, context.Canceled):
		return context.Canceled.Error()
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded.Error()
	case hasErrno:
		return errno.Error()
	}
	return Other
}

// Bounded is s cut to max bytes and made valid UTF-8.
func Bounded(s string, max int) string {
	if len(s) > max {
		s = s[:max]
	}
	return strings.ToValidUTF8(s, "?")
}
