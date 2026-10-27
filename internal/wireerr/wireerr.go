// Package wireerr holds what every wire's server does to an error's
// message before it crosses (RFC 0001 §15), so that remote and
// mcpworkspace cannot drift: a name inside the workspace is left
// relative to its root, any other absolute path is cut to its last
// element, and the message is bounded and made valid UTF-8.
package wireerr

import (
	"path"
	"strings"
)

// Scrub is msg with the served root cut from it, a name under it left
// relative to it and the root itself ".", and every other word that is
// an absolute path cut to its last element ("/usr/bin/tool" is
// ".../tool"). What remains is the served workspace's words and the
// last element of any path in them. root "" cuts no root.
func Scrub(root, msg string) string {
	if root != "" && root != "/" {
		msg = strings.ReplaceAll(msg, root+"/", "")
	}
	words := strings.Split(msg, " ")
	for i, w := range words {
		lead := len(w) - len(strings.TrimLeft(w, `"'(`))
		core := strings.TrimRight(w[lead:], `"'):,;`)
		switch {
		case core == root && root != "":
			words[i] = w[:lead] + "." + w[lead+len(core):]
		case len(core) > 1 && core[0] == '/':
			words[i] = w[:lead] + ".../" + path.Base(core) + w[lead+len(core):]
		}
	}
	return strings.Join(words, " ")
}

// Bounded is s cut to max bytes and made valid UTF-8.
func Bounded(s string, max int) string {
	if len(s) > max {
		s = s[:max]
	}
	return strings.ToValidUTF8(s, "?")
}
