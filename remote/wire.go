package remote

// The wire: the routes, the JSON shapes, the bounds and the signals.

import (
	"fmt"
	"io/fs"
	"path"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/ChristopherDavenport/agentworkspace"
)

// The defaults of the bounds a Handler keeps, each changed by its
// option.
const (
	// DefaultLease is how long a session lives unrenewed (WithLease).
	// A client renews it every third of that.
	DefaultLease = 30 * time.Second
	// DefaultMaxSessions bounds the sessions open at once on one
	// Handler (WithMaxSessions).
	DefaultMaxSessions = 64
	// DefaultMaxSessionsPerPrincipal bounds the sessions one principal
	// has open at once (WithMaxSessionsPerPrincipal).
	DefaultMaxSessionsPerPrincipal = 8
	// DefaultMaxProcesses bounds the processes open at once in one
	// session (WithMaxProcesses).
	DefaultMaxProcesses = 32
	// DefaultMaxWriteBytes bounds a WriteFile's data and an Exec's
	// Command.Stdin (WithMaxWriteBytes).
	DefaultMaxWriteBytes = 32 << 20
	// DefaultMaxDirEntries bounds one listing (WithMaxDirEntries).
	DefaultMaxDirEntries = 10000
	// DefaultMaxExecOutputBytes bounds each of the standard output and
	// the standard error an Exec without a Stream sends back
	// (WithMaxExecOutputBytes).
	DefaultMaxExecOutputBytes = 16 << 20
	// DefaultMaxTransfers bounds the Execs and the reads and writes of a
	// file's content served at once (WithMaxTransfers).
	DefaultMaxTransfers = 256
)

// The bounds that are not options.
const (
	// MaxRequestBytes bounds the JSON body of every route that takes
	// one, and, added to the base64 of the write bound, an Exec's: room
	// for a command line as long as Linux takes (ARG_MAX, 2 MiB) and
	// its environment, escaped.
	MaxRequestBytes = 4 << 20
	// MaxErrorBytes bounds an error's message on the wire.
	MaxErrorBytes = 512
	// StderrBytes is how much of a started process's standard error a
	// server holds that its client has not read yet; what does not fit
	// is dropped, so that the process never waits on a slow client's
	// Command.Stream.
	StderrBytes = 256 << 10
	// MaxReplyBytes bounds a JSON reply a client reads but Exec's
	// frames, each of which the client bounds by what the server says
	// it sends: a data frame at most a chunk, the Output twice the
	// base64 of the server's output bound.
	MaxReplyBytes = 16 << 20
)

// frameChunk bounds the data one of Exec's data frames carries.
const frameChunk = 32 << 10

// b64 is the length of n bytes in base64.
func b64(n int64) int64 { return (n + 2) / 3 * 4 }

// frameLimit bounds one of Exec's frames, for a server whose output
// bound is maxOutput: the Output frame's two streams, base64, and room
// for the rest of a frame.
func frameLimit(maxOutput int64) int64 { return 2*b64(maxOutput) + 64<<10 }

// The routes, under the base URL. A name is the query's path, a
// session's id and a process's are path elements.
const (
	routeInfo     = "/v1/info"
	routeStat     = "/v1/fs/stat"
	routeLstat    = "/v1/fs/lstat"
	routeReadLink = "/v1/fs/readlink"
	routeReadDir  = "/v1/fs/readdir"
	routeFile     = "/v1/fs/file"
	routeExec     = "/v1/exec"
	routeSessions = "/v1/sessions"
)

// descriptor is an agentworkspace.Descriptor on the wire.
type descriptor struct {
	Kind string `json:"kind"`
	Ref  string `json:"ref,omitempty"`
	Root string `json:"root"`
}

func descriptorOf(d agentworkspace.Descriptor) descriptor {
	return descriptor{Kind: d.Kind, Ref: d.Ref, Root: d.Root}
}

func (d descriptor) workspace() agentworkspace.Descriptor {
	return agentworkspace.Descriptor{Kind: d.Kind, Ref: d.Ref, Root: d.Root}
}

// limits are the bounds a server keeps, as the info route says them,
// so that a client sends no more.
type limits struct {
	LeaseMs        int64 `json:"leaseMs"`
	MaxProcesses   int   `json:"maxProcesses"`
	MaxWriteBytes  int64 `json:"maxWriteBytes"`
	MaxOutputBytes int64 `json:"maxOutputBytes"`
	MaxDirEntries  int   `json:"maxDirEntries"`
}

func (l limits) valid() bool {
	return l.LeaseMs > 0 && l.MaxProcesses > 0 && l.MaxWriteBytes > 0 && l.MaxOutputBytes > 0 && l.MaxDirEntries > 0
}

// infoReply is the info route's: the served workspace and what it, and
// the principal asking, can do.
type infoReply struct {
	Root   string     `json:"root"`
	Served descriptor `json:"served"`
	// Start says the served workspace is an agentworkspace.Starter.
	Start bool `json:"start"`
	// Links says its FS is an fs.ReadLinkFS.
	Links  bool     `json:"links"`
	Scopes []Scope  `json:"scopes"`
	Env    []string `json:"env,omitempty"` // only to a principal with exec
	Limits limits   `json:"limits"`
}

// fileInfo is an fs.FileInfo on the wire. Raw is the name's bytes when
// they are not UTF-8, which a JSON string cannot hold.
type fileInfo struct {
	Name    string      `json:"name"`
	Raw     []byte      `json:"raw,omitempty"`
	Size    int64       `json:"size"`
	Mode    fs.FileMode `json:"mode"`
	ModTime time.Time   `json:"modTime"`
}

func infoOf(fi fs.FileInfo) fileInfo {
	i := fileInfo{Name: fi.Name(), Size: fi.Size(), Mode: fi.Mode(), ModTime: fi.ModTime()}
	if !utf8.ValidString(i.Name) {
		i.Name, i.Raw = "", []byte(fi.Name())
	}
	return i
}

func (i fileInfo) name() string {
	if i.Raw != nil {
		return string(i.Raw)
	}
	return i.Name
}

// dirReply is readdir's: each entry as lstat describes it.
type dirReply struct {
	Entries []fileInfo `json:"entries"`
}

// linkReply is readlink's; Raw as fileInfo's.
type linkReply struct {
	Target string `json:"target"`
	Raw    []byte `json:"raw,omitempty"`
}

func (l linkReply) target() string {
	if l.Raw != nil {
		return string(l.Raw)
	}
	return l.Target
}

// sessionReply is a session's opening and renewal.
type sessionReply struct {
	ID      string `json:"id,omitempty"`
	LeaseMs int64  `json:"leaseMs"`
}

// execRequest is Exec's body. Stdin is null for none, as
// Command.Stdin's nil is.
type execRequest struct {
	Args      []string `json:"args"`
	Dir       string   `json:"dir,omitempty"`
	Env       []string `json:"env,omitempty"`
	Stdin     []byte   `json:"stdin"`
	TimeoutMs int64    `json:"timeoutMs,omitempty"`
	// Stream asks for the output as it arrives, in data frames, and
	// none in the Output.
	Stream bool `json:"stream,omitempty"`
}

// execFrame is one line of Exec's NDJSON reply: Command.Stream's
// output as it arrives, then one Output or one Error, which is the
// last.
type execFrame struct {
	Data   []byte      `json:"data,omitempty"`
	Output *execOutput `json:"output,omitempty"`
	Error  *wireError  `json:"error,omitempty"`
}

type execOutput struct {
	Stdout   []byte `json:"stdout,omitempty"`
	Stderr   []byte `json:"stderr,omitempty"`
	ExitCode int    `json:"exitCode"`
	TimedOut bool   `json:"timedOut,omitempty"`
}

// startRequest is a process's start.
type startRequest struct {
	Args      []string `json:"args"`
	Dir       string   `json:"dir,omitempty"`
	Env       []string `json:"env,omitempty"`
	TimeoutMs int64    `json:"timeoutMs,omitempty"`
	// Stderr asks for standard error, on the stderr route; without it,
	// it is discarded.
	Stderr bool `json:"stderr,omitempty"`
	// Key is the client's name for the start, which becomes the
	// process's id: a start repeated with the same key is answered
	// with the same process, and a client whose reply was lost closes
	// the process by it.
	Key string `json:"key,omitempty"`
}

// validKey reports whether key can name a start: an id, long enough
// that the client chose it at random.
func validKey(key string) bool { return len(key) >= 16 && validID(key) }

type startedReply struct {
	ID string `json:"id"`
}

type signalRequest struct {
	Signal string `json:"signal"`
}

// The values of exitReply's Ended.
const (
	endedTimeout  = "timeout"  // Command.Timeout ended it
	endedCanceled = "canceled" // its context did: a cancel, the session's end
)

// exitReply is how a process ended: wait's and close's.
type exitReply struct {
	Status int    `json:"status"`
	Ended  string `json:"ended,omitempty"`
	Error  string `json:"error,omitempty"`
}

// millis is d in whole milliseconds, rounded up, so that a timeout
// under one is not sent as none.
func millis(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64((d + time.Millisecond - 1) / time.Millisecond)
}

// signals are the signals a process may be sent, by name. The wire
// carries names, not numbers: a number is the client's platform's, and
// USR1 is 10 on Linux and 30 on macOS.
var signals = map[string]syscall.Signal{
	"HUP":   syscall.SIGHUP,
	"INT":   syscall.SIGINT,
	"QUIT":  syscall.SIGQUIT,
	"KILL":  syscall.SIGKILL,
	"USR1":  syscall.SIGUSR1,
	"USR2":  syscall.SIGUSR2,
	"PIPE":  syscall.SIGPIPE,
	"ALRM":  syscall.SIGALRM,
	"TERM":  syscall.SIGTERM,
	"CONT":  syscall.SIGCONT,
	"STOP":  syscall.SIGSTOP,
	"TSTP":  syscall.SIGTSTP,
	"WINCH": syscall.SIGWINCH,
}

// signalName is sig's name on the wire, false for one that cannot be
// sent.
func signalName(sig syscall.Signal) (string, bool) {
	for n, s := range signals {
		if s == sig {
			return n, true
		}
	}
	return "", false
}

// validID reports whether id can be a session's or a process's: the
// server's ids are rand.Text's, and a client puts nothing else in a
// path.
func validID(id string) bool {
	if id == "" || len(id) > 64 {
		return false
	}
	for _, c := range id {
		if !('a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9') {
			return false
		}
	}
	return true
}

// valid refuses a name that is not fs.ValidPath, as Local does: one
// that is absolute or climbs out with ".." leaves, and is
// agentworkspace.ErrOutside; any other is fs.ErrInvalid. Both halves
// check, so the refusal is the same whatever FS is served.
func valid(op, name string) error {
	if fs.ValidPath(name) {
		return nil
	}
	if c := path.Clean(name); path.IsAbs(c) || c == ".." || strings.HasPrefix(c, "../") {
		return fmt.Errorf("%w: %s", agentworkspace.ErrOutside, name)
	}
	return &fs.PathError{Op: op, Path: name, Err: fs.ErrInvalid}
}
