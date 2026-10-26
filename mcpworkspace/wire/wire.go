// Package wire is the protocol package mcpworkspace speaks over an MCP
// connection, as plain JSON shapes and names: the process methods, the
// file resources and their bounds. It imports the standard library
// alone, so a program that only relays or answers the methods (a host
// that sits between a client and a server and answers some of the
// processes itself) can name them without the MCP SDK.
//
// # Processes
//
// A process that lives longer than one call, with pipes to it, is
// started and driven by custom JSON-RPC methods, not tools: a tool would
// be offered to the model of any harness connected to the server, and
// starting a process is arbitrary execution. The methods are:
//
//	agentworkspace/process.start      {args, dir?, env?, timeoutMs?, stderr?} -> {id}
//	agentworkspace/process.write      {id, data (base64, at most MaxWriteBytes)} -> {}
//	agentworkspace/process.read       {id, fd: 1|2, max?} -> {data?, eof?}
//	agentworkspace/process.closeStdin {id} -> {}
//	agentworkspace/process.signal     {id, signal: INT|TERM|KILL} -> {done?}
//	agentworkspace/process.wait       {id} -> {status, ended?, error?}
//	agentworkspace/process.close      {id} -> {status, ended?, error?, stderr?}
//
// A read is a long poll: it returns as soon as there is output, at most
// max bytes (at most MaxReadBytes), or end of file, and waits otherwise.
// A wait waits for the process to exit. A close ends the process and
// gives back the standard error no read took. Each MCP session has its
// own table of processes, at most MaxProcesses open at once: an id
// another session started is refused as one that does not exist. When
// the session ends, every process it started ends with it.
//
// # Servers
//
// A workspace may offer MCP servers of its own (agentworkspace's
// ServerProvider), each a stdio server it starts by name:
//
//	agentworkspace/servers.list  {} -> {servers: [{name, description?}]}
//	agentworkspace/servers.start {name, stderr?} -> {id}
//
// A started server is a process like any other: its id takes the
// process methods, its standard input and output carry the MCP stream,
// and it counts toward MaxProcesses in its session. A name the
// workspace does not offer, agentworkspace.ValidServerName's refusals
// among them, is refused with CodeNoServer. An MCP server whose
// workspace offers none need not answer the methods, and a client takes
// a method not found as no servers.
//
// # Files
//
// The workspace's files are served read-only under one resource
// template:
//
//	agentworkspace:///{op}{?path}
//
// op is stat, lstat, readdir, readlink or read, and path a name in the
// workspace's file system (fs.ValidPath: relative to its root, slash
// separated, "." for the root), percent-encoded. A read gives at most
// MaxFileBytes of a regular file and refuses a larger one, a directory,
// a FIFO or a device; a listing gives at most MaxDirEntries entries and
// refuses a longer one. The reply is one application/json text content,
// a FileReply; an error is in it, with a kind the client maps back to a
// sentinel.
package wire

import (
	"fmt"
	"io/fs"
	"net/url"
	"slices"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"
)

// MethodPrefix begins every process method.
const MethodPrefix = "agentworkspace/process."

// The process methods.
const (
	MethodStart      = MethodPrefix + "start"
	MethodWrite      = MethodPrefix + "write"
	MethodRead       = MethodPrefix + "read"
	MethodCloseStdin = MethodPrefix + "closeStdin"
	MethodSignal     = MethodPrefix + "signal"
	MethodWait       = MethodPrefix + "wait"
	MethodClose      = MethodPrefix + "close"
)

// Methods are the process methods, in the order above, as a new slice
// each call.
func Methods() []string {
	return []string{MethodStart, MethodWrite, MethodRead, MethodCloseStdin, MethodSignal, MethodWait, MethodClose}
}

// ServersPrefix begins every server method.
const ServersPrefix = "agentworkspace/servers."

// The server methods.
const (
	MethodServersList  = ServersPrefix + "list"
	MethodServersStart = ServersPrefix + "start"
)

// ServerMethods are the server methods, in the order above, as a new
// slice each call.
func ServerMethods() []string { return []string{MethodServersList, MethodServersStart} }

// ServerInfo is one server servers.list names.
type ServerInfo struct {
	// Name is agentworkspace.ValidServerName's.
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// CodeNoServer is the JSON-RPC error code of a servers.start refused
// because the workspace offers no server by that name (and none can be
// called a name agentworkspace.ValidServerName refuses), in the range
// JSON-RPC leaves to an implementation's server errors.
const CodeNoServer = -32010

// ServerList is servers.list's result.
type ServerList struct {
	Servers []ServerInfo `json:"servers"`
}

// ServerStartArgs is servers.start's parameters; its result is a
// Started.
type ServerStartArgs struct {
	Name string `json:"name"`
	// Stderr asks for the server's standard error, read as fd 2;
	// without it, it is discarded.
	Stderr bool `json:"stderr,omitempty"`
}

// The bounds of the process methods: processes open at once in one
// session, bytes in one write, and bytes in one read's reply.
const (
	MaxProcesses  = 32
	MaxWriteBytes = 1 << 20
	MaxReadBytes  = 64 << 10
)

// Bounds are the process methods' bounds a server keeps, which it says
// to its clients (in a capability of the server's own) so that a client
// sends no more: what a server keeps may be less than the constants.
type Bounds struct {
	MaxProcesses  int `json:"maxProcesses"`
	MaxWriteBytes int `json:"maxWriteBytes"`
	MaxReadBytes  int `json:"maxReadBytes"`
}

// DefaultBounds are the constants as Bounds.
func DefaultBounds() Bounds {
	return Bounds{MaxProcesses: MaxProcesses, MaxWriteBytes: MaxWriteBytes, MaxReadBytes: MaxReadBytes}
}

// Valid reports whether every bound is positive.
func (b Bounds) Valid() bool { return b.MaxProcesses > 0 && b.MaxWriteBytes > 0 && b.MaxReadBytes > 0 }

// The signals a process may be sent, by name.
const (
	SignalINT  = "INT"
	SignalTERM = "TERM"
	SignalKILL = "KILL"
)

// signals are the signals a process may be sent, by name. Unexported,
// so that what a server accepts cannot be changed by anyone who imports
// the package.
var signals = map[string]syscall.Signal{SignalINT: syscall.SIGINT, SignalTERM: syscall.SIGTERM, SignalKILL: syscall.SIGKILL}

// SignalOf is the signal named name on the wire, false for a name that
// is not INT, TERM or KILL.
func SignalOf(name string) (syscall.Signal, bool) {
	s, ok := signals[name]
	return s, ok
}

// SignalName is sig's name on the wire, false for a signal that cannot
// be sent.
func SignalName(sig syscall.Signal) (string, bool) {
	for n, s := range signals {
		if s == sig {
			return n, true
		}
	}
	return "", false
}

// The values of Wait's Ended.
const (
	// EndedTimeout is a process the start's timeout ended.
	EndedTimeout = "timeout"
	// EndedCanceled is a process the session's end, or the server's,
	// ended.
	EndedCanceled = "canceled"
)

// StartArgs is process.start's parameters.
type StartArgs struct {
	Args      []string `json:"args"`
	Dir       string   `json:"dir,omitempty"`
	Env       []string `json:"env,omitempty"`
	TimeoutMs int64    `json:"timeoutMs,omitempty"`
	// Stderr asks for the process's standard error, read as fd 2;
	// without it, it is discarded.
	Stderr bool `json:"stderr,omitempty"`
}

// Started is process.start's result.
type Started struct {
	ID string `json:"id"`
}

// ProcessID is the parameters of the methods that name a process
// alone.
type ProcessID struct {
	ID string `json:"id"`
}

// WriteArgs is process.write's parameters.
type WriteArgs struct {
	ID   string `json:"id"`
	Data []byte `json:"data"`
}

// ReadArgs is process.read's parameters.
type ReadArgs struct {
	ID  string `json:"id"`
	FD  int    `json:"fd"`
	Max int    `json:"max,omitempty"`
}

// ReadReply is process.read's result.
type ReadReply struct {
	Data []byte `json:"data,omitempty"`
	EOF  bool   `json:"eof,omitempty"`
}

// SignalArgs is process.signal's parameters.
type SignalArgs struct {
	ID     string `json:"id"`
	Signal string `json:"signal"`
}

// Signaled is process.signal's result.
type Signaled struct {
	// Done says the process had already exited.
	Done bool `json:"done,omitempty"`
}

// Exit is process.wait's and process.close's result.
type Exit struct {
	Status int `json:"status"`
	// Ended is EndedTimeout or EndedCanceled when one of those ended it.
	Ended string `json:"ended,omitempty"`
	Error string `json:"error,omitempty"`
	// Stderr is, from a close, the standard error no read took.
	Stderr []byte `json:"stderr,omitempty"`
}

// FilesScheme is the URI scheme of the workspace's files.
const FilesScheme = "agentworkspace"

// FilesURITemplate is the resource template the workspace's files are
// served under.
const FilesURITemplate = FilesScheme + ":///{op}{?path}"

// MaxFileBytes bounds one read of a file.
const MaxFileBytes = 1 << 20

// MaxDirEntries bounds one listing, so a reply stays a few megabytes.
const MaxDirEntries = 10000

// MaxErrorBytes bounds a FileError's message, which is the server's
// text.
const MaxErrorBytes = 512

// The operations of FilesURITemplate.
const (
	OpStat     = "stat"
	OpLstat    = "lstat"
	OpReadDir  = "readdir"
	OpReadLink = "readlink"
	OpRead     = "read"
)

// ops are the operations of FilesURITemplate.
var ops = []string{OpStat, OpLstat, OpReadDir, OpReadLink, OpRead}

// Ops are the operations of FilesURITemplate, as a new slice each call.
func Ops() []string { return slices.Clone(ops) }

// The kinds of a FileError.
const (
	KindNotExist   = "notexist"
	KindOutside    = "outside"
	KindInvalid    = "invalid"
	KindPermission = "permission"
	KindOther      = "other"
)

// FileReply is the JSON of one reply: the field of its op, or Error.
type FileReply struct {
	Info    *FileInfo  `json:"info,omitempty"`    // stat, lstat
	Entries []FileInfo `json:"entries,omitempty"` // readdir, as lstat describes each
	Target  string     `json:"target,omitempty"`  // readlink
	Data    []byte     `json:"data,omitempty"`    // read, base64
	Error   *FileError `json:"error,omitempty"`
}

// FileInfo is an fs.FileInfo on the wire; Mode is Go's fs.FileMode.
type FileInfo struct {
	Name    string      `json:"name"`
	Size    int64       `json:"size"`
	Mode    fs.FileMode `json:"mode"`
	ModTime time.Time   `json:"modTime"`
}

// FileError is a refusal: Kind is one of the Kind constants, Message the
// server's words.
type FileError struct {
	Kind    string `json:"kind"`
	Message string `json:"message"`
}

// FileURI is the URI of op on name, every byte outside RFC 3986's
// unreserved set percent-encoded, as the template's {?path} matches.
func FileURI(op, name string) string {
	var b strings.Builder
	b.WriteString(FilesScheme + ":///" + op + "?path=")
	for i := 0; i < len(name); i++ {
		c := name[i]
		if 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' || c == '-' || c == '.' || c == '_' || c == '~' {
			b.WriteByte(c)
			continue
		}
		fmt.Fprintf(&b, "%%%02X", c)
	}
	return b.String()
}

// ParseFileURI is a request's op and name. A URI with anything else in
// it is fs.ErrInvalid.
func ParseFileURI(uri string) (op, name string, err error) {
	bad := func(why string) (string, string, error) {
		return "", "", &fs.PathError{Op: "resource", Path: uri, Err: fmt.Errorf("%w: %s", fs.ErrInvalid, why)}
	}
	u, err := url.Parse(uri)
	if err != nil {
		return bad(err.Error())
	}
	if u.Scheme != FilesScheme || u.Host != "" || u.User != nil || u.Fragment != "" || u.Opaque != "" {
		return bad("not a " + FilesURITemplate + " URI")
	}
	op = strings.TrimPrefix(u.Path, "/")
	if !slices.Contains(ops, op) {
		return bad(fmt.Sprintf("op %q: want one of %s", op, strings.Join(ops, ", ")))
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return bad(err.Error())
	}
	if len(q) != 1 || len(q["path"]) != 1 {
		return bad("want one path and nothing else")
	}
	name = q["path"][0]
	if !utf8.ValidString(name) {
		return bad("the path is not UTF-8")
	}
	return op, name, nil
}
