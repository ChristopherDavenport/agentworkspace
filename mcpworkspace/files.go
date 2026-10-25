package mcpworkspace

// The workspace's files, read-only, over MCP resources: the server half
// (ServeFiles) and the client's fs.FS (NewFS).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/mcpworkspace/wire"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ServeFiles registers fsys on srv, read-only, under
// wire.FilesURITemplate. Every name goes through fsys, which should be
// confined to the workspace's root, as an agentworkspace.Workspace's FS
// is: a name that leaves it, by ".." or a link, is refused, and nothing
// is ever written. The server checks each request itself, whatever the
// client did.
func ServeFiles(srv *mcp.Server, fsys fs.FS) {
	srv.AddResourceTemplate(&mcp.ResourceTemplate{
		Name:        "workspace-files",
		Title:       "The workspace's files, read-only",
		Description: "op is stat, lstat, readdir, readlink or read; path a name relative to the workspace's root. Reads are bounded; nothing is written.",
		MIMEType:    "application/json",
		URITemplate: wire.FilesURITemplate,
	}, func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		reply := fileOp(fsys, req.Params.URI)
		data, err := json.Marshal(reply)
		if err != nil {
			return nil, err
		}
		res := &mcp.ReadResourceResult{Contents: []*mcp.ResourceContents{{URI: req.Params.URI, MIMEType: "application/json", Text: string(data)}}}
		// The files change under the session; a client keeps no copy.
		res.CacheScope = "private"
		return res, nil
	})
}

func infoOf(fi fs.FileInfo) wire.FileInfo {
	return wire.FileInfo{Name: fi.Name(), Size: fi.Size(), Mode: fi.Mode(), ModTime: fi.ModTime()}
}

// fileErrorOf is err as a reply's error.
func fileErrorOf(err error) *wire.FileError {
	kind := wire.KindOther
	switch {
	case errors.Is(err, agentworkspace.ErrOutside):
		kind = wire.KindOutside
	case errors.Is(err, fs.ErrNotExist):
		kind = wire.KindNotExist
	case errors.Is(err, fs.ErrInvalid):
		kind = wire.KindInvalid
	case errors.Is(err, fs.ErrPermission):
		kind = wire.KindPermission
	}
	msg := err.Error()
	if len(msg) > wire.MaxErrorBytes {
		msg = msg[:wire.MaxErrorBytes]
	}
	return &wire.FileError{Kind: kind, Message: strings.ToValidUTF8(msg, "?")}
}

// checkName refuses a name that is not fs.ValidPath: one that is
// absolute or climbs out with ".." leaves the workspace and is
// ErrOutside; any other is fs.ErrInvalid. The workspace's FS checks
// again; this keeps the refusal the server's whatever FS it serves.
func checkName(op, name string) error {
	if fs.ValidPath(name) {
		return nil
	}
	if c := path.Clean(name); path.IsAbs(c) || c == ".." || strings.HasPrefix(c, "../") {
		return &fs.PathError{Op: op, Path: name, Err: agentworkspace.ErrOutside}
	}
	return &fs.PathError{Op: op, Path: name, Err: fs.ErrInvalid}
}

// fileOp answers one request.
func fileOp(fsys fs.FS, uri string) wire.FileReply {
	fail := func(err error) wire.FileReply { return wire.FileReply{Error: fileErrorOf(err)} }
	op, name, err := wire.ParseFileURI(uri)
	if err != nil {
		return fail(err)
	}
	if err := checkName(op, name); err != nil {
		return fail(err)
	}
	switch op {
	case wire.OpStat, wire.OpLstat:
		stat := fs.Stat
		if op == wire.OpLstat {
			stat = fs.Lstat
		}
		fi, err := stat(fsys, name)
		if err != nil {
			return fail(err)
		}
		info := infoOf(fi)
		return wire.FileReply{Info: &info}
	case wire.OpReadDir:
		es, err := fs.ReadDir(fsys, name)
		if err != nil {
			return fail(err)
		}
		if len(es) > wire.MaxDirEntries {
			return fail(fmt.Errorf("%s holds %d entries, over the %d a listing gives", name, len(es), wire.MaxDirEntries))
		}
		out := make([]wire.FileInfo, 0, len(es))
		for _, e := range es {
			fi, err := e.Info()
			if errors.Is(err, fs.ErrNotExist) {
				continue // removed since the listing
			}
			if err != nil {
				return fail(err)
			}
			out = append(out, infoOf(fi))
		}
		return wire.FileReply{Entries: out}
	case wire.OpReadLink:
		t, err := fs.ReadLink(fsys, name)
		if err != nil {
			return fail(err)
		}
		return wire.FileReply{Target: t}
	default: // wire.OpRead
		data, err := readBounded(fsys, name)
		if err != nil {
			return fail(err)
		}
		return wire.FileReply{Data: data}
	}
}

// readBounded reads a regular file of at most wire.MaxFileBytes, opened
// through fsys, which does not block on a FIFO.
func readBounded(fsys fs.FS, name string) ([]byte, error) {
	f, err := fsys.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, &fs.PathError{Op: "read", Path: name, Err: errors.New("not a regular file")}
	}
	tooBig := func(n int64) error {
		return &fs.PathError{Op: "read", Path: name, Err: fmt.Errorf("%d bytes, over the %d-byte limit of a read", n, wire.MaxFileBytes)}
	}
	if fi.Size() > wire.MaxFileBytes {
		return nil, tooBig(fi.Size())
	}
	data, err := io.ReadAll(io.LimitReader(f, wire.MaxFileBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > wire.MaxFileBytes {
		return nil, tooBig(int64(len(data)))
	}
	return data, nil
}

// A Source sends one request for the files: it calls f with the session
// to send it on, and returns f's error, or its own when it could not
// call f. A program that dials again after a lost connection reads again
// on the new one here, since a read of the files has no effect.
type Source func(ctx context.Context, f func(*mcp.ClientSession) error) error

// SessionSource is the files of cs alone, with no reconnection.
func SessionSource(cs *mcp.ClientSession) Source {
	return func(_ context.Context, f func(*mcp.ClientSession) error) error { return f(cs) }
}

// An FSOption configures NewFS.
type FSOption func(*remoteFS)

// WithTimeout bounds each request; one that takes longer fails as a
// read error does. The default is 30 seconds.
func WithTimeout(d time.Duration) FSOption { return func(f *remoteFS) { f.timeout = d } }

// WithName is the word the file system's errors start with, such as the
// program's name for the far side ("executor"); the default is
// "mcpworkspace".
func WithName(name string) FSOption { return func(f *remoteFS) { f.name = name } }

// NewFS is the files at the other end of src as an fs.FS, read through
// wire.FilesURITemplate. It implements fs.StatFS, fs.ReadDirFS,
// fs.ReadFileFS and fs.ReadLinkFS with one request each; Open stats the
// name, and a file's contents or a directory's entries are read on the
// first Read or ReadDir. It refuses an invalid name before asking, and
// checks what the server sends back: a read over wire.MaxFileBytes and a
// listing with a name that is not one element are errors. A name that
// leaves the workspace is agentworkspace.ErrOutside, as the server's
// own file system has it.
//
// An fs.FS takes no context, so each request runs under the timeout
// alone.
func NewFS(src Source, opts ...FSOption) fs.FS {
	f := &remoteFS{src: src, timeout: 30 * time.Second, name: "mcpworkspace"}
	for _, o := range opts {
		o(f)
	}
	return f
}

// remoteFS is NewFS's file system.
type remoteFS struct {
	src     Source
	timeout time.Duration
	name    string
}

var (
	_ fs.StatFS     = (*remoteFS)(nil)
	_ fs.ReadDirFS  = (*remoteFS)(nil)
	_ fs.ReadFileFS = (*remoteFS)(nil)
	_ fs.ReadLinkFS = (*remoteFS)(nil)
)

// failure is an error of the file system's own, a reply it cannot use
// or a request that failed, under its name.
type failure struct {
	name string
	err  error
}

func (e *failure) Error() string { return e.name + ": " + e.err.Error() }
func (e *failure) Unwrap() error { return e.err }

// sent marks a request's own error on its way through the source, which
// sees through it (Unwrap) to decide whether the connection was lost.
type sent struct{ err error }

func (e *sent) Error() string { return e.err.Error() }
func (e *sent) Unwrap() error { return e.err }

func (f *remoteFS) fail(format string, args ...any) error {
	return &failure{name: f.name, err: fmt.Errorf(format, args...)}
}

// do sends op on name and returns the reply, its error mapped back.
func (f *remoteFS) do(op, name string) (wire.FileReply, error) {
	if err := checkName(op, name); err != nil {
		return wire.FileReply{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), f.timeout)
	defer cancel()
	var res *mcp.ReadResourceResult
	err := f.src(ctx, func(cs *mcp.ClientSession) error {
		var err error
		if res, err = cs.ReadResource(ctx, &mcp.ReadResourceParams{URI: wire.FileURI(op, name)}); err != nil {
			return &sent{err}
		}
		return nil
	})
	var s *sent
	switch {
	case err == nil:
	case errors.As(err, &s):
		return wire.FileReply{}, &fs.PathError{Op: op, Path: name, Err: &failure{name: f.name, err: s.err}}
	default:
		// The source's own: it could not send the request.
		return wire.FileReply{}, &fs.PathError{Op: op, Path: name, Err: err}
	}
	if len(res.Contents) != 1 || res.Contents[0] == nil {
		return wire.FileReply{}, &fs.PathError{Op: op, Path: name, Err: f.fail("%d contents in the reply, want 1", len(res.Contents))}
	}
	var r wire.FileReply
	if err := json.Unmarshal([]byte(res.Contents[0].Text), &r); err != nil {
		return wire.FileReply{}, &fs.PathError{Op: op, Path: name, Err: f.fail("the reply: %w", err)}
	}
	if e := r.Error; e != nil {
		return wire.FileReply{}, &fs.PathError{Op: op, Path: name, Err: f.kindErr(e)}
	}
	return r, nil
}

// kindErr is a reply's error as this side's: its kind's sentinel,
// wrapped with the server's message, which is shown as its words.
func (f *remoteFS) kindErr(e *wire.FileError) error {
	msg := e.Message
	if len(msg) > wire.MaxErrorBytes {
		msg = msg[:wire.MaxErrorBytes]
	}
	msg = strings.ToValidUTF8(msg, "?")
	var sentinel error
	switch e.Kind {
	case wire.KindNotExist:
		sentinel = fs.ErrNotExist
	case wire.KindOutside:
		sentinel = agentworkspace.ErrOutside
	case wire.KindInvalid:
		sentinel = fs.ErrInvalid
	case wire.KindPermission:
		sentinel = fs.ErrPermission
	default:
		return f.fail("%s", msg)
	}
	return &remoteErr{name: f.name, sentinel: sentinel, msg: msg}
}

// remoteErr is an error the server gave: its words, and the sentinel
// its kind maps to.
type remoteErr struct {
	name     string
	sentinel error
	msg      string
}

func (e *remoteErr) Error() string { return e.name + ": " + e.msg }
func (e *remoteErr) Unwrap() error { return e.sentinel }

// info is a reply's FileInfo as an fs.FileInfo named name.
func (f *remoteFS) info(r wire.FileReply, op, name string) (fs.FileInfo, error) {
	if r.Info == nil {
		return nil, &fs.PathError{Op: op, Path: name, Err: f.fail("no info in the reply")}
	}
	i := *r.Info
	i.Name = path.Base(name)
	return remoteInfo{i}, nil
}

func (f *remoteFS) Stat(name string) (fs.FileInfo, error) {
	r, err := f.do(wire.OpStat, name)
	if err != nil {
		return nil, err
	}
	return f.info(r, wire.OpStat, name)
}

func (f *remoteFS) Lstat(name string) (fs.FileInfo, error) {
	r, err := f.do(wire.OpLstat, name)
	if err != nil {
		return nil, err
	}
	return f.info(r, wire.OpLstat, name)
}

func (f *remoteFS) ReadLink(name string) (string, error) {
	r, err := f.do(wire.OpReadLink, name)
	if err != nil {
		return "", err
	}
	if r.Target == "" {
		return "", &fs.PathError{Op: wire.OpReadLink, Path: name, Err: f.fail("no target in the reply")}
	}
	return r.Target, nil
}

// ReadDir lists name, sorted by name. An entry whose name is not one
// element of a path ("", ".", "..", or one with a slash) fails it,
// since a walk would join it into a name somewhere else.
func (f *remoteFS) ReadDir(name string) ([]fs.DirEntry, error) {
	r, err := f.do(wire.OpReadDir, name)
	if err != nil {
		return nil, err
	}
	if len(r.Entries) > wire.MaxDirEntries {
		return nil, &fs.PathError{Op: wire.OpReadDir, Path: name, Err: f.fail("%d entries, over %d", len(r.Entries), wire.MaxDirEntries)}
	}
	out := make([]fs.DirEntry, 0, len(r.Entries))
	seen := map[string]bool{}
	for _, e := range r.Entries {
		if e.Name == "." || !fs.ValidPath(e.Name) || strings.Contains(e.Name, "/") || seen[e.Name] {
			return nil, &fs.PathError{Op: wire.OpReadDir, Path: name, Err: f.fail("the listing names %q", e.Name)}
		}
		seen[e.Name] = true
		out = append(out, fs.FileInfoToDirEntry(remoteInfo{e}))
	}
	slices.SortFunc(out, func(a, b fs.DirEntry) int { return strings.Compare(a.Name(), b.Name()) })
	return out, nil
}

func (f *remoteFS) ReadFile(name string) ([]byte, error) {
	r, err := f.do(wire.OpRead, name)
	if err != nil {
		return nil, err
	}
	if len(r.Data) > wire.MaxFileBytes {
		return nil, &fs.PathError{Op: wire.OpRead, Path: name, Err: f.fail("%d bytes, over the %d-byte limit", len(r.Data), wire.MaxFileBytes)}
	}
	return r.Data, nil
}

// Open stats name, following links inside the workspace; the file
// reads its contents or entries when first asked.
func (f *remoteFS) Open(name string) (fs.File, error) {
	fi, err := f.Stat(name)
	if err != nil {
		var pe *fs.PathError
		if errors.As(err, &pe) {
			pe.Op = "open"
		}
		return nil, err
	}
	return &remoteFile{fs: f, name: name, info: fi}, nil
}

// remoteInfo is a FileInfo as an fs.FileInfo.
type remoteInfo struct{ i wire.FileInfo }

func (r remoteInfo) Name() string       { return r.i.Name }
func (r remoteInfo) Size() int64        { return r.i.Size }
func (r remoteInfo) Mode() fs.FileMode  { return r.i.Mode }
func (r remoteInfo) ModTime() time.Time { return r.i.ModTime }
func (r remoteInfo) IsDir() bool        { return r.i.Mode.IsDir() }
func (r remoteInfo) Sys() any           { return nil }

// remoteFile is an opened name: its Stat, and its contents or entries
// read on first use.
type remoteFile struct {
	fs   *remoteFS
	name string
	info fs.FileInfo

	data    *strings.Reader
	entries []fs.DirEntry
	listed  bool
	closed  bool
}

func (f *remoteFile) Stat() (fs.FileInfo, error) {
	if f.closed {
		return nil, &fs.PathError{Op: "stat", Path: f.name, Err: fs.ErrClosed}
	}
	return f.info, nil
}

func (f *remoteFile) Read(p []byte) (int, error) {
	switch {
	case f.closed:
		return 0, &fs.PathError{Op: "read", Path: f.name, Err: fs.ErrClosed}
	case f.info.IsDir():
		return 0, &fs.PathError{Op: "read", Path: f.name, Err: errors.New("is a directory")}
	}
	if f.data == nil {
		data, err := f.fs.ReadFile(f.name)
		if err != nil {
			return 0, err
		}
		f.data = strings.NewReader(string(data))
	}
	return f.data.Read(p)
}

func (f *remoteFile) ReadDir(n int) ([]fs.DirEntry, error) {
	switch {
	case f.closed:
		return nil, &fs.PathError{Op: "readdir", Path: f.name, Err: fs.ErrClosed}
	case !f.info.IsDir():
		return nil, &fs.PathError{Op: "readdir", Path: f.name, Err: errors.New("not a directory")}
	}
	if !f.listed {
		es, err := f.fs.ReadDir(f.name)
		if err != nil {
			return nil, err
		}
		f.entries, f.listed = es, true
	}
	if n <= 0 {
		out := f.entries
		f.entries = nil
		return out, nil
	}
	if len(f.entries) == 0 {
		return nil, io.EOF
	}
	n = min(n, len(f.entries))
	out := f.entries[:n:n]
	f.entries = f.entries[n:]
	return out, nil
}

func (f *remoteFile) Close() error {
	if f.closed {
		return &fs.PathError{Op: "close", Path: f.name, Err: fs.ErrClosed}
	}
	f.closed = true
	return nil
}
