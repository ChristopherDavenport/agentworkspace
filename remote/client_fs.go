package remote

// The client's file system: the served workspace's files, read over
// the wire.

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strings"
	"time"
)

// FS reads the served workspace's files. It implements fs.StatFS and
// fs.ReadDirFS, and fs.ReadLinkFS when the served FS does: one that
// cannot tell a link from what it leads to stays a plain FS here too,
// so a check that must follow links asks instead of allowing. Open
// stats the name, following links inside the workspace, and never
// blocks on what is there: a regular file's content is streamed on its
// first Read, and a FIFO or a device opens, its Stat answers, and a
// Read of it is an error. Each request but a content's stream is
// bounded by WithTimeout.
func (c *Client) FS() fs.FS {
	f := clientFS{c}
	if c.info.Links {
		return linkFS{f}
	}
	return f
}

type clientFS struct{ c *Client }

// linkFS is clientFS over a served FS that can read links.
type linkFS struct{ clientFS }

var (
	_ fs.StatFS     = clientFS{}
	_ fs.ReadDirFS  = clientFS{}
	_ fs.ReadLinkFS = linkFS{}
)

// get asks route about name and decodes the reply into v.
func (f clientFS) get(op, name, route string, v any) error {
	if err := valid(op, name); err != nil {
		return err
	}
	if f.c.isClosed() {
		return &fs.PathError{Op: op, Path: name, Err: fs.ErrClosed}
	}
	ctx, cancel := context.WithTimeout(f.c.ctx, f.c.timeout)
	defer cancel()
	if err := f.c.call(ctx, http.MethodGet, route, url.Values{"path": {name}}, nil, v); err != nil {
		return &fs.PathError{Op: op, Path: name, Err: err}
	}
	return nil
}

func (f clientFS) Stat(name string) (fs.FileInfo, error) {
	var i fileInfo
	if err := f.get("stat", name, routeStat, &i); err != nil {
		return nil, err
	}
	i.Name, i.Raw = path.Base(name), nil
	return info{i}, nil
}

func (f linkFS) Lstat(name string) (fs.FileInfo, error) {
	var i fileInfo
	if err := f.get("lstat", name, routeLstat, &i); err != nil {
		return nil, err
	}
	i.Name, i.Raw = path.Base(name), nil
	return info{i}, nil
}

func (f linkFS) ReadLink(name string) (string, error) {
	var l linkReply
	if err := f.get("readlink", name, routeReadLink, &l); err != nil {
		return "", err
	}
	if l.target() == "" {
		return "", &fs.PathError{Op: "readlink", Path: name, Err: errors.New("remote: no target in the reply")}
	}
	return l.target(), nil
}

// ReadDir lists name, sorted by name, each entry as Lstat describes it.
// An entry whose name is not one element of a path ("", ".", "..", or
// one with a slash), or one named twice, fails it, since a walk would
// join it into a name somewhere else. A name that is not UTF-8 is
// listed as the served FS lists it, and is no fs.ValidPath there
// either.
func (f clientFS) ReadDir(name string) ([]fs.DirEntry, error) {
	var d dirReply
	if err := f.get("readdir", name, routeReadDir, &d); err != nil {
		return nil, err
	}
	bad := func(why string) error {
		return &fs.PathError{Op: "readdir", Path: name, Err: errors.New("remote: " + why)}
	}
	if len(d.Entries) > f.c.info.Limits.MaxDirEntries {
		return nil, bad("more entries than the server's bound")
	}
	out := make([]fs.DirEntry, 0, len(d.Entries))
	seen := make(map[string]bool, len(d.Entries))
	for _, e := range d.Entries {
		n := e.name()
		if n == "" || n == "." || n == ".." || strings.Contains(n, "/") || seen[n] {
			return nil, bad("the listing names " + strings.ToValidUTF8(n, "?"))
		}
		seen[n] = true
		e.Name, e.Raw = n, nil
		out = append(out, fs.FileInfoToDirEntry(info{e}))
	}
	slices.SortFunc(out, func(a, b fs.DirEntry) int { return strings.Compare(a.Name(), b.Name()) })
	return out, nil
}

// Open stats name; the file reads its content or its entries when
// first asked.
func (f clientFS) Open(name string) (fs.File, error) {
	fi, err := f.Stat(name)
	if err != nil {
		var pe *fs.PathError
		if errors.As(err, &pe) {
			pe.Op = "open"
		}
		return nil, err
	}
	return &file{fs: f, name: name, info: fi}, nil
}

// info is a fileInfo as an fs.FileInfo.
type info struct{ i fileInfo }

func (r info) Name() string       { return r.i.Name }
func (r info) Size() int64        { return r.i.Size }
func (r info) Mode() fs.FileMode  { return r.i.Mode }
func (r info) ModTime() time.Time { return r.i.ModTime }
func (r info) IsDir() bool        { return r.i.Mode.IsDir() }
func (r info) Sys() any           { return nil }

// file is an opened name: its Stat, and its content or entries read on
// first use. Like most fs.Files it is for one goroutine at a time.
type file struct {
	fs   clientFS
	name string
	info fs.FileInfo

	body    io.ReadCloser
	stop    context.CancelFunc
	entries []fs.DirEntry
	listed  bool
	closed  bool
}

func (f *file) Stat() (fs.FileInfo, error) {
	if f.closed {
		return nil, &fs.PathError{Op: "stat", Path: f.name, Err: fs.ErrClosed}
	}
	return f.info, nil
}

func (f *file) Read(p []byte) (int, error) {
	fail := func(err error) (int, error) { return 0, &fs.PathError{Op: "read", Path: f.name, Err: err} }
	switch {
	case f.closed:
		return fail(fs.ErrClosed)
	case f.info.IsDir():
		return fail(errors.New("is a directory"))
	case !f.info.Mode().IsRegular():
		return fail(errors.New("not a regular file"))
	}
	if f.body == nil {
		if err := f.open(); err != nil {
			return fail(err)
		}
	}
	n, err := f.body.Read(p)
	if err != nil && err != io.EOF {
		return n, &fs.PathError{Op: "read", Path: f.name, Err: err}
	}
	return n, err
}

// open starts the content's stream, whose answer, but not whose
// content, the timeout bounds; Close ends it.
func (f *file) open() error {
	c := f.fs.c
	if c.isClosed() {
		return fs.ErrClosed
	}
	ctx, stop := context.WithCancel(c.ctx)
	late := time.AfterFunc(c.timeout, stop)
	resp, err := c.request(ctx, http.MethodGet, routeFile, url.Values{"path": {f.name}}, nil, "")
	if !late.Stop() && err == nil {
		resp.Body.Close()
		err = context.DeadlineExceeded
	}
	if err != nil {
		stop()
		return err
	}
	f.body, f.stop = resp.Body, stop
	return nil
}

func (f *file) ReadDir(n int) ([]fs.DirEntry, error) {
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

func (f *file) Close() error {
	if f.closed {
		return &fs.PathError{Op: "close", Path: f.name, Err: fs.ErrClosed}
	}
	f.closed = true
	if f.body != nil {
		f.body.Close()
		f.stop()
	}
	return nil
}
