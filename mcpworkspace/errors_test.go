package mcpworkspace

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/mcpworkspace/wire"
)

// servedPath is a path on the serving side's machine that a refusal's
// own text names, and that must not cross.
const servedPath = "/srv/hidden/layout"

// refusing is a workspace whose files and starts are refused with the
// sentinel of the kind they name ("k/<kind>", args[0] "<kind>"), its
// error's text naming servedPath.
type refusing struct{ agentworkspace.Workspace }

func refusalOf(kind string) error {
	return &fs.PathError{Op: "open", Path: servedPath + "/" + kind, Err: sentinelOf(kind)}
}

func (r refusing) FS() fs.FS { return refusingFS{} }

func (r refusing) Start(_ context.Context, cmd agentworkspace.Command) (agentworkspace.Process, error) {
	return nil, refusalOf(cmd.Args[0])
}

type refusingFS struct{}

func (refusingFS) Open(name string) (fs.File, error) {
	return nil, refusalOf(strings.TrimPrefix(name, "k/"))
}

// Every kind of RFC 0001 §2 crosses both ways, for a file's refusal and
// a method's, so errors.Is holds on the client as on the server; and
// the message names no path of the serving side's but under
// WithErrorText.
func TestEveryKindCrosses(t *testing.T) {
	for _, text := range []bool{false, true} {
		t.Run(fmt.Sprintf("WithErrorText %v", text), func(t *testing.T) {
			dir := t.TempDir()
			l, err := agentworkspace.NewLocal(dir, []string{"PATH=" + os.Getenv("PATH")})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { l.Close() })
			var opts []ServeOption
			if text {
				opts = append(opts, WithErrorText())
			}
			ws := refusing{l}
			s := &testServer{dir: dir, ws: ws, procs: NewProcesses(ws, opts...), opts: opts}
			t.Cleanup(s.procs.Close)
			cs, _ := s.connect(t)
			c := conn(t, cs)
			files := NewFS(SessionSource(cs))
			for _, kind := range wire.Kinds() {
				want := sentinelOf(kind)
				_, ferr := fs.Stat(files, "k/"+kind)
				_, serr := Start(context.Background(), c, agentworkspace.Command{Args: []string{kind}})
				for what, err := range map[string]error{"stat": ferr, "start": serr} {
					if !errors.Is(err, want) {
						t.Errorf("%s refused as %s: %v, want it to be %v", what, kind, err, want)
					}
					if err != nil && strings.Contains(err.Error(), servedPath) != text {
						t.Errorf("%s refused as %s: %q; the served path crossed: %v, want %v", what, kind, err, !text, text)
					}
				}
			}
		})
	}
}
