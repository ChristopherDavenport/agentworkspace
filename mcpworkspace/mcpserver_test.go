package mcpworkspace

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// serverEnv makes the test binary an MCP stdio server: the server a
// program starts in the workspace, through Start, to talk to over the
// process's pipes.
const serverEnv = "MCPWORKSPACE_TEST_SERVER"

func TestMain(m *testing.M) {
	if os.Getenv(serverEnv) == "1" {
		srv := mcp.NewServer(&mcp.Implementation{Name: "in-the-workspace", Version: "1"}, nil)
		mcp.AddTool(srv, &mcp.Tool{Name: "where", Description: "The server's working directory."},
			func(context.Context, *mcp.CallToolRequest, struct{}) (*mcp.CallToolResult, struct{}, error) {
				wd, err := os.Getwd()
				if err != nil {
					return nil, struct{}{}, err
				}
				return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: wd}}}, struct{}{}, nil
			})
		if err := srv.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// An MCP stdio server started through Start runs in the workspace at
// the other end of the connection, and answers over the process's
// pipes: what a program needs to run a user's MCP server where its
// tools act. Closing the client ends the server.
func TestAnMCPServerRunsThroughStart(t *testing.T) {
	s := newTestServer(t)
	cs, _ := s.connect(t)
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	p := start(t, conn(cs), agentworkspace.Command{Args: []string{exe}, Env: []string{serverEnv + "=1"}})

	ctx := context.Background()
	inner, err := mcp.NewClient(&mcp.Implementation{Name: "program", Version: "test"}, nil).
		Connect(ctx, &mcp.IOTransport{Reader: p.Stdout(), Writer: p.Stdin()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tools, err := inner.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(tools.Tools) != 1 || tools.Tools[0].Name != "where" {
		t.Fatalf("tools %+v", tools.Tools)
	}
	res, err := inner.CallTool(ctx, &mcp.CallToolParams{Name: "where"})
	if err != nil {
		t.Fatal(err)
	}
	if got := res.Content[0].(*mcp.TextContent).Text; got != s.ws.Root() {
		t.Errorf("the server runs in %q, want the workspace's root %q", got, s.ws.Root())
	}
	within(t, 15*time.Second, "the server's end", func() {
		inner.Close()
		p.Wait()
	})
}
