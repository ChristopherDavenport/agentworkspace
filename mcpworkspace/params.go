package mcpworkspace

import (
	"errors"

	"github.com/ChristopherDavenport/agentworkspace/mcpworkspace/wire"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// The process methods' parameters and results as the SDK sends them:
// package wire's shapes with the SDK's _meta. A client calling a method
// itself (mcp.CallCustomMethod) uses these.
type (
	StartParams struct {
		mcp.ParamsBase
		wire.StartArgs
	}
	StartResult struct {
		mcp.ResultBase
		wire.Started
	}
	IDParams struct {
		mcp.ParamsBase
		wire.ProcessID
	}
	WriteParams struct {
		mcp.ParamsBase
		wire.WriteArgs
	}
	ReadParams struct {
		mcp.ParamsBase
		wire.ReadArgs
	}
	ReadResult struct {
		mcp.ResultBase
		wire.ReadReply
	}
	SignalParams struct {
		mcp.ParamsBase
		wire.SignalArgs
	}
	SignalResult struct {
		mcp.ResultBase
		wire.Signaled
	}
	ExitResult struct {
		mcp.ResultBase
		wire.Exit
	}
	EmptyResult struct{ mcp.ResultBase }
)

// RegisterClient registers the process methods on c, before it
// connects; Start needs them on the session's client.
func RegisterClient(c *mcp.Client) error {
	return errors.Join(
		mcp.AddSendingCustomMethod[*StartParams, *StartResult](c, wire.MethodStart),
		mcp.AddSendingCustomMethod[*WriteParams, *EmptyResult](c, wire.MethodWrite),
		mcp.AddSendingCustomMethod[*ReadParams, *ReadResult](c, wire.MethodRead),
		mcp.AddSendingCustomMethod[*IDParams, *EmptyResult](c, wire.MethodCloseStdin),
		mcp.AddSendingCustomMethod[*SignalParams, *SignalResult](c, wire.MethodSignal),
		mcp.AddSendingCustomMethod[*IDParams, *ExitResult](c, wire.MethodWait),
		mcp.AddSendingCustomMethod[*IDParams, *ExitResult](c, wire.MethodClose),
	)
}
