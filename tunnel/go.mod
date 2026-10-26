module github.com/ChristopherDavenport/agentworkspace/tunnel

go 1.26

// The root requirement names the version this module is released at,
// and the replace below points at the tree so the release commit can
// name a version the proxy does not serve yet. Consumers ignore the
// replace and get the require; make extracted builds the module with
// it dropped, the way a consumer does.
require (
	github.com/ChristopherDavenport/agentworkspace v0.0.8
	github.com/coder/websocket v1.8.15
)

replace github.com/ChristopherDavenport/agentworkspace => ..
