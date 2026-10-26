module github.com/ChristopherDavenport/agentworkspace/mcpworkspace

go 1.26

// The root requirement names the version this module is released at,
// and the replace below points at the tree so the release commit can
// name a version the proxy does not serve yet. Consumers ignore the
// replace and get the require; make extracted builds the module with
// it dropped, the way a consumer does.
require (
	github.com/ChristopherDavenport/agentworkspace v0.0.5
	github.com/modelcontextprotocol/go-sdk v1.8.0
)

require (
	github.com/google/jsonschema-go v0.4.3 // indirect
	github.com/segmentio/asm v1.1.3 // indirect
	github.com/segmentio/encoding v0.5.4 // indirect
	github.com/yosida95/uritemplate/v3 v3.0.2 // indirect
	golang.org/x/oauth2 v0.35.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/sys v0.41.0 // indirect
	golang.org/x/time v0.15.0 // indirect
)

replace github.com/ChristopherDavenport/agentworkspace => ..
