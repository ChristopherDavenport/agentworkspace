package agentworkspace

import (
	"go/build"
	"strings"
	"testing"
)

const modulePath = "github.com/ChristopherDavenport/agentworkspace"

// TestImportBoundary enforces the module's one dependency rule: the
// package and its tests import the standard library alone, so a
// product that wants only the interface and Local pays for nothing
// else. A remote client or a container starter is a subpackage or a
// nested module, with its own dependencies.
func TestImportBoundary(t *testing.T) {
	pkg, err := build.Default.ImportDir(".", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, imp := range pkg.Imports {
		if !isStandard(imp) {
			t.Errorf("agentworkspace imports %q; only the standard library is allowed", imp)
		}
	}
	for _, imp := range append(pkg.TestImports, pkg.XTestImports...) {
		if !isStandard(imp) && !strings.HasPrefix(imp, modulePath) {
			t.Errorf("agentworkspace tests import %q; only the standard library is allowed", imp)
		}
	}
}

func isStandard(imp string) bool {
	first, _, _ := strings.Cut(imp, "/")
	return !strings.Contains(first, ".")
}
