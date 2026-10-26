package agentworkspace

import (
	"go/build"
	"slices"
	"strings"
	"testing"
)

const modulePath = "github.com/ChristopherDavenport/agentworkspace"

// rootPackages are the root module's packages: the interface and Local,
// remote, the workspace over HTTP, and workspacetest, the conformance
// tables. A nested module (mcpworkspace, tunnel) is not one of them.
var rootPackages = []string{modulePath, modulePath + "/remote", modulePath + "/workspacetest"}

// TestImportBoundary enforces the module's one dependency rule: every
// package in it, and every test of one, imports the standard library
// and the module's own packages alone, so a product that wants only the
// interface, Local, remote or the tables pays for nothing else. A container
// starter, or anything with a dependency, is a nested module.
func TestImportBoundary(t *testing.T) {
	for _, dir := range []string{".", "remote", "workspacetest"} {
		pkg, err := build.Default.ImportDir(dir, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range pkg.Imports {
			if !isStandard(imp) && !slices.Contains(rootPackages, imp) {
				t.Errorf("%s imports %q; only the standard library and the root module are allowed", dir, imp)
			}
		}
		for _, imp := range append(pkg.TestImports, pkg.XTestImports...) {
			if !isStandard(imp) && !slices.Contains(rootPackages, imp) {
				t.Errorf("the tests in %s import %q; only the standard library and the root module are allowed", dir, imp)
			}
		}
	}
}

func isStandard(imp string) bool {
	first, _, _ := strings.Cut(imp, "/")
	return !strings.Contains(first, ".")
}
