package rules

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRulesPackageIsPure enforces the rules-engine purity contract that lets the
// CLI and SaaS share this code verbatim: no network, DB, filesystem, or os
// access, and no clock reads (time is injected via RuleConfig.Now).
func TestRulesPackageIsPure(t *testing.T) {
	forbiddenImports := map[string]bool{
		"net":           true,
		"net/http":      true,
		"database/sql":  true,
		"os":            true,
		"io/ioutil":     true,
		"os/exec":       true,
		"bufio":         true,
		"path/filepath": true,
	}
	// Clock/entropy reads break determinism. Checked at the AST level (not by
	// raw regex) so doc comments mentioning time.Now don't trip the guard.
	forbiddenCalls := map[string]map[string]bool{
		"time": {"Now": true, "Since": true, "Until": true},
		"rand": {}, // any rand.* call (empty set = match the package, see below)
	}

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue // tests may use os/filepath (this file does)
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(fset, f, src, 0) // comments excluded from AST
		if err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		for _, imp := range file.Imports {
			path := strings.Trim(imp.Path.Value, `"`)
			if forbiddenImports[path] {
				t.Errorf("%s imports forbidden package %q — rules engine must stay pure", f, path)
			}
		}
		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkg, ok := sel.X.(*ast.Ident)
			if !ok {
				return true
			}
			sels, watched := forbiddenCalls[pkg.Name]
			if !watched {
				return true
			}
			if len(sels) == 0 || sels[sel.Sel.Name] { // empty set = whole package banned
				t.Errorf("%s uses forbidden clock/entropy call %s.%s — inject time via RuleConfig.Now", f, pkg.Name, sel.Sel.Name)
			}
			return true
		})
	}
}

// TestEvaluateIsDeterministic runs the same inputs twice and requires identical
// output — the observable face of purity.
func TestEvaluateIsDeterministic(t *testing.T) {
	h, cm, cfg := demoFixture()
	a := Evaluate(h, cm, cfg)
	b := Evaluate(h, cm, cfg)
	if len(a) != len(b) {
		t.Fatalf("non-deterministic count: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i].ResourceKey != b[i].ResourceKey || a[i].EstSavingEUR != b[i].EstSavingEUR {
			t.Fatalf("non-deterministic finding at %d: %+v vs %+v", i, a[i], b[i])
		}
	}
}

var _ = ast.Print
