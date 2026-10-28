// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import (
	"os/exec"
	"strings"
	"testing"
)

// allowedImports is the closed stdlib set the protocol package may use.
// The contract must stay dependency-free so core and collectors import it
// without inheriting a dependency tree: no eshu-main internals, no other
// eshu-hq modules, no third-party packages.
var allowedImports = map[string]bool{
	"embed": true, "encoding": true, "encoding/json": true,
	"errors": true, "fmt": true, "sort": true,
	"strconv": true, "strings": true, "time": true,
}

func TestImportBoundaryStaysStdlibOnly(t *testing.T) {
	out, err := exec.Command("go", "list", "-f", `{{range .Imports}}{{println .}}{{end}}`, ".").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	for _, imp := range strings.Fields(string(out)) {
		if !allowedImports[imp] {
			t.Fatalf("forbidden import %q: the protocol package must stay stdlib-only", imp)
		}
	}
}
