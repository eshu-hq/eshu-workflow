// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

// Package ciworkflow holds the contract test for .github/workflows/ci.yml:
// the lane bodies live in the pinned eshu-hq/.github reusable, and this
// caller keeps its classifier, the lanes call, and the required aggregator.
// The checks are plain string reads so the module stays dependency-free.
package ciworkflow

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// reusablePin is the exact eshu-hq/.github commit the caller must pin (the
// merge of the opt-in Postgres bootstrap, eshu-hq/.github#12). A pin move
// fails the tests until the new reusable body is re-verified.
const reusablePin = "eshu-hq/.github/.github/workflows/go-collector-ci.yml@048d30a904b9e5f02ab8276285f0e889ce7820b0"

// actionlintSHA256 is the sha256 of actionlint 1.7.12's linux_amd64 tarball.
const actionlintSHA256 = "8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8"

// laneGate builds the expected run-* expression: lanes run on every push,
// and on pull requests only when one of the named classifier outputs fired.
// The weekly schedule runs only the lanes that name it explicitly.
func laneGate(schedule bool, outputs ...string) string {
	parts := make([]string, len(outputs))
	for i, out := range outputs {
		parts[i] = "needs.changes.outputs." + out + " == 'true'"
	}
	pr := strings.Join(parts, " || ")
	if len(parts) > 1 {
		pr = "(" + pr + ")"
	}
	gate := "github.event_name == 'push' || "
	if schedule {
		gate += "github.event_name == 'schedule' || "
	}
	return "${{ " + gate + "(github.event_name == 'pull_request' && " + pr + ") }}"
}

// retiredGates pins every run-* input to the path classes the retired inline
// lane's `if:` keyed on. A flipped or widened gate turns the suite red, so
// lane selection cannot go dark (or wide) unnoticed.
var retiredGates = map[string]string{
	"run-lint":        laneGate(false, "go", "misc", "workflows"),
	"run-test":        laneGate(false, "go", "misc", "workflows"),
	"run-race":        laneGate(false, "go", "misc", "workflows"),
	"run-shell":       laneGate(false, "shell"),
	"run-boundary":    laneGate(false, "go", "misc", "workflows"),
	"run-image":       laneGate(false, "image", "go", "workflows"),
	"run-govulncheck": laneGate(true, "deps", "workflows"),
	"run-gosec":       laneGate(true, "go", "workflows"),
	"run-actionlint":  laneGate(false, "workflows"),
}

// toolPins are the tool versions the retired lanes pinned, plus the lane
// settings and Postgres bootstrap values that must survive the move.
var toolPins = map[string]string{
	"module":                   "eshu-workflow",
	"go-version-file":          "go.mod",
	"coverage-floor":           "93.5",
	"golangci-lint-version":    "v2.12.2",
	"govulncheck-version":      "v1.9.0",
	"gosec-version":            "v2.30.0",
	"actionlint-version":       "1.7.12",
	"actionlint-sha256":        actionlintSHA256,
	"gosec-fail-on-findings":   "false",
	"image-tag":                "ci-workflow:ci",
	"image-context":            ".",
	"boundary-command":         "go test -count=1 -run TestImportBoundaryStaysStdlibOnly ./workflowv1/",
	"postgres-image":           "postgres:18",
	"postgres-user":            "claimplane_test",
	"postgres-password":        "claimplane_test",
	"postgres-db":              "claimplane_test_store",
	"postgres-extra-databases": "claimplane_test_server,claimplane_test_coordinator",
}

var (
	jobHeader    = regexp.MustCompile(`^  ([A-Za-z][A-Za-z0-9_-]*):\s*$`)
	withInput    = regexp.MustCompile(`^      ([a-z][a-z0-9-]*): (.+)$`)
	needsLine    = regexp.MustCompile(`(?m)^    needs: \[(.*)\]\s*$`)
	usesLine     = regexp.MustCompile(`(?m)^\s*(?:- )?uses: (\S+)(.*)$`)
	pinnedAction = regexp.MustCompile(`^[A-Za-z0-9._-]+/[A-Za-z0-9._/-]+@[0-9a-f]{40}$`)
	secretUse    = regexp.MustCompile(`secrets\.([A-Za-z0-9_]+)`)
	floorLine    = regexp.MustCompile(`(?m)^COVERAGE_FLOOR\s*\??=\s*["']?([0-9.]+)["']?`)
)

func readFile(t *testing.T, path string) string {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// stripComments drops full-line comments so explanatory header text never
// satisfies (or trips) an executable-shape check.
func stripComments(text string) string {
	var b strings.Builder
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(strings.TrimLeft(line, " \t"), "#") {
			continue
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return b.String()
}

// jobBlocks splits ci.yml into its top-level jobs, returning the names in
// file order and each job's body (comments included).
func jobBlocks(t *testing.T) ([]string, map[string]string) {
	t.Helper()

	var (
		names  []string
		blocks = map[string][]string{}
		inJobs bool
		cur    string
	)
	for _, line := range strings.Split(readFile(t, ".github/workflows/ci.yml"), "\n") {
		if line == "jobs:" {
			inJobs = true
			continue
		}
		if !inJobs {
			continue
		}
		if m := jobHeader.FindStringSubmatch(line); m != nil {
			cur = m[1]
			names = append(names, cur)
			continue
		}
		if cur != "" {
			blocks[cur] = append(blocks[cur], line)
		}
	}
	out := make(map[string]string, len(blocks))
	for name, lines := range blocks {
		out[name] = "\n" + strings.Join(lines, "\n")
	}
	return names, out
}

func block(t *testing.T, name string) string {
	t.Helper()

	_, blocks := jobBlocks(t)
	body, ok := blocks[name]
	if !ok {
		t.Fatalf("ci.yml has no %q job", name)
	}
	return body
}

// withInputs returns the single-line `with:` inputs of a job body
// (six-space indent). A block-scalar input (`name: |`) is recorded as "|"
// and its indented body lines are skipped.
func withInputs(body string) map[string]string {
	inputs := map[string]string{}
	inWith := false
	for _, line := range strings.Split(body, "\n") {
		if line == "    with:" {
			inWith = true
			continue
		}
		if !inWith {
			continue
		}
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") || strings.HasPrefix(line, "        ") {
			continue
		}
		m := withInput.FindStringSubmatch(line)
		if m == nil {
			break
		}
		inputs[m[1]] = m[2]
	}
	return inputs
}

// TestCallerCallsPinnedReusable pins the thin-caller shape: lanes uses the
// reusable at an exact SHA with the version as a trailing comment, every
// other action is SHA-pinned, and no inline lane body remains.
func TestCallerCallsPinnedReusable(t *testing.T) {
	t.Parallel()

	lanes := block(t, "lanes")
	if !strings.Contains(lanes, "\n    uses: "+reusablePin+" # v2+postgres\n") {
		t.Errorf("lanes does not call %s with a trailing # v2+postgres comment", reusablePin)
	}
	if strings.Contains(lanes, "\n    steps:") {
		t.Error("lanes carries inline steps: lane bodies live in the reusable")
	}

	text := stripComments(readFile(t, ".github/workflows/ci.yml"))
	for _, m := range usesLine.FindAllStringSubmatch(text, -1) {
		if !pinnedAction.MatchString(m[1]) {
			t.Errorf("uses %q, want a full commit SHA pin", m[1])
		}
		if !strings.Contains(m[2], "# ") {
			t.Errorf("uses %q has no trailing version comment", m[1])
		}
	}
	for _, inline := range []string{
		"golangci-lint run", "golangci-lint-action", "go test ./... -race",
		"govulncheck -scan", "gosec -no-fail", "rhysd/actionlint",
		"docker build", "ludeeus/action-shellcheck",
		"codeql-action/upload-sarif", "upload-artifact",
	} {
		if strings.Contains(text, inline) {
			t.Errorf("ci.yml still carries inline lane body %q: it lives in the reusable", inline)
		}
	}
}

// TestCallerHasNoLocalPostgres keeps the Postgres bootstrap in the reusable:
// no services block and no psql step may linger in the caller.
func TestCallerHasNoLocalPostgres(t *testing.T) {
	t.Parallel()

	text := stripComments(readFile(t, ".github/workflows/ci.yml"))
	for _, leftover := range []string{"services:", "psql", "PGPASSWORD", "pg_isready", "--health-cmd"} {
		if strings.Contains(text, leftover) {
			t.Errorf("ci.yml still carries %q: Postgres comes from the reusable's postgres-* inputs", leftover)
		}
	}
}

// TestCallerKeepsFrozenJobNames keeps the org contract: the classifier, the
// reusable call, and the only ruleset-required context.
func TestCallerKeepsFrozenJobNames(t *testing.T) {
	t.Parallel()

	got, _ := jobBlocks(t)
	want := []string{"changes", "lanes", "ci-complete"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("ci.yml jobs = %v, want exactly %v", got, want)
	}
	if !strings.Contains(block(t, "lanes"), "\n    name: lanes\n") {
		t.Error("lanes must set name: lanes so inner jobs show as `lanes / <lane>`")
	}
}

// TestChangesRunsOnEveryEvent guards the schedule trap: every lane needs the
// changes job, so an if: guard would skip every lane on a scheduled run.
func TestChangesRunsOnEveryEvent(t *testing.T) {
	t.Parallel()

	body := block(t, "changes")
	if strings.Contains(stripComments(body), "\n    if:") {
		t.Error("changes job has an if: guard; every lane would skip when it skips")
	}
	for _, out := range []string{"go", "shell", "deps", "image", "workflows", "misc"} {
		if !strings.Contains(body, "      "+out+": ${{ steps.filter.outputs."+out+" }}") {
			t.Errorf("changes drops output %q that a lane gate reads", out)
		}
	}
	for _, want := range []string{"predicate-quantifier: 'some-with-excludes'", "misc:"} {
		if !strings.Contains(body, want) {
			t.Errorf("changes lacks %q: unclassified files would stop running the Go lanes", want)
		}
	}
}

// TestLanesKeepRetiredGates fails if any lane selector drifts from the path
// classes its retired inline lane keyed on, or is dropped (the lane would
// silently un-run).
func TestLanesKeepRetiredGates(t *testing.T) {
	t.Parallel()

	lanes := block(t, "lanes")
	if !strings.Contains(lanes, "\n    needs: [changes]\n") {
		t.Error("lanes must need exactly [changes]")
	}
	inputs := withInputs(lanes)
	for input, want := range retiredGates {
		if got, ok := inputs[input]; !ok || got != want {
			t.Errorf("lanes input %q = %q, want the retired gate %q", input, got, want)
		}
	}
	// The gate helper must agree with a hand-written expression, so a bug in
	// laneGate cannot make the table above vacuous.
	const hand = "${{ github.event_name == 'push' || (github.event_name == 'pull_request' && needs.changes.outputs.shell == 'true') }}"
	if got := laneGate(false, "shell"); got != hand {
		t.Errorf("laneGate(shell) = %q, want %q", got, hand)
	}
}

// TestLanesKeepToolPins keeps every tool pin, the coverage floor, the
// boundary command, and the Postgres bootstrap values the retired lanes
// carried.
func TestLanesKeepToolPins(t *testing.T) {
	t.Parallel()

	inputs := withInputs(block(t, "lanes"))
	for input, want := range toolPins {
		if got, ok := inputs[input]; !ok || got != want {
			t.Errorf("lanes input %q = %q, want %q", input, got, want)
		}
	}
}

// TestImageCommandKeepsDigestPinCheck keeps the Dockerfile digest-pin proof
// that the retired image lane ran after the build.
func TestImageCommandKeepsDigestPinCheck(t *testing.T) {
	t.Parallel()

	lanes := block(t, "lanes")
	if withInputs(lanes)["image-command"] != "|" {
		t.Fatal("lanes must set image-command as a block scalar")
	}
	for _, want := range []string{
		"set -euo pipefail",
		`from_total=$(grep -Ec '^[[:space:]]*FROM ' Dockerfile)`,
		`from_pinned=$(grep -Ec '^[[:space:]]*FROM [^[:space:]]+@sha256:[0-9a-f]{64}' Dockerfile)`,
		`[ "$from_pinned" -ge 1 ] && [ "$from_pinned" -eq "$from_total" ]`,
	} {
		if !strings.Contains(lanes, "        "+want+"\n") {
			t.Errorf("image-command lost %q", want)
		}
	}
}

// aggregatorProblems returns every way run breaks the ci-complete contract:
// compare each job's result (whole-object comparison never matches and a
// bare needs.*.result array breaks green runs), fail failed and cancelled
// lanes, and say skips are allowed.
func aggregatorProblems(run string) []string {
	var problems []string
	if !strings.Contains(run, `v.get("result") in ("failure", "cancelled")`) {
		problems = append(problems, `aggregator must test v.get("result") in ("failure", "cancelled") per job, not whole job objects (false-green)`)
	}
	if strings.Contains(run, "join(needs.*.result") {
		problems = append(problems, "aggregator must not expand needs.*.result (bare array breaks green runs)")
	}
	for _, want := range []string{"skip", "toJSON(needs)"} {
		if !strings.Contains(run, want) {
			problems = append(problems, "aggregator never mentions "+want)
		}
	}
	return problems
}

// TestCICompleteAggregatesEveryLane keeps ci-complete the only required
// context: it always runs, needs the classifier and the lanes call, and its
// aggregator keeps the per-job result shape.
func TestCICompleteAggregatesEveryLane(t *testing.T) {
	t.Parallel()

	body := block(t, "ci-complete")
	m := needsLine.FindStringSubmatch(body)
	if m == nil {
		t.Fatal("ci-complete has no needs list")
	}
	var needed []string
	for _, name := range strings.Split(m[1], ",") {
		needed = append(needed, strings.TrimSpace(name))
	}
	if strings.Join(needed, ",") != "changes,lanes" {
		t.Errorf("ci-complete needs = %v, want exactly [changes lanes]", needed)
	}
	if !strings.Contains(body, "\n    if: always()\n") {
		t.Error("ci-complete must run with if: always()")
	}
	if problems := aggregatorProblems(stripComments(body)); len(problems) > 0 {
		t.Errorf("ci-complete aggregator breaks its contract:\n%s", strings.Join(problems, "\n"))
	}

	for name, run := range map[string]string{
		"bare needs array": "RESULTS: ${{ join(needs.*.result, ' ') }}\nfor result in $RESULTS; do :; done",
		"whole objects":    `bad = {k: v for k, v in needs.items() if v == "failure"}`,
	} {
		if len(aggregatorProblems(run)) == 0 {
			t.Errorf("%s: the gate accepted a violating aggregator", name)
		}
	}
}

// TestCoverageFloorParity keeps `make ci` honest: the Makefile floor and the
// lanes coverage-floor input must name the same number.
func TestCoverageFloorParity(t *testing.T) {
	t.Parallel()

	makeFloor := floorLine.FindStringSubmatch(readFile(t, "Makefile"))
	ciFloor, ok := withInputs(block(t, "lanes"))["coverage-floor"]
	if makeFloor == nil || !ok {
		t.Fatalf("COVERAGE_FLOOR literal missing (Makefile=%v lanes=%q)", makeFloor, ciFloor)
	}
	if makeFloor[1] != ciFloor {
		t.Errorf("COVERAGE_FLOOR drift: Makefile=%s lanes=%s, want one shared value", makeFloor[1], ciFloor)
	}
}

// TestCallerSafetyContract keeps fork PRs working and hung runners bounded:
// contents: read only, one cancelling concurrency group per PR, no secret
// beyond the automatic GITHUB_TOKEN, and a timeout on every job that can
// carry one (the schema forbids it on the reusable call).
func TestCallerSafetyContract(t *testing.T) {
	t.Parallel()

	text := readFile(t, ".github/workflows/ci.yml")
	if !strings.Contains(text, "\npermissions:\n  contents: read\n") {
		t.Error("top-level permissions must be contents: read")
	}
	if !strings.Contains(text, "github.event.pull_request.number") ||
		!strings.Contains(text, "cancel-in-progress: ${{ github.event_name == 'pull_request' }}") {
		t.Error("concurrency must group per PR and cancel superseded PR runs")
	}
	for _, m := range secretUse.FindAllStringSubmatch(stripComments(text), -1) {
		if m[1] != "GITHUB_TOKEN" {
			t.Errorf("ci.yml uses configured secret %q, want none", m[1])
		}
	}

	names, blocks := jobBlocks(t)
	for _, name := range names {
		has := strings.Contains(blocks[name], "\n    timeout-minutes:")
		if name == "lanes" && has {
			t.Error("lanes sets timeout-minutes: the schema forbids it on reusable calls")
		}
		if name != "lanes" && !has {
			t.Errorf("job %s sets no timeout-minutes", name)
		}
	}
}
