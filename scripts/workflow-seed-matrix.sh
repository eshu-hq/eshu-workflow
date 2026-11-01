#!/usr/bin/env bash
# Seeded RED/GREEN matrix for the locally provable CI lanes.
#
# Local runner, not a CI gate: for each seed it plants a defect, asserts the
# named lane command turns red AND names the seed, removes the seed, and
# asserts the lane turns green again. It never compares the tree against a
# committed count or list.
#
# Seeds: vet, lint, format, race, tidy (unused require AND a go.mod-only
# reorder), the 500-line cap, and the coverage floor (RED + GREEN each). Each
# seed is also run through the matching `make lint` / `make race` recipe, so a
# Makefile recipe that swallows a failure shows up as a FAIL here.
#
# Lane commands are the exact steps of the lint and race jobs in
# .github/workflows/ci.yml (and the Makefile recipes `make ci` runs). One
# deliberate substitution: the lane checks write the race profile to the temp
# dir. The floor checks run `make race`, which enforces the Makefile floor; the
# script requires it to equal the floor in ci.yml. The test, boundary, shell,
# image, govulncheck, gosec, actionlint and ci-complete lanes are not reseeded,
# and the `git diff --check` step of the lint job is not seeded.
#
# Seed .go files live in workflowv1/ (package workflowv1, stdlib-only imports
# so the import-boundary test stays green). The cleanup trap removes them on
# every exit path.
#
# Usage: bash scripts/workflow-seed-matrix.sh
# Needs: go, rg (ripgrep), awk, golangci-lint v2.12.2 in GOPATH/bin, and the
# Postgres the race suite needs (README "Tests need Postgres").
# Must leave the tree exactly as found: every seed is removed after its check.
set -euo pipefail

cd "$(dirname "$0")/.."

GOBIN_DIR="${GOBIN:-$(go env GOPATH)/bin}"
export PATH="$GOBIN_DIR:$PATH"

TMP="$(mktemp -d)"
LOG="$TMP/last.log"
PROFILE="$TMP/coverage-lane.out"
# A private lint cache keeps results cached from other worktrees (absolute
# paths, files since deleted) from leaking into the lane, as a fresh CI runner.
export GOLANGCI_LINT_CACHE="$TMP/golangci-lint-cache"
pass=0
fail=0
had_coverage_out=0
[ -e coverage.out ] && had_coverage_out=1

PKG=workflowv1
SEEDS=(
	"$PKG/zz_seed_vet.go" "$PKG/zz_seed_lint.go" "$PKG/zz_seed_format.go"
	"$PKG/zz_seed_race_test.go" "$PKG/zz_seed_cap.go"
)

restore_mod() {
	git checkout -q -- go.mod go.sum
}

cleanup() {
	git reset -q -- "$PKG/zz_seed_cap.go" 2>/dev/null || true
	rm -f "${SEEDS[@]}"
	restore_mod 2>/dev/null || true
	[ "$had_coverage_out" -eq 0 ] && rm -f coverage.out
	rm -rf "$TMP"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# Refuse only on leftover seeds from an earlier run; unrelated uncommitted
# work is expected. The end-of-run check diffs the tree against a snapshot.
if ls "$PKG"/zz_seed_* >/dev/null 2>&1; then
	echo "FAIL: leftover seed files present; refusing to seed"
	ls "$PKG"/zz_seed_*
	exit 1
fi
if ! git diff --exit-code -- go.mod go.sum >/dev/null 2>&1; then
	echo "FAIL: go.mod/go.sum already drifted; refusing to seed"
	exit 1
fi
SNAPSHOT="$TMP/status-before.txt"
git status --porcelain >"$SNAPSHOT"

ok() { # $1: check name
	echo "PASS: $1"
	pass=$((pass + 1))
}
bad() { # $1: check name
	echo "FAIL: $1"
	tail -n 20 "$LOG" 2>/dev/null || true
	fail=$((fail + 1))
}

# red NAME SEED_PATTERN CMD...: CMD must exit non-zero and its output must match.
red() {
	local name="$1" pattern="$2"
	shift 2
	if "$@" >"$LOG" 2>&1; then
		bad "$name (lane stayed green)"
	elif rg -q -- "$pattern" "$LOG"; then
		ok "$name"
	else
		bad "$name (output does not name the seed)"
	fi
}

# green NAME CMD...: CMD must exit zero.
green() {
	local name="$1"
	shift
	if "$@" >"$LOG" 2>&1; then
		ok "$name"
	else
		bad "$name"
	fi
}

# --- lane commands, verbatim from the lint and race jobs of ci.yml ---
lane_vet() { go vet ./...; }
lane_lint() { golangci-lint run ./... --timeout 10m; }
lane_format() { golangci-lint fmt --diff ./...; }
lane_race() { go test ./... -race -count=1 -covermode=atomic -coverprofile="$PROFILE"; }
lane_tidy_diff() { go mod tidy -diff; }
lane_tidy() {
	# ci.yml runs these under bash -e; inside the `if` that calls a lane errexit
	# is off, so chain each step explicitly.
	go mod tidy -diff || return 1
	git diff --exit-code -- go.mod go.sum || return 1
}
lane_cap() {
	local over=0 f n
	for f in $(git ls-files '*.go'); do
		n=$(wc -l <"$f")
		if [ "$n" -gt 500 ]; then
			echo "$f: $n lines"
			over=1
		fi
	done
	if [ "$over" -ne 0 ]; then
		echo "FAIL: .go files over 500 lines"
		return 1
	fi
}

# --- vet: seeded printf mismatch turns `go vet ./...` red ---
cat >"$PKG/zz_seed_vet.go" <<'EOF'
package workflowv1

import "fmt"

var _ = fmt.Sprintf("%d", "not-a-number")
EOF
red "vet RED on seeded printf mismatch" "zz_seed_vet.go" lane_vet
red "make lint RED on seeded printf mismatch" "zz_seed_vet.go" make lint
rm -f "$PKG/zz_seed_vet.go"
green "vet GREEN after seed removal" lane_vet

# --- lint: seeded strings.Replace with -1 trips staticcheck QF1004 ---
cat >"$PKG/zz_seed_lint.go" <<'EOF'
package workflowv1

import "strings"

var _ = strings.Replace("aaa", "a", "b", -1)
EOF
red "lint RED on seeded QF1004" "zz_seed_lint.go.*QF1004" lane_lint
red "make lint RED on seeded QF1004" "zz_seed_lint.go.*QF1004" make lint
rm -f "$PKG/zz_seed_lint.go"
green "lint GREEN after seed removal" lane_lint

# --- format: seeded unformatted file trips the gofumpt diff check ---
printf 'package workflowv1\n\nvar _ = func() int { x:=1; return x }()\n' >"$PKG/zz_seed_format.go"
red "format RED on seeded unformatted file" "zz_seed_format.go" lane_format
red "make lint RED on seeded unformatted file" "zz_seed_format.go" make lint
rm -f "$PKG/zz_seed_format.go"
green "format GREEN after seed removal" lane_format

# --- race: seeded data race fails the race lane command ---
cat >"$PKG/zz_seed_race_test.go" <<'EOF'
package workflowv1

import "testing"

func TestSeedMatrixRace(t *testing.T) {
	x := 0
	done := make(chan bool)
	go func() { x++; done <- true }()
	x++
	<-done
}
EOF
if lane_race >"$LOG" 2>&1; then
	bad "race RED on seeded data race (lane stayed green)"
elif rg -q "DATA RACE" "$LOG" && rg -q "zz_seed_race_test.go" "$LOG"; then
	ok "race RED on seeded data race"
else
	bad "race RED on seeded data race (no race reported on the seed)"
fi
if make race >"$LOG" 2>&1; then
	bad "make race RED on seeded data race (recipe exited 0)"
elif rg -q "DATA RACE" "$LOG" && rg -q "zz_seed_race_test.go" "$LOG"; then
	ok "make race RED on seeded data race"
else
	bad "make race RED on seeded data race (no race reported on the seed)"
fi
rm -f "$PKG/zz_seed_race_test.go"
green "race GREEN after seed removal" lane_race

# --- tidy: seeded unused requirement trips the tidy drift check ---
go mod edit -require=gopkg.in/yaml.v3@v3.0.1
red "tidy RED on seeded unused require" "yaml.v3" lane_tidy
red "go mod tidy -diff alone RED on seeded unused require" "yaml.v3" lane_tidy_diff
red "make lint RED on seeded unused require" "yaml.v3" make lint
restore_mod
green "tidy GREEN after seed removal" lane_tidy

# --- tidy: go.mod-only drift ---
# Swapping the first two lines of the first require block leaves go.sum
# untouched. The seeded edit is itself a `git diff` against HEAD, so the
# `git diff --exit-code` step would go red for any go.mod edit; the checks that
# prove tidy catches the drift are `go mod tidy -diff` alone and `make lint`
# (which has no git diff step).
awk 'f==1{a=$0; f=2; next} f==2{print $0; print a; f=3; next} {print} /^require \(/ && !d{f=1; d=1}' go.mod >"$TMP/go.mod.swapped"
cat "$TMP/go.mod.swapped" >go.mod
if git diff --quiet -- go.sum && ! git diff --quiet -- go.mod; then
	red "go mod tidy -diff alone RED on go.mod-only drift" "pgx|prometheus" lane_tidy_diff
	red "tidy RED on go.mod-only drift" "pgx|prometheus" lane_tidy
	red "make lint RED on go.mod-only drift" "pgx|prometheus" make lint
else
	echo "FAIL: go.mod-only seed did not produce a go.mod-only change"
	fail=$((fail + 1))
fi
restore_mod
green "make lint GREEN after the drift is removed" make lint

# --- 500-line cap: a 501-line .go file trips the lane's wc step ---
# Both ci.yml and the Makefile read `git ls-files`, so the seed is added
# intent-to-add; the cleanup trap and the step below drop it from the index.
{
	echo "package workflowv1"
	echo ""
	for i in $(seq 1 499); do echo "var _ = $i"; done
} >"$PKG/zz_seed_cap.go"
if [ "$(wc -l <"$PKG/zz_seed_cap.go")" -ne 501 ]; then
	echo "FAIL: cap seed is not 501 lines"
	exit 1
fi
git add -N "$PKG/zz_seed_cap.go"
red "cap RED on seeded 501-line file" "zz_seed_cap.go" lane_cap
red "make lint RED on seeded 501-line file" "zz_seed_cap.go" make lint
git reset -q -- "$PKG/zz_seed_cap.go"
rm -f "$PKG/zz_seed_cap.go"
green "cap GREEN after seed removal" lane_cap

# --- coverage floor: `make race` enforces the Makefile floor ---
mk_floor=$(sed -n 's/^COVERAGE_FLOOR ?= \([0-9.]*\).*/\1/p' Makefile | head -n 1)
ci_floor=$(sed -n 's/^ *COVERAGE_FLOOR: "\{0,1\}\([0-9.]*\)"\{0,1\}.*/\1/p' .github/workflows/ci.yml | head -n 1)
red "floor RED at 100" "FAIL: coverage below floor" make race COVERAGE_FLOOR=100
if [ -z "$mk_floor" ] || [ "$mk_floor" != "$ci_floor" ]; then
	echo "makefile floor='$mk_floor' ci.yml floor='$ci_floor'" >"$LOG"
	bad "floor GREEN (Makefile and ci.yml floors differ or are missing)"
else
	green "floor GREEN at $mk_floor" make race
	if [ "$fail" -eq 0 ]; then
		echo "  $(rg -o 'statement coverage total=[0-9.]+% floor=[0-9.]+%' "$LOG")"
	fi
fi

echo "seed matrix: pass=$pass fail=$fail"
[ "$had_coverage_out" -eq 0 ] && rm -f coverage.out
if ! diff -u "$SNAPSHOT" <(git status --porcelain) >"$TMP/status-diff.txt" 2>&1; then
	echo "FAIL: tree changed by the matrix run:"
	cat "$TMP/status-diff.txt"
	exit 1
fi
[ "$fail" -eq 0 ]
