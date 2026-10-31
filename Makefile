# Local parity for the CI lanes lint/test/race. CI adds shell, boundary,
# govulncheck, gosec, and actionlint; see .github/workflows/ci.yml.
# Needs: go (version in go.mod), golangci-lint v2.12.2 (same binary Eshu
# uses), git (for the 500-line cap file list).
#
# Coverage floor may only rise; the finish line follows the org contract.

COVERAGE_FLOOR ?= 74.7

.PHONY: ci lint test race

ci: lint test race

lint:
	go vet ./...
	golangci-lint run ./...
	golangci-lint fmt --diff ./...
	go mod tidy -diff
	@over=0; for f in $$(git ls-files '*.go'); do n=$$(wc -l < "$$f"); if [ "$$n" -gt 500 ]; then echo "$$f: $$n lines"; over=1; fi; done; test "$$over" -eq 0

test:
	go build ./...
	go test ./... -count=1

race:
	go test ./... -race -count=1 -covermode=atomic -coverprofile=coverage.out
	go tool cover -func=coverage.out
	@total=$$(go tool cover -func=coverage.out | awk '/^total:/ {gsub(/%/, "", $$3); print $$3}'); echo "statement coverage total=$${total}% floor=$(COVERAGE_FLOOR)%"; awk -v t="$$total" -v f="$(COVERAGE_FLOOR)" 'BEGIN { if (t + 0 < f + 0) { print "FAIL: coverage below floor"; exit 1 } }'
