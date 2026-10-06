.PHONY: setup lint test demo bench audit ci

setup:
	go mod download

# gofmt must be clean, then go vet.
lint:
	@test -z "$$(gofmt -l cmd internal)" || (gofmt -l cmd internal; exit 1)
	go vet ./...

# 17 tests: shard map and router unit tests, and integration tests that start three real
# PostgreSQL 17 servers (binaries downloaded once into .tmp/pg/cache). Add -short to skip those.
test:
	go test -count=1 ./...

# Move a 200,000-order tenant between two shards while four workers write to it.
demo:
	go run ./cmd/zero-downtime-resharding

bench:
	@echo "M3: k6 load during every move, recording error rate and p99 latency"

# Known vulnerabilities in the modules and the Go standard library the code calls.
audit:
	go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./...

ci: setup lint test demo
