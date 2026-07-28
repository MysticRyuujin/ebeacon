.PHONY: help fmt fmt-check lint test vet vuln mod-check docker-build ci-local hooks-install loadtest reliability

GO_FILES := $(shell rg --files -g '*.go')

help:
	@echo "Available targets:"
	@echo "  make fmt           - format Go files with gofmt"
	@echo "  make fmt-check     - fail if any Go file is not gofmt-formatted"
	@echo "  make lint          - run golangci-lint"
	@echo "  make test          - run Go tests"
	@echo "  make vet           - run go vet"
	@echo "  make vuln          - run the optional govulncheck scan"
	@echo "  make mod-check     - verify go.mod/go.sum are tidy"
	@echo "  make loadtest      - build scripts/loadtest/loadtest"
	@echo "  make reliability   - build scripts/reliability/reliability"
	@echo "  make docker-build  - build local Docker image"
	@echo "  make ci-local      - run local CI-equivalent checks"
	@echo "  make hooks-install - install git hooks"

fmt:
	@gofmt -w $(GO_FILES)

fmt-check:
	@UNFORMATTED="$$(gofmt -l $(GO_FILES))"; \
	if [ -n "$$UNFORMATTED" ]; then \
		echo "These files need gofmt:"; \
		echo "$$UNFORMATTED"; \
		exit 1; \
	fi

lint:
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "golangci-lint is required. Install: https://golangci-lint.run/welcome/install/"; \
		exit 1; \
	}
	@golangci-lint run ./...

test:
	@go test -race ./...

vet:
	@go vet ./...

vuln:
	@# Pin GOTOOLCHAIN to the Go version declared in go.mod so local vuln
	@# scans use the same stdlib as CI (which installs Go per go.mod). Without
	@# this, a developer running a newer local Go than go.mod declares will
	@# silently miss stdlib vulns that CI reports against the older toolchain.
	@GOTOOLCHAIN=go$$(awk '/^go /{print $$2; exit}' go.mod) \
		go run golang.org/x/vuln/cmd/govulncheck@latest ./...

mod-check:
	@cp go.mod go.mod.bak && cp go.sum go.sum.bak
	@go mod tidy
	@if ! diff -q go.mod go.mod.bak > /dev/null 2>&1 || ! diff -q go.sum go.sum.bak > /dev/null 2>&1; then \
		cp go.mod.bak go.mod && cp go.sum.bak go.sum; \
		rm -f go.mod.bak go.sum.bak; \
		echo "go.mod/go.sum are not tidy — run 'go mod tidy'"; \
		exit 1; \
	fi
	@rm -f go.mod.bak go.sum.bak

loadtest:
	@go build -o scripts/loadtest/loadtest ./scripts/loadtest

reliability:
	@go build -o scripts/reliability/reliability ./scripts/reliability

docker-build:
	@docker build -t ebeacon:local .

ci-local: fmt-check mod-check vet lint test

hooks-install:
	@./scripts/install-hooks.sh
