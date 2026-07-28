# Contributing to eBeacon

Thanks for considering a contribution. eBeacon is an open-source reverse proxy for Ethereum Beacon API nodes; it is operationally critical for the deployments that depend on it, so we keep the bar for changes high.

## Before you open a pull request

1. **Open an issue first** for anything beyond a small bug fix or doc change. This avoids wasted work on changes the maintainers wouldn't merge.
2. **Read [`AGENTS.md`](AGENTS.md)** — it's the single source of truth for the architecture and the package layout.
3. **Check existing patterns** — eBeacon prefers minimal new abstractions. Reuse the helpers that already exist before introducing new ones.

## Development setup

- Go version: see [`go.mod`](go.mod) (`go` directive).
- Install local hooks: `./scripts/install-hooks.sh` — runs `gofmt` on staged Go files and tests on commit, plus `go vet` and the race detector on push.
- Install `golangci-lint` to run the same lint checks as CI. `make vuln` is also available as an optional dependency scan.

## Required checks

Before pushing, run:

```bash
make ci-local
```

That verifies formatting and module tidiness, then runs `go vet`, `golangci-lint`, and `go test -race ./...`. CI runs the same set on every PR.

For changes that touch upstream routing, caching, or the SSE relay, also run the validation harnesses against a local instance:

```bash
go run ./scripts/loadtest/ -base http://127.0.0.1:5555/mainnet -concurrency 50 -duration 60
go run ./scripts/reliability/ -duration 30m -report 1m
```

## Pull request expectations

- **Tests are required** for new behavior. Bug fixes need a regression test that fails on `main` and passes on the change.
- **One logical change per PR.** Refactors and feature work are separate PRs.
- **No drive-by formatting** — diffs that re-format unrelated code make review hard.
- **No new TODO/FIXME comments.** If you find one, either fix it in a separate PR or open an issue and link it.
- **Match commit message style.** Short imperative subject (`Add X`, `Fix Y`), wrap at 72 chars, body explains *why*. The history is reviewed; please look at recent commits.
- **Rebase, don't merge.** Keep history linear on top of `main`.

## What we won't merge

- Changes that add backwards-compatibility shims for unreleased configuration keys.
- Half-finished features behind a flag that's never flipped.
- New dependencies for problems that can be solved with the standard library or existing dependencies.
- Breaking changes to the public CLI flags or the YAML config schema without a deprecation path documented in the PR.

## Reporting bugs

Use the GitHub issue tracker. For a useful report, include:

- eBeacon version (release tag or `git rev-parse HEAD`).
- A redacted `ebeacon.yaml` sufficient to reproduce.
- Logs from `logLevel: debug` covering the failure.
- For routing or caching bugs: which upstreams were involved and their client/version.

Security issues: see [`SECURITY.md`](SECURITY.md). Do not file them as public issues.

## License

By contributing, you agree that your contributions are licensed under the [Apache License, Version 2.0](LICENSE), the same terms as the rest of the project.
