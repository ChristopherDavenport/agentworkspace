# Contributing

Issues and pull requests are welcome.

## Before you start

This library is where an agent's tools act: a file system, processes,
a working directory and an environment, behind one interface. It does
not decide what a tool may do (that is the product's policy), and it
does not hold the tools themselves. A change to the interface is a
change every backend must make, so open an issue first for anything
larger than a bug fix, and say which rule in `AGENTS.md` it touches.

## Development

Go 1.26 or later is required, on Linux or macOS. The full local check
is:

```sh
make check        # gofmt, tidy, vet, deps, staticcheck, govulncheck, race tests, guard tests
```

The module imports the standard library alone; `make deps` and a test
fail if anything else creeps in.

## Pull requests

- Keep the change focused; unrelated cleanups belong in their own PR.
- Add or update tests. Tests are table-driven and run offline; a
  confinement rule is tested on every backend in `backends`.
- Run `make check` before pushing. CI runs the same steps on the minimum
  and current Go versions.
- Note user-visible changes under *Unreleased* in `CHANGELOG.md`.
