# Contributing

## Before Editing

1. Read [`AGENTS.md`](AGENTS.md) and the affected module's current docs.
2. Run `make inventory` and the narrow baseline gate for the module.
3. Identify owned dependencies and reverse dependants in `modules.json`.
4. Preserve unrelated work and maintained fixtures.

## Changes

Keep commits focused and conventional. Update every affected changelog with
the behavior and migration impact. Public API changes require compatibility
evidence and documentation. Protocol or specification behavior requires an
executable contract and an explicit decision only when a material ambiguity
affects compatibility.

New direct dependencies and dependency updates must follow the
[dependency governance policy](AGENTS.md#dependencies-and-supply-chain). Package-local
update bots are forbidden; the root policy owns every module and action update.

Specification-backed changes must follow the
[design contract](AGENTS.md#design), update an affected stable decision when a
register owns the behavior, and complete the Specification Decisions section
of the pull request template. An unresolved material interpretation is
release-blocking; peer behavior cannot silently select policy.

Review the affected [root](docs/specification-decisions.md) or
[MSK IAM](adapters/mskiam/docs/specification-decisions.md) register before
changing behavior they own. OpenTelemetry behavior is owned by the canonical
[`adapters/otel`](adapters/otel) implementation and its executable tests.

When mutation testing is selected for a material risk, it must finish with zero
surviving viable mutants.

Do not add package-local workflows, permanent replacements, machine-specific
paths, bypass flags, broad mutation exclusions, or aggregate quality metrics
that hide a failing package.

## Verification

Use Go 1.27.3; public module minima remain Go 1.27.0. An isolated checkout
needs no sibling repositories or coordination goal files. Begin with root
deterministic tests and one active target when its hostile fetch boundary
changes, using a disposable cache per bounded session:

```bash
(
  set -eu
  GOCACHE="$(mktemp -d)"
  export GOCACHE
  trap 'rm -rf "${GOCACHE}"' EXIT
  GOWORK=off go test ./...
  GOWORK=off go test . -run '^$' -fuzz '^FuzzFetchDecompression$' -fuzztime=2s -timeout=2m
)
```

Deterministic tests execute fuzz seeds/corpus, not active fuzzing. They omit
intentional nested modules and integration/interoperability tags. Install the
public v2.4.0 binary archive for your platform from
[go-library-tools releases](https://github.com/faustbrian/go-library-tools/releases/tag/v2.4.0),
checking `checksums.txt` against `.golib.yaml` and the archive against that
checksum set. Put the extracted `golib` binary on `PATH`, or select it through
`GOLIB`; do not build Tools from source. CI uses this public release and checks
out its immutable reusable workflow source.

Run adjacent commands in one disposable-cache session as shown above:

```bash
make inventory
make check
make ci
make specification-check
golib check --local --module adapters/mskiam
golib mutation --module . --package .
```

`make check` and `make ci` select the bounded local contract. The root race
runs once with a two-minute timeout: observer reentry cannot fence or close
the consumer; successful rebalance shutdown closes the backend exactly once;
cancelled record and batch runners admit no handler or commit. These owned
backend assertions are not real broker, ACL or credential-rotation proof.
Base-free checks run seeds/corpus; changed-source CI selects causal active
fuzz smoke. Optional mutation selects one production package, one worker and
a twenty-minute deadline. Offline specification checking is ordinary.
Nested modules retain independent public contracts and dependency baselines;
native checks run inside the selected module with `GOWORK=off`. The
`benchmarks/clients` integration tag retains its owned broker fixtures even
though the service catalog is empty; all-module checks are not necessarily
Docker-free.

Select full qualification and release rehearsal explicitly:

```bash
make check-full
make specification-check-online
make -f verification/package.mk conformance
make -f verification/package.mk interoperability
golib api check
golib release dry-run --module .
```

Full checks retain all 18 fuzz targets at 10000x/20m, both 100ms/20m benchmark
operations, five 10m package-contract operations and all conformance,
interoperability, API and mutation declarations. Select affected real-broker
integration with `GOWORK=off go test -tags=integration -count=1 -timeout=20m ./...`
inside its owning module when broker/protocol behavior changes. Online
authority checks and independently released modules' own release rehearsals
remain explicit. Scheduled CI uses the shared bounded route, not an automatic
full broker matrix. Report unavailable or failing commands; bounded checks
alone do not establish release qualification.

## Adding A Module

Follow [repository structure policy](AGENTS.md#repository-structure). New modules
require an explicit purpose, ownership boundary, dependency review, package
catalog entry, full quality gates, documentation, changelog, license, security
policy, compatibility plan, and release dry-run.

## Source Documentation

Install the checksum-pinned public verifier declared in `.golib.yaml`.
`make source-docs-check` validates objective Go comment, marker, link and
generated-producer structure; `make check`, `make check-full` and `make ci`
require it before their existing checks. Shared CI runs the same strict
contract and includes its result in Required. This is structural enforcement,
not a substitute for source-truth review or behavioral verification.
