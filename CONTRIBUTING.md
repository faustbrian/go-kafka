# Contributing

## Before Editing

1. Read [`AGENTS.md`](AGENTS.md) and the affected module's goals and docs.
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

Run the affected module gate during development:

```bash
make inventory
golib check --module <directory>
```

Before submitting a repository-wide change:

```bash
make ci
```

The full scheduled and release gate is `make ci`. Run it only for a repository
release or repository-wide change. Report every unavailable or failing
command; do not describe partial results as release-ready.

## Adding A Module

Follow [repository structure policy](AGENTS.md#repository-structure). New modules
require an explicit purpose, ownership boundary, dependency review, package
catalog entry, full quality gates, documentation, changelog, license, security
policy, compatibility plan, and release dry-run.
