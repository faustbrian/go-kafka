# Changelog

All notable changes to this module are documented here.

## Unreleased

### Documentation

- Move detailed module guidance behind a concise README and documentation index.

### Changed

- Adopt the released `go-library-tools` v1.2.0 repository contract and
  immutable workflow `1f9629e5f27418600460b55a50a5b2fc81697fab` while
  retaining the module's package-specific interoperability operations.
## 1.0.0 - 2026-08-25

### Fixed

- Retry the documented transient Kafka observer-reentry fence during the
  overlapping-member shutdown interoperability scenario, while keeping its
  shutdown callback retry-safe and failing immediately for every other cause.

### Changed

- Publish the module from its standalone `github.com/faustbrian/go-kafka/kafkaservice` identity while preserving its documented API and behavior.
- Document lifecycle ordering, Kubernetes SIGTERM, duplicate and ambiguous
  delivery windows, API adoption, compatibility, migration, and FAQ guidance.
- Stop retaining obsolete Cobra command-line dependencies after the CLI module
  replaced its Cobra implementation.
- Require owned sibling modules at local `v0.0.0`; clean external consumers
  pin each module to an exact main pseudo-version.
- Serialize startup, readiness, publishing, and consumer run callbacks with
  resource shutdown so concurrent stop cannot close a resource still in use.
- Fence producer and consumer admission when service drain begins, before task
  cancellation, and redact ordinary callback causes while preserving error
  identity for programmatic inspection.

### Added

- pinned Apache Kafka 4.3.1 interoperability evidence for concrete producer
  startup, readiness, publication, consumer drain-before-shutdown ordering,
  pre-cancellation settlement, in-flight cancellation redelivery, and post-stop
  admission fencing
- race-enabled pinned-broker failure evidence for overlapping-member rebalance,
  slow-handler cancellation, commit timeout and redelivery, broker loss and
  recovery, partial startup rollback, and producer flush ambiguity
- independently versioned service lifecycle adapter for explicit Kafka
  producers and consumers
- bounded startup, readiness, drain, retryable shutdown, task supervision,
  correlation propagation, and caller-owned trace propagation
- secret-safe panic containment for every application callback, including
  startup cleanup and retryable shutdown after panic
- pre-copy and post-propagation record validation, bounded UTF-8 lifecycle
  names, and non-aliasing consumed-header propagation
- deterministic lifecycle, concurrency, ownership, fuzz, compatibility, and
  allocation evidence
- broker-independent producer and consumer shutdown-latency benchmarks
