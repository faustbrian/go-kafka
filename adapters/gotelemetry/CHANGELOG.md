# Changelog

All notable changes to this module are documented here.

## Unreleased

### Documentation

- Move detailed module guidance behind a concise README and documentation index.

### Changed

- Publish schema-v2 cohesion metadata and versioned ecosystem navigation for
  the independently released OpenTelemetry adapter.
- Register and enforce the adapter's OpenTelemetry decisions in the
  [specification decision register](docs/specification-decisions.md):
  `KAFKA-OTEL-DEC-001 sha256:d362738340cab3e5fe46a98b1b5ada8311b1f09a81db9888ac4cb5ae2292c731`,
  `KAFKA-OTEL-DEC-002 sha256:0008db30bf7af660c609405faadea2f5d366ea1564172a37f9a0c1da30f86463`, and
  `KAFKA-OTEL-DEC-003 sha256:f6c97b3949856b5eb9bc59cb86c92bf44f15932240fa2b159704b5c50574567c`.
- Adopt the released `go-library-tools` v1.2.0 repository contract and
  immutable workflow `1f9629e5f27418600460b55a50a5b2fc81697fab` while
  retaining the module's specification and interoperability operations.
## 1.0.0 - 2026-08-25

### Fixed

- make documentation and specification checks work on stock CI runners without
  a nonstandard search tool
- suppress unproved standard producer-send and poll-receive signals, restrict
  remaining standard metric dimensions to the pinned 1.44.0 schemas, keep
  adapter metrics identity-free, fail closed on unmapped observations, and
  contain provider panics without exposing panic values
- keep skipped and pre-handler replay failures out of OpenTelemetry application
  processing and delivery telemetry
- keep consumer record and batch observations out of standard processing
  telemetry because pre-handler exits and handler failures are indistinguishable

### Added

- add an auditable specification decision register for span semantics,
  identity and propagation bounds, and caller-owned provider lifecycle
- exhaustive span, metric, attribute, lifecycle, sampling, shutdown,
  concurrency, fuzz, and benchmark contracts plus enforced API, privacy, FAQ,
  and migration documentation
- explicit, concurrency-safe W3C Trace Context injection and extraction for
  bounded Kafka records, with defensive producer copies, borrowed-consumer
  ownership, fail-closed duplicate fields, no baggage, and no global
  OpenTelemetry state
- pinned Apache Kafka 4.3.1 producer-to-consumer integration evidence that
  preserves and extracts the same remote W3C span context before source-offset
  settlement
- internal spans and adapter-owned duration metrics for the bounded local wait
  from blocked-callback entry through poll-gate release or failure
- internal spans and adapter-owned operation metrics for bounded consumer retry
  decisions without double-counting semantic consumption or processing
- bounded `kafka.authentication.method` broker-connect span attributes that
  identify the configured SASL flow without credentials or a fabricated
  standalone authentication span
- `INTERNAL` spans for producer, consumer, and consume-transform-produce
  shutdown-attempt observations
- `CLIENT` spans and bounded adapter-owned diagnostics for cluster, topic,
  consumer-group, and dependency-health queries, with `INTERNAL` readiness and
  inspector-shutdown spans
- replay plan, record-processing, exact aggregate progress, and shutdown spans
  plus fixed replay progress attributes
- independently versioned OpenTelemetry adapter for every stable root Kafka
  observation
- OpenTelemetry messaging semantic-convention 1.44.0 spans and metrics only
  where root observations prove replay processing, settle, and pulled-message
  semantics
- explicit deny-by-default client, topic, and consumer-group attribute
  allowlists with bounded validation and defensive copies
- adapter-owned broker request-size, queue-duration, throttle, lifecycle, and
  transaction telemetry without record data, endpoints, credentials, or
  application error text
- exact operation timestamps, stable redacted error categories, concurrent
  observer safety, fuzz targets, race coverage, and allocation benchmarks

### Changed

- Publish the module from its standalone `github.com/faustbrian/go-kafka/adapters/gotelemetry` identity while preserving its documented API and behavior.
- select and record OpenTelemetry messaging semantic conventions 1.44.0,
  retaining explicit completion-observer boundaries for cluster identity,
  create/client-send spans, and creation-context links
- Require owned sibling modules at local `v0.0.0`; clean external consumers
  pin each module to an exact main pseudo-version.

- validate public observations through the root Kafka contract so adapters do
  not define divergent settlement or cardinality rules
