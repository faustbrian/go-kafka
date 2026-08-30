# Specification decisions

This register is authoritative together with `specification/decisions.json`. Source and update authorities are pinned independently in `specification/monitoring.json`.

## KAFKA-OTEL-DEC-001: Span semantics follow observed lifecycle boundaries

- **status:** resolved
- **owner:** Kafka OpenTelemetry adapter maintainers
- **classification:** interoperability policy
- **decision_scope:** defensive
- **specification:** OpenTelemetry semantic conventions 1.44.0 for Kafka messaging spans and metrics
- **version:** OpenTelemetry semantic conventions v1.44.0
- **source_authority:** otel-semconv-144
- **section:** Kafka messaging spans
- **requirement_strength:** not specified
- **issue:** A root completion observation does not prove a narrower producer send, receive, or process lifecycle boundary.
- **interpretations:** `Expose the upstream default directly`, `Select and document one bounded package policy`
- **peer_behavior:** Maintained OpenTelemetry client instrumentations start spans at different client-hook boundaries and therefore cannot justify inferred operation spans here.
- **selected_behavior:** Completion, poll, and record observations use span kind NONE unless the root observation proves the complete standard operation.
- **rationale:** The package must keep the observable policy stable, bounded, and attributable to the pinned authority.
- **security_consequences:** Credentials, payloads, endpoints, and unbounded upstream diagnostics remain outside the public result.
- **resource_consequences:** The selected behavior preserves the package's documented time, memory, concurrency, and input bounds.
- **compatibility_consequences:** Changing this selection requires compatibility review and a new retained decision digest.
- **wire_consequences:** Observation mapping emits no Kafka wire traffic; propagation changes only the explicitly owned trace context headers.
- **executable_evidence:** `TestEveryObservationHasAnExactPublicSpanAndDurationContract`, `TestCompletionObservationsDoNotClaimUnprovedMessagingOperations`
- **fixture_evidence:** None.
- **fuzz_evidence:** `FuzzObserverValidation`
- **interoperability_evidence:** None.
- **differential_evidence:** `adapters/gotelemetry/specification/maintained-peer-differential.md`
- **public_apis:** `Observer`, `Instrumentation`
- **documentation:** `adapters/gotelemetry/docs/specification-decisions.md`
- **upstream_status:** The pinned authority defines the protocol or provider boundary; package policy owns the narrower observable contract.
- **reconsider_when:** A monitored authority changes, a maintained peer exposes contrary evidence, or the public contract is deliberately revised.
- **authoritative_url:** https://raw.githubusercontent.com/open-telemetry/semantic-conventions/e10a930844c6951757a43b849d364f7d056ac32b/docs/messaging/kafka.md

## KAFKA-OTEL-DEC-002: Identity and propagation are explicit and bounded

- **status:** resolved
- **owner:** Kafka OpenTelemetry adapter maintainers
- **classification:** interoperability policy
- **decision_scope:** defensive
- **specification:** OpenTelemetry semantic conventions 1.44.0 for Kafka messaging spans and metrics
- **version:** OpenTelemetry semantic conventions v1.44.0
- **source_authority:** otel-semconv-144
- **section:** Messaging attributes and W3C propagation
- **requirement_strength:** not specified
- **issue:** Semantic conventions permit identities and propagation while leaving application disclosure, duplicate-header, and Kafka-header bounds to instrumentation policy.
- **interpretations:** `Expose the upstream default directly`, `Select and document one bounded package policy`
- **peer_behavior:** Maintained OpenTelemetry propagators agree on valid W3C context but client instrumentations differ on identity cardinality and duplicate-header handling.
- **selected_behavior:** Standard dimensions are allowlisted; canonical traceparent and tracestate values reject ambiguity, clear stale owned fields, and preserve unrelated headers.
- **rationale:** The package must keep the observable policy stable, bounded, and attributable to the pinned authority.
- **security_consequences:** Credentials, payloads, endpoints, and unbounded upstream diagnostics remain outside the public result.
- **resource_consequences:** The selected behavior preserves the package's documented time, memory, concurrency, and input bounds.
- **compatibility_consequences:** Changing this selection requires compatibility review and a new retained decision digest.
- **wire_consequences:** Observation mapping emits no Kafka wire traffic; propagation changes only the explicitly owned trace context headers.
- **executable_evidence:** `TestStandardMetricsUseOnlyPinnedConventionDimensions`, `TestTraceContextPropagationRejectsInvalidOrAmbiguousFields`
- **fixture_evidence:** None.
- **fuzz_evidence:** `FuzzAttributePolicyValidate`, `FuzzTraceContextPropagation`
- **interoperability_evidence:** None.
- **differential_evidence:** `adapters/gotelemetry/specification/maintained-peer-differential.md`
- **public_apis:** `AttributePolicy`, `TraceContextPropagation`
- **documentation:** `adapters/gotelemetry/docs/specification-decisions.md`
- **upstream_status:** The pinned authority defines the protocol or provider boundary; package policy owns the narrower observable contract.
- **reconsider_when:** A monitored authority changes, a maintained peer exposes contrary evidence, or the public contract is deliberately revised.
- **authoritative_url:** https://raw.githubusercontent.com/open-telemetry/semantic-conventions/e10a930844c6951757a43b849d364f7d056ac32b/docs/messaging/kafka.md

## KAFKA-OTEL-DEC-003: Providers remain caller-owned and failure-contained

- **status:** resolved
- **owner:** Kafka OpenTelemetry adapter maintainers
- **classification:** interoperability policy
- **decision_scope:** defensive
- **specification:** OpenTelemetry Go API and SDK 1.44.0
- **version:** OpenTelemetry Go v1.44.0
- **source_authority:** otel-go-144
- **section:** TracerProvider and MeterProvider lifecycle
- **requirement_strength:** not specified
- **issue:** The API permits global or caller-owned providers while exporters may block or panic; it does not assign adapter shutdown ownership.
- **interpretations:** `Expose the upstream default directly`, `Select and document one bounded package policy`
- **peer_behavior:** Maintained OpenTelemetry integrations vary between global bootstrap and explicit providers; the Go API supports the explicit caller-owned boundary selected here.
- **selected_behavior:** Construction requires explicit providers, installs no globals, starts no goroutines, never shuts down caller state, and contains provider panics.
- **rationale:** The package must keep the observable policy stable, bounded, and attributable to the pinned authority.
- **security_consequences:** Credentials, payloads, endpoints, and unbounded upstream diagnostics remain outside the public result.
- **resource_consequences:** The selected behavior preserves the package's documented time, memory, concurrency, and input bounds.
- **compatibility_consequences:** Changing this selection requires compatibility review and a new retained decision digest.
- **wire_consequences:** Observation mapping emits no Kafka wire traffic; propagation changes only the explicitly owned trace context headers.
- **executable_evidence:** `TestAdapterProductionStartsNoGoroutines`, `TestObserverContainsHostileProviderPanicsAndClosesStartedSpan`
- **fixture_evidence:** None.
- **fuzz_evidence:** `FuzzHostileProviders`
- **interoperability_evidence:** None.
- **differential_evidence:** `adapters/gotelemetry/specification/maintained-peer-differential.md`
- **public_apis:** `Config`, `New`, `Observer`
- **documentation:** `adapters/gotelemetry/docs/specification-decisions.md`
- **upstream_status:** The pinned authority defines the protocol or provider boundary; package policy owns the narrower observable contract.
- **reconsider_when:** A monitored authority changes, a maintained peer exposes contrary evidence, or the public contract is deliberately revised.
- **authoritative_url:** https://raw.githubusercontent.com/open-telemetry/opentelemetry-go/b62d92831b2dd142f5a0cc89c828270274196877/go.mod
