# Specification conformance matrix

The [decision register](../docs/specification-decisions.md) binds every observable decision to pinned authorities and current evidence.

| Decision | Authority | Executable evidence | Differential evidence |
| --- | --- | --- | --- |
| KAFKA-OTEL-DEC-001 | `otel-semconv-144` | `TestEveryObservationHasAnExactPublicSpanAndDurationContract`, `TestCompletionObservationsDoNotClaimUnprovedMessagingOperations` | `adapters/gotelemetry/specification/maintained-peer-differential.md` |
| KAFKA-OTEL-DEC-002 | `otel-semconv-144` | `TestStandardMetricsUseOnlyPinnedConventionDimensions`, `TestTraceContextPropagationRejectsInvalidOrAmbiguousFields` | `adapters/gotelemetry/specification/maintained-peer-differential.md` |
| KAFKA-OTEL-DEC-003 | `otel-go-144` | `TestAdapterProductionStartsNoGoroutines`, `TestObserverContainsHostileProviderPanicsAndClosesStartedSpan` | `adapters/gotelemetry/specification/maintained-peer-differential.md` |
