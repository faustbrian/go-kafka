# Specification decisions

This register is authoritative together with `specification/decisions.json`. Source and update authorities are pinned independently in `specification/monitoring.json`.

## KAFKA-DEC-001: Protocol implementation and version negotiation boundary

- **status:** resolved
- **owner:** Kafka maintainers
- **classification:** interoperability policy
- **decision_scope:** defensive
- **specification:** Apache Kafka protocol and client semantics
- **version:** Apache Kafka 4.3.1
- **source_authority:** kafka-431-protocol
- **section:** Protocol guide and ApiVersions negotiation
- **requirement_strength:** not specified
- **issue:** The client must negotiate Kafka APIs without exposing implementation-specific protocol types or turning an upstream capability into a broker-support claim.
- **interpretations:** `Expose the upstream default directly`, `Select and document one bounded package policy`
- **peer_behavior:** Maintained Kafka clients expose materially different policy defaults while preserving the same protocol boundary.
- **selected_behavior:** franz-go owns wire negotiation; public APIs expose Kafka policy, and MinimumVersion is a downgrade floor rather than a maximum or support claim.
- **rationale:** The package must keep the observable policy stable, bounded, and attributable to the pinned authority.
- **security_consequences:** Credentials, payloads, endpoints, and unbounded upstream diagnostics remain outside the public result.
- **resource_consequences:** The selected behavior preserves the package's documented time, memory, concurrency, and input bounds.
- **compatibility_consequences:** Changing this selection requires compatibility review and a new retained decision digest.
- **wire_consequences:** Kafka wire effects remain limited to the selected protocol operation and its documented failure outcome.
- **executable_evidence:** `TestClientRolesApplyProtocolAndBoundedEOFRecoveryPolicy`, `TestClientRolesRejectInvalidProtocolPolicy`
- **fixture_evidence:** None.
- **fuzz_evidence:** `FuzzObservationValidation`
- **interoperability_evidence:** `integration_apache_test.go`
- **differential_evidence:** `specification/maintained-peer-differential.md`
- **public_apis:** `ProtocolPolicy`, `NewProducer`, `NewConsumer`, `NewInspector`
- **documentation:** `docs/specification-decisions.md`
- **upstream_status:** The pinned authority defines the protocol or provider boundary; package policy owns the narrower observable contract.
- **reconsider_when:** A monitored authority changes, a maintained peer exposes contrary evidence, or the public contract is deliberately revised.
- **authoritative_url:** https://raw.githubusercontent.com/apache/kafka/4.3.1/docs/design/protocol.md
## KAFKA-DEC-002: Producer durability, partitioning, and byte ownership

- **status:** resolved
- **owner:** Kafka maintainers
- **classification:** interoperability policy
- **decision_scope:** defensive
- **specification:** Apache Kafka protocol and client semantics
- **version:** Apache Kafka 4.3.1
- **source_authority:** kafka-431-protocol
- **section:** Produce API and producer configuration
- **requirement_strength:** not specified
- **issue:** Acknowledgement, idempotence, partition selection, cancellation, and byte ownership permit incompatible delivery and aliasing interpretations.
- **interpretations:** `Expose the upstream default directly`, `Select and document one bounded package policy`
- **peer_behavior:** Maintained Kafka clients expose materially different policy defaults while preserving the same protocol boundary.
- **selected_behavior:** Production uses bounded idempotent all-ISR delivery, explicit key or partition policy, owned input bytes, and explicit post-admission ambiguity.
- **rationale:** The package must keep the observable policy stable, bounded, and attributable to the pinned authority.
- **security_consequences:** Credentials, payloads, endpoints, and unbounded upstream diagnostics remain outside the public result.
- **resource_consequences:** The selected behavior preserves the package's documented time, memory, concurrency, and input bounds.
- **compatibility_consequences:** Changing this selection requires compatibility review and a new retained decision digest.
- **wire_consequences:** Kafka wire effects remain limited to the selected protocol operation and its documented failure outcome.
- **executable_evidence:** `TestNewProducerAppliesBoundedIdempotentDeliveryPolicy`, `TestProducerPublishRecordReturnsDeliveryMetadataAndOwnsInput`
- **fixture_evidence:** None.
- **fuzz_evidence:** `FuzzMessageValidation`
- **interoperability_evidence:** `integration_apache_test.go`
- **differential_evidence:** `specification/maintained-peer-differential.md`
- **public_apis:** `Producer`, `ProducerConfig`, `ProducerRecord`, `DeliveryResult`
- **documentation:** `docs/specification-decisions.md`
- **upstream_status:** The pinned authority defines the protocol or provider boundary; package policy owns the narrower observable contract.
- **reconsider_when:** A monitored authority changes, a maintained peer exposes contrary evidence, or the public contract is deliberately revised.
- **authoritative_url:** https://raw.githubusercontent.com/apache/kafka/4.3.1/docs/design/protocol.md

## KAFKA-DEC-003: Consumer settlement and rebalance ownership

- **status:** resolved
- **owner:** Kafka maintainers
- **classification:** interoperability policy
- **decision_scope:** defensive
- **specification:** implemented Kafka Improvement Proposals
- **version:** KIP-429 accepted
- **source_authority:** kafka-kip-429
- **section:** Incremental cooperative rebalance protocol
- **requirement_strength:** not specified
- **issue:** Fetch success, handler success, assignment ownership, and committed offsets are separate effects during a rebalance.
- **interpretations:** `Expose the upstream default directly`, `Select and document one bounded package policy`
- **peer_behavior:** Maintained Kafka clients expose materially different policy defaults while preserving the same protocol boundary.
- **selected_behavior:** Processing is partition-sequential, settlement follows handler success, stale assignment epochs cannot settle, and cooperative balancing is the default.
- **rationale:** The package must keep the observable policy stable, bounded, and attributable to the pinned authority.
- **security_consequences:** Credentials, payloads, endpoints, and unbounded upstream diagnostics remain outside the public result.
- **resource_consequences:** The selected behavior preserves the package's documented time, memory, concurrency, and input bounds.
- **compatibility_consequences:** Changing this selection requires compatibility review and a new retained decision digest.
- **wire_consequences:** Kafka wire effects remain limited to the selected protocol operation and its documented failure outcome.
- **executable_evidence:** `TestConsumerRunOnceCommitsOnlyContiguousPartitionSuccess`, `TestConsumerFencesSettlementAfterAssignmentEpochChanges`
- **fixture_evidence:** None.
- **fuzz_evidence:** `FuzzConsumerConfig`
- **interoperability_evidence:** `integration_apache_test.go`
- **differential_evidence:** `specification/maintained-peer-differential.md`
- **public_apis:** `Consumer`, `ConsumerConfig`, `Handler`, `BatchHandler`
- **documentation:** `docs/specification-decisions.md`
- **upstream_status:** The pinned authority defines the protocol or provider boundary; package policy owns the narrower observable contract.
- **reconsider_when:** A monitored authority changes, a maintained peer exposes contrary evidence, or the public contract is deliberately revised.
- **authoritative_url:** https://cwiki.apache.org/confluence/rest/api/content/103090108?expand=body.storage%2Cversion
- **additional_authoritative_sources:** `{"id":"kafka-kip-345","version":"KIP-345 accepted","url":"https://cwiki.apache.org/confluence/rest/api/content/87300241?expand=body.storage%2Cversion","specifications":["implemented Kafka Improvement Proposals"]}` `{"id":"kafka-kip-392","version":"KIP-392 accepted","url":"https://cwiki.apache.org/confluence/rest/api/content/95653762?expand=body.storage%2Cversion","specifications":["implemented Kafka Improvement Proposals"]}`

## KAFKA-DEC-004: Retry-topic and dead-letter effects

- **status:** resolved
- **owner:** Kafka maintainers
- **classification:** interoperability policy
- **decision_scope:** defensive
- **specification:** Apache Kafka protocol and client semantics
- **version:** Apache Kafka 4.3.1
- **source_authority:** kafka-431-protocol
- **section:** Delivery semantics and offset commits
- **requirement_strength:** not specified
- **issue:** Kafka has no queue nack, and publishing a retry or dead-letter record is not atomic with source settlement outside a transaction.
- **interpretations:** `Expose the upstream default directly`, `Select and document one bounded package policy`
- **peer_behavior:** Maintained Kafka clients expose materially different policy defaults while preserving the same protocol boundary.
- **selected_behavior:** Only definite failure-publication success permits source settlement; partial, failed, canceled, or ambiguous publication leaves the source unsettled.
- **rationale:** The package must keep the observable policy stable, bounded, and attributable to the pinned authority.
- **security_consequences:** Credentials, payloads, endpoints, and unbounded upstream diagnostics remain outside the public result.
- **resource_consequences:** The selected behavior preserves the package's documented time, memory, concurrency, and input bounds.
- **compatibility_consequences:** Changing this selection requires compatibility review and a new retained decision digest.
- **wire_consequences:** Kafka wire effects remain limited to the selected protocol operation and its documented failure outcome.
- **executable_evidence:** `TestFailureHandlerPublishFailureDoesNotResolveSource`, `TestFailureHandlerPublishesOwnedDeadLetterMetadata`
- **fixture_evidence:** None.
- **fuzz_evidence:** `FuzzFailureHandlerConfig`
- **interoperability_evidence:** `integration_apache_test.go`
- **differential_evidence:** `specification/maintained-peer-differential.md`
- **public_apis:** `FailureHandlerConfig`, `BatchFailureHandlerConfig`
- **documentation:** `docs/specification-decisions.md`
- **upstream_status:** The pinned authority defines the protocol or provider boundary; package policy owns the narrower observable contract.
- **reconsider_when:** A monitored authority changes, a maintained peer exposes contrary evidence, or the public contract is deliberately revised.
- **authoritative_url:** https://raw.githubusercontent.com/apache/kafka/4.3.1/docs/design/protocol.md

## KAFKA-DEC-005: Kafka-scoped transactions and exactly-once claims

- **status:** resolved
- **owner:** Kafka maintainers
- **classification:** interoperability policy
- **decision_scope:** defensive
- **specification:** implemented Kafka Improvement Proposals
- **version:** KIP-447 accepted
- **source_authority:** kafka-kip-447
- **section:** Producer scalability for exactly-once semantics
- **requirement_strength:** not specified
- **issue:** Kafka transactions can atomically bind Kafka offsets and outputs but cannot include external databases, HTTP, or object storage.
- **interpretations:** `Expose the upstream default directly`, `Select and document one bounded package policy`
- **peer_behavior:** Maintained Kafka clients expose materially different policy defaults while preserving the same protocol boundary.
- **selected_behavior:** Exactly-once language is restricted to compatible Kafka read-process-write effects; ambiguous commit outcomes remain explicit and fatal where reuse is unsafe.
- **rationale:** The package must keep the observable policy stable, bounded, and attributable to the pinned authority.
- **security_consequences:** Credentials, payloads, endpoints, and unbounded upstream diagnostics remain outside the public result.
- **resource_consequences:** The selected behavior preserves the package's documented time, memory, concurrency, and input bounds.
- **compatibility_consequences:** Changing this selection requires compatibility review and a new retained decision digest.
- **wire_consequences:** Kafka wire effects remain limited to the selected protocol operation and its documented failure outcome.
- **executable_evidence:** `TestRunTransactionRedactsUnknownCommitOutcome`, `TestTransactionProcessorCommitsCompletePollAtomically`
- **fixture_evidence:** None.
- **fuzz_evidence:** `FuzzTransactionProcessorConfig`
- **interoperability_evidence:** `integration_apache_test.go`
- **differential_evidence:** `specification/maintained-peer-differential.md`
- **public_apis:** `Transaction`, `Producer.RunTransaction`, `TransactionProcessor`
- **documentation:** `docs/specification-decisions.md`
- **upstream_status:** The pinned authority defines the protocol or provider boundary; package policy owns the narrower observable contract.
- **reconsider_when:** A monitored authority changes, a maintained peer exposes contrary evidence, or the public contract is deliberately revised.
- **authoritative_url:** https://cwiki.apache.org/confluence/rest/api/content/103093950?expand=body.storage%2Cversion
- **additional_authoritative_sources:** `{"id":"kafka-kip-98","version":"KIP-98 accepted","url":"https://cwiki.apache.org/confluence/rest/api/content/66854913?expand=body.storage%2Cversion","specifications":["implemented Kafka Improvement Proposals"]}`

## KAFKA-DEC-006: Exact direct-partition replay

- **status:** resolved
- **owner:** Kafka maintainers
- **classification:** interoperability policy
- **decision_scope:** defensive
- **specification:** Apache Kafka protocol and client semantics
- **version:** Apache Kafka 4.3.1
- **source_authority:** kafka-431-protocol
- **section:** Log retention, compaction, and fetch offsets
- **requirement_strength:** not specified
- **issue:** Retention, compaction, and truncation can leave missing offsets inside a numeric range, while group mutation would change unrelated application state.
- **interpretations:** `Expose the upstream default directly`, `Select and document one bounded package policy`
- **peer_behavior:** Maintained Kafka clients expose materially different policy defaults while preserving the same protocol boundary.
- **selected_behavior:** Replay uses direct exact ranges, never mutates a group, requires side-effect opt-in, and returns incomplete progress for every gap or unsatisfied offset.
- **rationale:** The package must keep the observable policy stable, bounded, and attributable to the pinned authority.
- **security_consequences:** Credentials, payloads, endpoints, and unbounded upstream diagnostics remain outside the public result.
- **resource_consequences:** The selected behavior preserves the package's documented time, memory, concurrency, and input bounds.
- **compatibility_consequences:** Changing this selection requires compatibility review and a new retained decision digest.
- **wire_consequences:** Kafka wire effects remain limited to the selected protocol operation and its documented failure outcome.
- **executable_evidence:** `TestInspectorPlansReplayTimestampWindowAsOwnedExactRanges`, `TestReplayFailsClosedOnUnexpectedRecordsAndOffsetGaps`
- **fixture_evidence:** None.
- **fuzz_evidence:** `FuzzReplayConfig`, `FuzzReplayTimestampRequest`
- **interoperability_evidence:** `integration_apache_test.go`
- **differential_evidence:** `specification/maintained-peer-differential.md`
- **public_apis:** `ReplayPlan`, `ReplayReader`, `ReplayCheckpoint`, `ReplayResult`
- **documentation:** `docs/specification-decisions.md`
- **upstream_status:** The pinned authority defines the protocol or provider boundary; package policy owns the narrower observable contract.
- **reconsider_when:** A monitored authority changes, a maintained peer exposes contrary evidence, or the public contract is deliberately revised.
- **authoritative_url:** https://raw.githubusercontent.com/apache/kafka/4.3.1/docs/design/protocol.md

## KAFKA-DEC-007: Read-only inspection and health separation

- **status:** resolved
- **owner:** Kafka maintainers
- **classification:** interoperability policy
- **decision_scope:** defensive
- **specification:** implemented Kafka Improvement Proposals
- **version:** KIP-848 accepted
- **source_authority:** kafka-kip-848
- **section:** Next generation consumer rebalance protocol
- **requirement_strength:** not specified
- **issue:** Classic and consumer-protocol groups expose different states, and dependency health is not equivalent to local liveness or readiness.
- **interpretations:** `Expose the upstream default directly`, `Select and document one bounded package policy`
- **peer_behavior:** Maintained Kafka clients expose materially different policy defaults while preserving the same protocol boundary.
- **selected_behavior:** Inspection is bounded and read-only, preserves partial failures, separates group protocols, and reports liveness, dependency health, and readiness independently.
- **rationale:** The package must keep the observable policy stable, bounded, and attributable to the pinned authority.
- **security_consequences:** Credentials, payloads, endpoints, and unbounded upstream diagnostics remain outside the public result.
- **resource_consequences:** The selected behavior preserves the package's documented time, memory, concurrency, and input bounds.
- **compatibility_consequences:** Changing this selection requires compatibility review and a new retained decision digest.
- **wire_consequences:** Kafka wire effects remain limited to the selected protocol operation and its documented failure outcome.
- **executable_evidence:** `TestInspectorPreservesRequestAndPartitionFailures`, `TestInspectorReadinessUsesHysteresisWithoutAffectingLiveness`
- **fixture_evidence:** None.
- **fuzz_evidence:** `FuzzInspectionConsumerProtocolGroupMetadata`
- **interoperability_evidence:** `integration_apache_test.go`
- **differential_evidence:** `specification/maintained-peer-differential.md`
- **public_apis:** `Inspector`, `ConsumerGroup`, `ConsumerProtocolGroup`, `ReadinessPolicy`
- **documentation:** `docs/specification-decisions.md`
- **upstream_status:** The pinned authority defines the protocol or provider boundary; package policy owns the narrower observable contract.
- **reconsider_when:** A monitored authority changes, a maintained peer exposes contrary evidence, or the public contract is deliberately revised.
- **authoritative_url:** https://cwiki.apache.org/confluence/rest/api/content/217387038?expand=body.storage%2Cversion

## KAFKA-DEC-008: Verified transport and rotating authentication

- **status:** resolved
- **owner:** Kafka maintainers
- **classification:** interoperability policy
- **decision_scope:** defensive
- **specification:** Apache Kafka protocol and client semantics
- **version:** Apache Kafka 4.3.1
- **source_authority:** kafka-431-protocol
- **section:** SASL authentication and transport security
- **requirement_strength:** not specified
- **issue:** Kafka mechanisms permit materially different transport and credential-lifetime policies, including unsafe PLAIN over plaintext.
- **interpretations:** `Expose the upstream default directly`, `Select and document one bounded package policy`
- **peer_behavior:** Maintained Kafka clients expose materially different policy defaults while preserving the same protocol boundary.
- **selected_behavior:** Production defaults to verified TLS 1.2 or later; PLAIN requires verified TLS; mTLS, SCRAM, and OAUTHBEARER use bounded caller-owned rotating providers.
- **rationale:** The package must keep the observable policy stable, bounded, and attributable to the pinned authority.
- **security_consequences:** Credentials, payloads, endpoints, and unbounded upstream diagnostics remain outside the public result.
- **resource_consequences:** The selected behavior preserves the package's documented time, memory, concurrency, and input bounds.
- **compatibility_consequences:** Changing this selection requires compatibility review and a new retained decision digest.
- **wire_consequences:** Kafka wire effects remain limited to the selected protocol operation and its documented failure outcome.
- **executable_evidence:** `TestClientSecurityDefaultsToVerifiedTLSAndRequiresExplicitPlaintext`, `TestTrustAnchorProviderIsBoundedOwnedRotatingAndPanicSafe`
- **fixture_evidence:** None.
- **fuzz_evidence:** `FuzzTrustAnchors`
- **interoperability_evidence:** `integration_apache_test.go`
- **differential_evidence:** `specification/maintained-peer-differential.md`
- **public_apis:** `ClientSecurity`, `Authentication`, `TrustAnchorProvider`
- **documentation:** `docs/specification-decisions.md`
- **upstream_status:** The pinned authority defines the protocol or provider boundary; package policy owns the narrower observable contract.
- **reconsider_when:** A monitored authority changes, a maintained peer exposes contrary evidence, or the public contract is deliberately revised.
- **authoritative_url:** https://raw.githubusercontent.com/apache/kafka/4.3.1/docs/design/protocol.md

## KAFKA-DEC-009: Payload-free synchronous observation

- **status:** resolved
- **owner:** Kafka maintainers
- **classification:** interoperability policy
- **decision_scope:** defensive
- **specification:** Apache Kafka protocol and client semantics
- **version:** Apache Kafka 4.3.1
- **source_authority:** kafka-431-protocol
- **section:** Client callback and request lifecycle
- **requirement_strength:** not specified
- **issue:** Client hooks can expose payloads, high-cardinality identities, internal request types, blocking, panic, and reentry behavior.
- **interpretations:** `Expose the upstream default directly`, `Select and document one bounded package policy`
- **peer_behavior:** Maintained Kafka clients expose materially different policy defaults while preserving the same protocol boundary.
- **selected_behavior:** Observations are synchronous, immutable, bounded, payload-free, panic-contained, cooperatively timed, and fenced against same-client reentry.
- **rationale:** The package must keep the observable policy stable, bounded, and attributable to the pinned authority.
- **security_consequences:** Credentials, payloads, endpoints, and unbounded upstream diagnostics remain outside the public result.
- **resource_consequences:** The selected behavior preserves the package's documented time, memory, concurrency, and input bounds.
- **compatibility_consequences:** Changing this selection requires compatibility review and a new retained decision digest.
- **wire_consequences:** Observation emits no Kafka request and cannot alter wire behavior.
- **executable_evidence:** `TestObservationValidationRejectsMetadataOutsideThePublicContract`, `TestObserverFailuresAreContainedAndReportedInOrder`
- **fixture_evidence:** None.
- **fuzz_evidence:** `FuzzObservationValidation`
- **interoperability_evidence:** `integration_apache_test.go`
- **differential_evidence:** `specification/maintained-peer-differential.md`
- **public_apis:** `Observation`, `ObserverPolicy`, `ObserverFunc`
- **documentation:** `docs/specification-decisions.md`
- **upstream_status:** The pinned authority defines the protocol or provider boundary; package policy owns the narrower observable contract.
- **reconsider_when:** A monitored authority changes, a maintained peer exposes contrary evidence, or the public contract is deliberately revised.
- **authoritative_url:** https://raw.githubusercontent.com/apache/kafka/4.3.1/docs/design/protocol.md
