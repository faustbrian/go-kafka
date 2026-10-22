# Source documentation audit

[Documentation map](index.md) · [Repository README](../README.md)
[Next: API](api.md)

This is a historical audit of the v1 source at the base below. Its
`adapters/golog`, `adapters/gotelemetry` and `kafkaservice` rows describe paths
v2 removes. Its adapter module rows carry the pre-v2 module paths.

## Acceptance, ownership, direct consumers and bounded checks

Base 895add4c2e19dc49162420fddcd9b7e07fa97d47, freshly fetched remote main in a clean detached task-owned worktree. Tier A comments/docs only.
The root owns bounded Kafka policy over franz-go; optional adapters own auth,
observability or service seams. Applications own broker/ACL/topic/credential
policy, external effects, callbacks and shutdown orchestration. Root catalogs
identify CloudEvents, Event Sourcing, Outbox and Service direct consumers; nested
modules retain independent release/import identities and public dependency pins.

Named risks: record copying/borrowed callbacks, producer admission versus broker
receipt, transaction commit uncertainty, offsets/rebalance fences and pause/close
lifetimes, replay retention/ranges/checkpoints, hostile fetch/decompression,
provider/TLS/credential time bounds and redaction, observer reentry/cardinality,
service teardown ownership and deprecated facade compatibility.

Smallest checks: every tracked Go/module/package/variant/helper inventory; source-
backed meaningful public/private contracts; all-source AST/raw-token/literal/
directive equality and affected formatting; parsed HTML/native package renders;
configured native docs/example routes without duplicate example runs, frozen
structural API baselines and typed decision/history identities; local links/docs/
source/offline specs; pinned full/changed/staged secrets and ordinary final-diff
truth/usefulness review. No runtime/dependency/history/fixture repair or normative
expectation rewrite; no broker/live AWS/race/fuzz/benchmark/consumer/release campaign.
All Go/native verification awaits the explicit serialized coordinator slot and
uses three owned disposable caches with immediate cleanup on every path.

## Missing versus technical investigation

Canonical headers exist but need source-backed ownership/non-goals/version and
lifecycle detail where thin. Missing named public fields/interface methods are
listed below; retain existing meaningful contracts. Investigation precedes prose
for callback-under-lock, Close/Wait timeout behavior, transaction/offset uncertainty,
observer reentry, fetch bounds and IAM/SDK erasure/trust assumptions. Deprecation
headers and historical authoritative decisions are not runtime-repair permission.

## Native module/package/API/generator provenance

- Module .: github.com/faustbrian/go-kafka; Go minimum 1.27.0; specifications: Apache Kafka protocol and client semantics, implemented Kafka Improvement Proposals.
- Module adapters/gotelemetry: github.com/faustbrian/go-kafka/adapters/gotelemetry; Go minimum 1.27.0; specifications: none declared.
- Module adapters/mskiam: github.com/faustbrian/go-kafka/adapters/mskiam; Go minimum 1.27.0; specifications: AWS MSK IAM SASL/OAUTHBEARER signer contract v1.0.4, AWS SDK for Go v2 credential-provider contract v1.43.0.
- Module benchmarks/clients: github.com/faustbrian/go-kafka/benchmarks/clients; Go minimum 1.27.0; specifications: none declared.
- Module kafkaservice: github.com/faustbrian/go-kafka/kafkaservice; Go minimum 1.27.0; specifications: none declared.
- Module adapters/otel: github.com/faustbrian/go-kafka/adapters/otel; Go minimum 1.27.0; specifications: none declared.
- Module adapters/service: github.com/faustbrian/go-kafka/adapters/service; Go minimum 1.27.0; specifications: none declared.
- Package . / .: kafka (public).
- Package . / adapters/golog: golog (public).
- Package adapters/gotelemetry / .: gotelemetry (public).
- Package adapters/mskiam / .: mskiam (public).
- Package benchmarks/clients / .: clients (harness).
- Package kafkaservice / .: kafkaservice (public).
- Package . / kafkatest: kafkatest (test support).
- Package . / adapters/slog: kafkaslog (public).
- Package adapters/otel / .: kafkaotel (public).
- Package adapters/service / .: kafkaservice (public).

Repository toolchain is Go1.27.2. Root go.work/go.work.sum and every independent
go.mod/go.sum remain frozen. Six configured apidiff baselines are binary structural
inputs, not full-prose GoDoc snapshots: root, gotelemetry, mskiam, otel, service
and deprecated kafkaservice. No Go generated/go:generate producer was found.
Native root and module verification/package.mk/scripts define docs/package-
contract operations; broker comparison/interoperability targets are not docs proof.
Immutable shared CI Tools v2.3.0 identity is retained; parent owns enforcement.
Two source-decision registers (root and MSK IAM) and pinned official/peer manifests
remain normative historical inputs. No online source or desired expectation update.

Reproduce the source inventory with `git ls-files '*.go'`; module/package
ownership comes from `modules.json` and `packages.json`, not active build tags.
Inspect `git ls-files '*go.mod' '*go.sum' '*go.work*'` for dependency inputs
and `.golib.yaml` plus `verification/package.mk`/scripts for native producers.

## Every tracked Go file

| Path | Package | Role / prewrite missing public members and retained entrypoints |
| --- | --- | --- |
| a_batch_failure_guard_test.go | kafka | retained 1 tests |
| a_client_guard_test.go | kafka | retained 1 tests |
| a_consumer_failure_guard_test.go | kafka | retained 1 tests |
| a_consumer_guard_test.go | kafka | retained 2 tests |
| a_inspector_guard_test.go | kafka | retained 1 tests |
| a_observer_guard_test.go | kafka | retained 1 tests |
| a_producer_guard_test.go | kafka | retained 1 tests |
| a_replay_guard_test.go | kafka | retained 2 tests |
| a_transaction_processor_guard_test.go | kafka | retained 1 tests |
| adapters/golog/example_test.go | golog_test | retained 0 tests; Example |
| adapters/golog/facade_internal_test.go | golog | retained 2 tests |
| adapters/golog/facade_test.go | golog_test | retained 1 tests |
| adapters/golog/observer.go | golog | source API/private invariants; retained deprecation marker |
| adapters/gotelemetry/benchmark_test.go | gotelemetry | retained 0 tests; BenchmarkObserver; BenchmarkObserverModes; BenchmarkTraceContextPropagation |
| adapters/gotelemetry/concurrency_test.go | gotelemetry | retained 2 tests |
| adapters/gotelemetry/configuration_test.go | gotelemetry | retained 5 tests |
| adapters/gotelemetry/example_test.go | gotelemetry_test | retained 0 tests; Example; ExampleTraceContextPropagation |
| adapters/gotelemetry/facade_internal_test.go | gotelemetry | retained 3 tests |
| adapters/gotelemetry/instrumentation.go | gotelemetry | source API/private invariants; missing: Runtime.TracerProvider, Runtime.MeterProvider, AttributePolicy.AllowedClientIDs, AttributePolicy.AllowedTopics, AttributePolicy.AllowedConsumerGroups, Config.Runtime, Config.Attributes; retained deprecation marker |
| adapters/gotelemetry/instrumentation_failure_test.go | gotelemetry | retained 1 tests |
| adapters/gotelemetry/interoperability_test.go | gotelemetry_test | build variant interoperability; retained 1 tests |
| adapters/gotelemetry/lifecycle_contract_test.go | gotelemetry | retained 4 tests |
| adapters/gotelemetry/observation_contract_test.go | gotelemetry | retained 2 tests |
| adapters/gotelemetry/observer_test.go | gotelemetry | retained 9 tests |
| adapters/gotelemetry/propagation.go | gotelemetry | source API/private invariants |
| adapters/gotelemetry/propagation_test.go | gotelemetry | retained 5 tests |
| adapters/mskiam/a_mutation_guard_test.go | mskiam | retained 4 tests |
| adapters/mskiam/benchmark_test.go | mskiam_test | retained 0 tests; BenchmarkTokenGeneration; BenchmarkTokenContention; BenchmarkExternalCredentialRetrieval |
| adapters/mskiam/concurrency_test.go | mskiam_test | retained 1 tests |
| adapters/mskiam/configuration_test.go | mskiam_test | retained 6 tests |
| adapters/mskiam/credential_sources_test.go | mskiam_test | retained 4 tests |
| adapters/mskiam/example_test.go | mskiam_test | retained 0 tests; Example |
| adapters/mskiam/fuzz_test.go | mskiam | retained 0 tests; FuzzConfigValidate; FuzzTokenResultValidation |
| adapters/mskiam/hardening_test.go | mskiam | retained 9 tests |
| adapters/mskiam/load_contract_test.go | mskiam_test | retained 1 tests |
| adapters/mskiam/msk_compatibility_test.go | mskiam_test | build variant msk; retained 3 tests |
| adapters/mskiam/provider.go | mskiam | source API/private invariants; missing: Config.Region, Config.CredentialsProvider, Config.TokenTimeout; retained deprecation marker |
| adapters/mskiam/provider_internal_test.go | mskiam | retained 11 tests |
| adapters/mskiam/provider_test.go | mskiam_test | retained 2 tests |
| adapters/otel/a_mutation_guard_test.go | kafkaotel | retained 2 tests |
| adapters/otel/benchmark_test.go | kafkaotel | retained 0 tests; BenchmarkObserver; BenchmarkObserverModes; BenchmarkTraceContextPropagation |
| adapters/otel/concurrency_test.go | kafkaotel | retained 2 tests |
| adapters/otel/configuration_test.go | kafkaotel | retained 5 tests |
| adapters/otel/example_test.go | kafkaotel_test | retained 0 tests; Example; ExampleTraceContextPropagation |
| adapters/otel/fuzz_test.go | kafkaotel | retained 0 tests; FuzzAttributePolicyValidate; FuzzHostileProviders; FuzzObserverValidation; FuzzTraceContextPropagation |
| adapters/otel/hardening_contract_test.go | kafkaotel | retained 10 tests |
| adapters/otel/instrumentation.go | kafkaotel | source API/private invariants; missing: Runtime.TracerProvider, Runtime.MeterProvider, AttributePolicy.AllowedClientIDs, AttributePolicy.AllowedTopics, AttributePolicy.AllowedConsumerGroups, Config.Runtime, Config.Attributes |
| adapters/otel/instrumentation_failure_test.go | kafkaotel | retained 1 tests |
| adapters/otel/interoperability_test.go | kafkaotel_test | build variant interoperability; retained 1 tests |
| adapters/otel/lifecycle_contract_test.go | kafkaotel | retained 4 tests |
| adapters/otel/observation_contract_test.go | kafkaotel | retained 2 tests |
| adapters/otel/observer_test.go | kafkaotel | retained 9 tests |
| adapters/otel/propagation.go | kafkaotel | source API/private invariants |
| adapters/otel/propagation_test.go | kafkaotel | retained 7 tests |
| adapters/otel/successor_contract_test.go | kafkaotel_test | retained 1 tests |
| adapters/otel/validation_test.go | kafkaotel | retained 4 tests |
| adapters/service/a_lifecycle_stop_test.go | kafkaservice_test | retained 3 tests |
| adapters/service/api_compat_test.go | kafkaservice_test | retained 0 tests |
| adapters/service/benchmark_test.go | kafkaservice_test | retained 0 tests; BenchmarkProducerPublish; BenchmarkLifecycleShutdown |
| adapters/service/credential_expiry_interoperability_test.go | kafkaservice_test | build variant interoperability; retained 2 tests |
| adapters/service/example_test.go | kafkaservice_test | retained 0 tests; ExampleNewProducer |
| adapters/service/fuzz_test.go | kafkaservice_test | retained 0 tests; FuzzHandlerCorrelationBoundary |
| adapters/service/interoperability_test.go | kafkaservice_test | build variant interoperability; retained 3 tests |
| adapters/service/kafkaservice.go | kafkaservice | source API/private invariants |
| adapters/service/kafkaservice_test.go | kafkaservice_test | retained 22 tests |
| adapters/service/lifecycle_hardening_test.go | kafkaservice_test | retained 8 tests |
| adapters/service/successor_contract_test.go | kafkaservice_test | retained 1 tests |
| adapters/slog/a_observer_guard_test.go | kafkaslog | retained 1 tests |
| adapters/slog/benchmark_test.go | kafkaslog | retained 0 tests; BenchmarkObserver |
| adapters/slog/example_test.go | kafkaslog_test | retained 0 tests; Example |
| adapters/slog/fuzz_test.go | kafkaslog | retained 0 tests; FuzzIdentityPolicyValidate; FuzzObserverValidation |
| adapters/slog/observer.go | kafkaslog | source API/private invariants |
| adapters/slog/observer_test.go | kafkaslog | retained 12 tests |
| adapters/slog/successor_contract_test.go | kafkaslog_test | retained 1 tests |
| benchmark_test.go | kafka | retained 0 tests; BenchmarkBoundedFetchDecompression; BenchmarkMessageValidation; BenchmarkProducerPolicyOverhead; BenchmarkConsumerPolicyOverhead; BenchmarkTransactionProcessorPolicyOverhead; BenchmarkFailureHandlerSuccess; BenchmarkBatchFailureHandlerSuccess; BenchmarkConsumerPartitionWorkers; BenchmarkReplayProgress; BenchmarkInspectorTopicState; BenchmarkInspectorConsumerGroupState; BenchmarkInspectorReadiness |
| benchmarks/clients/consumer_benchmark_test.go | clients_test | build variant integration; retained 3 tests; BenchmarkEquivalentConsumerHandling |
| benchmarks/clients/consumer_cross_partition_benchmark_test.go | clients_test | build variant integration; retained 3 tests; BenchmarkEquivalentCrossPartitionConsumerHandling |
| benchmarks/clients/doc.go | clients | source API/private invariants |
| benchmarks/clients/inspection_benchmark_test.go | clients_test | build variant integration; retained 1 tests; BenchmarkEquivalentInspection |
| benchmarks/clients/producer_benchmark_test.go | clients_test | build variant integration; retained 6 tests; BenchmarkEquivalentSynchronousProduce; BenchmarkEquivalentSynchronousBatchProduce; BenchmarkEquivalentAsynchronousProduce; TestMain lifecycle must be inspected before selecting execution |
| benchmarks/clients/producer_multi_partition_benchmark_test.go | clients_test | build variant integration; retained 2 tests; BenchmarkEquivalentMultiPartitionProduce |
| benchmarks/clients/rebalance_benchmark_test.go | clients_test | build variant integration; retained 1 tests; BenchmarkEquivalentConsumerRebalance |
| benchmarks/clients/replay_benchmark_test.go | clients_test | build variant integration; retained 1 tests; BenchmarkEquivalentReplay |
| benchmarks/clients/resource_benchmark_test.go | clients_test | build variant integration; retained 2 tests; BenchmarkEquivalentInspectionReconnect; BenchmarkEquivalentInspectionIdleResources |
| benchmarks/clients/tls_benchmark_test.go | clients_test | build variant integration; retained 2 tests; BenchmarkEquivalentTLSSynchronousProduce; BenchmarkEquivalentTLSConnectProduceClose; BenchmarkEquivalentAuthenticatedSynchronousProduce; BenchmarkEquivalentAuthenticatedConnectProduceClose |
| benchmarks/clients/transaction_processor_benchmark_test.go | clients_test | build variant integration; retained 1 tests; BenchmarkEquivalentConsumeTransformProduce |
| benchmarks/clients/transaction_producer_benchmark_test.go | clients_test | build variant integration; retained 2 tests; BenchmarkEquivalentTransactionalProduce |
| client.go | kafka | source API/private invariants; missing: UsernamePassword.Username, UsernamePassword.Password, UsernamePassword.AuthorizationID, UsernamePasswordProvider.Credentials, OAuthBearerToken.Token, OAuthBearerToken.ExpiresAt, OAuthBearerToken.AuthorizationID, OAuthBearerToken.Extensions, OAuthBearerProvider.Token, ClientCertificateRequest.AcceptableCAs, ClientCertificateRequest.SignatureSchemes, ClientCertificateRequest.Version, ClientCertificateProvider.ClientCertificate, TrustAnchors.Certificates, TrustAnchorProvider.TrustAnchors, ClientSecurity.Transport, ClientSecurity.TLS, ClientSecurity.Authentication, ClientSecurity.ClientCertificateProvider, ClientSecurity.TrustAnchorProvider, ClientSecurity.CredentialTimeout |
| client_test.go | kafka | retained 27 tests |
| compress_interoperability_test.go | kafka | retained 2 tests |
| consumer.go | kafka | source API/private invariants; missing: ConsumerConfig.Brokers, ConsumerConfig.ClientID, ConsumerConfig.Protocol, ConsumerConfig.GroupID, ConsumerConfig.InstanceID, ConsumerConfig.Rack, ConsumerConfig.Topics, ConsumerConfig.ResetOffset, ConsumerConfig.BalancePolicy, ConsumerConfig.RebalanceHandler, ConsumerConfig.Limits, ConsumerConfig.MaxPollRecords, ConsumerConfig.MaxPausedPartitions, ConsumerConfig.MaxAssignedPartitions, ConsumerConfig.MaxConcurrentFetches, ConsumerConfig.FetchMaxBytes, ConsumerConfig.FetchMaxPartitionBytes, ConsumerConfig.FetchMaxWait, ConsumerConfig.SessionTimeout, ConsumerConfig.RebalanceTimeout, ConsumerConfig.HeartbeatInterval, ConsumerConfig.HandlerTimeout, ConsumerConfig.CommitTimeout, ConsumerConfig.ShutdownTimeout, ConsumerConfig.DialTimeout, ConsumerConfig.Security, ConsumerConfig.Observers, Handler.Handle, PollResult.Polled, PollResult.Processed, PollResult.Committed, ConsumerAssignment.Epoch, ConsumerAssignment.Partitions, ConsumerAssignment.Lost |
| consumer_assignment.go | kafka | source API/private invariants |
| consumer_batch.go | kafka | source API/private invariants; missing: ConsumedBatch.Topic, ConsumedBatch.Partition, ConsumedBatch.Records, BatchHandler.HandleBatch |
| consumer_batch_failure.go | kafka | source API/private invariants; missing: BatchFailure.Batch, BatchFailure.Attempt, BatchFailure.Category, BatchFailureDelegate.HandleBatchFailure, BatchFailurePublisher.PublishBatch, BatchFailureHandlerConfig.Handler, BatchFailureHandlerConfig.Classifier, BatchFailureHandlerConfig.Retry, BatchFailureHandlerConfig.Mode, BatchFailureHandlerConfig.Target, BatchFailureHandlerConfig.Publisher, BatchFailureHandlerConfig.Delegate, BatchFailureHandlerConfig.Limits, BatchFailureHandlerConfig.MaxBatchRecords, BatchFailureHandlerConfig.MaxBatchBytes, BatchFailureHandlerConfig.PublishTimeout |
| consumer_batch_failure_example_test.go | kafka_test | retained 0 tests; ExampleNewBatchFailureHandler |
| consumer_batch_failure_test.go | kafka | retained 17 tests |
| consumer_batch_test.go | kafka | retained 15 tests |
| consumer_drain_test.go | kafka | retained 10 tests |
| consumer_failure.go | kafka | source API/private invariants; missing: FailureRetryPolicy.MaxAttempts, FailureRetryPolicy.InitialBackoff, FailureRetryPolicy.MaxBackoff, FailureRetryPolicy.Categories, FailureTarget.Topic, FailureTarget.Version, HandlerFailure.Record, HandlerFailure.Attempt, HandlerFailure.Category, FailureClassifier.ClassifyFailure, FailureDelegate.HandleFailure, FailurePublisher.PublishRecord, FailureHandlerConfig.Handler, FailureHandlerConfig.Classifier, FailureHandlerConfig.Retry, FailureHandlerConfig.Mode, FailureHandlerConfig.Target, FailureHandlerConfig.Publisher, FailureHandlerConfig.Delegate, FailureHandlerConfig.Limits, FailureHandlerConfig.PublishTimeout |
| consumer_failure_test.go | kafka | retained 21 tests |
| consumer_observer_test.go | kafka | retained 31 tests |
| consumer_parallel.go | kafka | source API/private invariants |
| consumer_parallel_test.go | kafka | retained 12 tests |
| consumer_rebalance.go | kafka | source API/private invariants |
| consumer_test.go | kafka | retained 50 tests |
| error.go | kafka | source API/private invariants |
| error_test.go | kafka | retained 6 tests |
| export_test.go | kafka | retained 0 tests |
| fetch_safety.go | kafka | source API/private invariants |
| fetch_safety_test.go | kafka | retained 16 tests |
| fuzz_test.go | kafka | retained 0 tests; FuzzObservationValidation; FuzzFetchDecompression; FuzzTrustAnchors; FuzzConsumerConfig; FuzzFailureHandlerConfig; FuzzBatchFailureHandlerConfig; FuzzMessageValidation; FuzzTransactionProcessorConfig; FuzzReplayConfig; FuzzReplayTimestampRequest; FuzzInspectionBrokerMetadata; FuzzInspectionTopicPolicyConfig; FuzzInspectionConsumerGroupMetadata; FuzzInspectionConsumerProtocolGroupMetadata |
| inspector.go | kafka | source API/private invariants; missing: InspectorConfig.Brokers, InspectorConfig.ClientID, InspectorConfig.Protocol, InspectorConfig.Security, InspectorConfig.DialTimeout, InspectorConfig.RequestTimeout, InspectorConfig.MaxMetadataBrokers, InspectorConfig.MaxMetadataPartitions, InspectorConfig.MaxGroupMembers, InspectorConfig.Readiness, ReadinessPolicy.FailureThreshold, ReadinessPolicy.RecoveryThreshold, ReadinessState.Ready, ReadinessState.DependencyHealthy, ReadinessState.ConsecutiveFailures, ReadinessState.ConsecutiveSuccesses, LivenessState.Live, BrokerState.NodeID, BrokerState.Host, BrokerState.Port, BrokerState.Rack, ClusterState.ID, ClusterState.IDVisible, ClusterState.ControllerID, ClusterState.ControllerVisible, ClusterState.Brokers, TopicPartitionState.Partition, TopicPartitionState.Leader, TopicPartitionState.LeaderEpoch, TopicPartitionState.Replicas, TopicPartitionState.InSyncReplicaIDs, TopicPartitionState.OfflineReplicaIDs, TopicPartitionState.ReplicationFactor, TopicPartitionState.InSyncReplicas, TopicPartitionState.OfflineReplicas, TopicPartitionState.BeginningOffset, TopicPartitionState.EndOffset, ConsumerGroupPartitionLag.Topic, ConsumerGroupPartitionLag.Partition, ConsumerGroupPartitionLag.CommittedOffset, ConsumerGroupPartitionLag.StartOffset, ConsumerGroupPartitionLag.EndOffset, ConsumerGroupPartitionLag.Lag, ConsumerGroupState.Group, ConsumerGroupState.CoordinatorID, ConsumerGroupState.State, ConsumerGroupState.ProtocolType, ConsumerGroupState.Protocol, ConsumerGroupState.Members, ConsumerGroupState.Partitions, TopicInspectionResult.Topic, TopicInspectionResult.State, TopicInspectionResult.Category, TopicInspectionResult.Err, ConsumerGroupInspectionResult.Group, ConsumerGroupInspectionResult.State, ConsumerGroupInspectionResult.Category, ConsumerGroupInspectionResult.Err, ConsumerGroupMemberState.MemberID, ConsumerGroupMemberState.InstanceID, ConsumerGroupMemberState.InstanceIDVisible, ConsumerGroupMemberState.ClientID, ConsumerGroupMemberState.ClientHost, ConsumerGroupMemberState.Assignments |
| inspector_consumer_protocol.go | kafka | source API/private invariants; missing: ConsumerProtocolGroupMemberState.MemberID, ConsumerProtocolGroupMemberState.InstanceIDVisible, ConsumerProtocolGroupMemberState.RackIDVisible, ConsumerProtocolGroupMemberState.MemberEpoch, ConsumerProtocolGroupMemberState.MemberType, ConsumerProtocolGroupMemberState.ClientID, ConsumerProtocolGroupMemberState.ClientHost, ConsumerProtocolGroupMemberState.SubscribedTopicRegexVisible, ConsumerProtocolGroupMemberState.TargetAssignments, ConsumerProtocolGroupState.Group, ConsumerProtocolGroupState.CoordinatorID, ConsumerProtocolGroupState.Epoch, ConsumerProtocolGroupState.Assignor, ConsumerProtocolGroupInspectionResult.Group, ConsumerProtocolGroupInspectionResult.Err |
| inspector_consumer_protocol_test.go | kafka | retained 14 tests |
| inspector_group_health_test.go | kafka | retained 16 tests |
| inspector_metadata_test.go | kafka | retained 20 tests |
| inspector_observer_test.go | kafka | retained 7 tests |
| inspector_partial_test.go | kafka | retained 6 tests |
| inspector_test.go | kafka | retained 6 tests |
| integration_apache_test.go | kafka_test | build variant interoperability; retained 14 tests |
| integration_broker_concurrency_race_test.go | kafka_test | build variant interoperability && race; retained 0 tests |
| integration_broker_concurrency_test.go | kafka_test | build variant interoperability && !race; retained 0 tests |
| integration_broker_gate_test.go | kafka_test | build variant interoperability; retained 3 tests |
| integration_cleanup_test.go | kafka_test | build variant interoperability; retained 12 tests |
| integration_conformance_test.go | kafka_test | build variant interoperability; retained 1 tests |
| integration_main_test.go | kafka_test | build variant interoperability; retained 1 tests; TestMain lifecycle must be inspected before selecting execution |
| integration_oauth_jwks_rotation_test.go | kafka_test | build variant interoperability; retained 1 tests |
| integration_oauth_rotation_test.go | kafka_test | build variant interoperability; retained 1 tests |
| integration_oauth_token_endpoint_test.go | kafka_test | build variant interoperability; retained 1 tests |
| integration_plain_rolling_test.go | kafka_test | build variant interoperability; retained 1 tests |
| integration_rebalance_safety_test.go | kafka_test | build variant interoperability; retained 1 tests |
| integration_security_test.go | kafka_test | build variant interoperability; retained 8 tests |
| integration_session_timeout_unix_test.go | kafka_test | build variant interoperability && (darwin || linux); retained 2 tests |
| integration_test.go | kafka_test | build variant interoperability; retained 2 tests |
| integration_tiered_storage_test.go | kafka_test | build variant interoperability; retained 1 tests |
| kafkaservice/a_lifecycle_stop_test.go | kafkaservice_test | retained 3 tests |
| kafkaservice/api_compat_test.go | kafkaservice_test | retained 0 tests |
| kafkaservice/benchmark_test.go | kafkaservice_test | retained 0 tests; BenchmarkProducerPublish; BenchmarkLifecycleShutdown |
| kafkaservice/credential_expiry_interoperability_test.go | kafkaservice_test | build variant interoperability; retained 2 tests |
| kafkaservice/example_test.go | kafkaservice_test | retained 0 tests; ExampleNewProducer |
| kafkaservice/facade_internal_test.go | kafkaservice | retained 3 tests |
| kafkaservice/fuzz_test.go | kafkaservice_test | retained 0 tests; FuzzHandlerCorrelationBoundary |
| kafkaservice/interoperability_test.go | kafkaservice_test | build variant interoperability; retained 3 tests |
| kafkaservice/kafkaservice.go | kafkaservice | source API/private invariants; missing: OptionsError.Field, OptionsError.Reason, CallbackPanicError.Operation, CallbackError.Operation, CallbackError.Err, StartupError.Validation, StartupError.Cleanup, HandlerOptions.Correlation, HandlerOptions.CorrelationCodec, HandlerOptions.TrustedMetadata, HandlerOptions.RejectInvalidMetadata, HandlerOptions.TracePropagator, HandlerOptions.Handler; retained deprecation marker |
| kafkaservice/kafkaservice_test.go | kafkaservice_test | retained 22 tests |
| kafkaservice/lifecycle_hardening_test.go | kafkaservice_test | retained 8 tests |
| kafkatest/authentication.go | kafkatest | source API/private invariants; missing: AuthenticationProviderHarness.UsernamePassword, AuthenticationProviderHarness.OAuthBearer, AuthenticationProviderHarness.ClientCertificate, AuthenticationProviderHarness.TrustAnchors |
| kafkatest/conformance_test.go | kafkatest_test | retained 2 tests |
| kafkatest/consumer.go | kafkatest | source API/private invariants |
| kafkatest/harness.go | kafkatest | source API/private invariants; missing: ReadRequest.Topic, ReadRequest.Partition, ReadRequest.StartOffset, ReadRequest.MaxRecords, ReadRequest.Isolation, BrokerHarness.Brokers, BrokerHarness.Security |
| kafkatest/harness_test.go | kafkatest | retained 3 tests |
| kafkatest/inspector.go | kafkatest | source API/private invariants |
| kafkatest/observer.go | kafkatest | source API/private invariants |
| kafkatest/producer.go | kafkatest | source API/private invariants |
| kafkatest/replay.go | kafkatest | source API/private invariants |
| kafkatest/transaction.go | kafkatest | source API/private invariants |
| lifecycle_observer_test.go | kafka | retained 3 tests |
| lz4_interoperability_test.go | kafka | retained 4 tests |
| observer.go | kafka | source API/private invariants |
| observer_broker_test.go | kafka | retained 8 tests |
| observer_external_test.go | kafka_test | retained 1 tests |
| observer_franz.go | kafka | source API/private invariants |
| observer_test.go | kafka | retained 22 tests |
| producer.go | kafka | source API/private invariants; missing: ProducerConfig.Brokers, ProducerConfig.ClientID, ProducerConfig.Protocol, ProducerConfig.AllowedTopics, ProducerConfig.KeyPolicy, ProducerConfig.Limits, ProducerConfig.MaxBufferedRecords, ProducerConfig.MaxBufferedBytes, ProducerConfig.MaxBatchRecords, ProducerConfig.MaxBatchBytes, ProducerConfig.RecordRetries, ProducerConfig.RetryBackoffMin, ProducerConfig.RetryBackoffMax, ProducerConfig.DeliveryTimeout, ProducerConfig.ShutdownTimeout, ProducerConfig.RequestTimeout, ProducerConfig.DialTimeout, ProducerConfig.Linger, ProducerConfig.CompressionPreferences, ProducerConfig.TransactionalID, ProducerConfig.TransactionTimeout, ProducerConfig.TransactionEndTimeout, ProducerConfig.Security, ProducerConfig.Observers, MessageLimits.MaxTopicBytes, MessageLimits.MaxKeyBytes, MessageLimits.MaxValueBytes, MessageLimits.MaxHeaders, MessageLimits.MaxHeaderKeyBytes, MessageLimits.MaxHeaderValueBytes, MessageLimits.MaxHeaderBytes, Header.Key, Header.Value, DeliveryResult.Topic, DeliveryResult.Partition, DeliveryResult.Offset, DeliveryResult.Timestamp, DeliveryResult.Err |
| producer_config_external_test.go | kafka_test | retained 1 tests |
| producer_test.go | kafka | retained 56 tests |
| protocol.go | kafka | source API/private invariants; missing: ProtocolPolicy.MinimumVersion |
| protocol_test.go | kafka | retained 4 tests |
| rebalance_shutdown_test.go | kafka | retained 5 tests |
| record.go | kafka | source API/private invariants; missing: TopicPartition.Topic, TopicPartition.Partition, PartitionSelection.Mode, PartitionSelection.Partition, ProducerRecord.Topic, ProducerRecord.Partition, ProducerRecord.Key, ProducerRecord.Value, ProducerRecord.Headers, ProducerRecord.Timestamp, ConsumedRecord.Topic, ConsumedRecord.Key, ConsumedRecord.Value, ConsumedRecord.Headers, ConsumedRecord.Timestamp, ConsumedRecord.TimestampType, ConsumedRecord.Partition, ConsumedRecord.Offset, ConsumedRecord.LeaderEpoch |
| record_test.go | kafka | retained 22 tests |
| replay.go | kafka | source API/private invariants; missing: ReplayRange.Topic, ReplayRange.Partition, ReplayRange.StartOffset, ReplayRange.EndOffset, ReplayMetadata.Range, ReplayMetadata.EffectiveStartOffset, ReplayRecord.Metadata, ReplayHandler.HandleReplay, ReplayPosition.Topic, ReplayPosition.Partition, ReplayPosition.NextOffset, ReplayCheckpoint.Positions, ReplayConfig.Brokers, ReplayConfig.ClientID, ReplayConfig.Protocol, ReplayConfig.Ranges, ReplayConfig.Checkpoint, ReplayConfig.SideEffects, ReplayConfig.Limits, ReplayConfig.Security, ReplayConfig.MaxPollRecords, ReplayConfig.FetchMaxBytes, ReplayConfig.FetchMaxPartitionBytes, ReplayConfig.FetchMaxWait, ReplayConfig.PlanningTimeout, ReplayConfig.ProgressTimeout, ReplayConfig.HandlerTimeout, ReplayConfig.ShutdownTimeout, ReplayConfig.DialTimeout, ReplayPlannedRange.NextOffset, ReplayPlannedRange.Remaining, ReplayPlan.Ranges, ReplayPlan.TotalRemaining, ReplayRangeResult.NextOffset, ReplayRangeResult.Processed, ReplayRangeResult.Skipped, ReplayRangeResult.Failed, ReplayRangeResult.Complete, ReplayResult.Polled, ReplayResult.Processed, ReplayResult.Skipped, ReplayResult.Failed, ReplayResult.CompletedRanges, ReplayResult.IncompleteRanges, ReplayResult.Ranges |
| replay_example_test.go | kafka_test | retained 0 tests; ExampleReplayResult_Checkpoint |
| replay_observer_test.go | kafka | retained 4 tests |
| replay_parallel.go | kafka | source API/private invariants |
| replay_parallel_test.go | kafka | retained 9 tests |
| replay_safety_test.go | kafka | retained 16 tests |
| replay_test.go | kafka | retained 15 tests |
| replay_timestamp.go | kafka | source API/private invariants; missing: ReplayTimestampRequest.StartInclusive, ReplayTimestampRequest.EndExclusive, ReplayTimestampRequest.Partitions, ReplayTimestampPartition.Topic, ReplayTimestampPartition.Partition, ReplayTimestampPartition.StartOffset, ReplayTimestampPartition.EndOffset, ReplayTimestampPartition.Remaining, ReplayTimestampPlan.StartInclusive, ReplayTimestampPlan.EndExclusive, ReplayTimestampPlan.Partitions, ReplayTimestampPlan.TotalRemaining |
| replay_timestamp_test.go | kafka | retained 12 tests |
| transaction_error_test.go | kafka | retained 7 tests |
| transaction_observer_test.go | kafka | retained 7 tests |
| transaction_processor.go | kafka | source API/private invariants; missing: TransactionConnectionConfig.Brokers, TransactionConnectionConfig.ClientID, TransactionConnectionConfig.Protocol, TransactionConnectionConfig.DialTimeout, TransactionConnectionConfig.Security, TransactionGroupConfig.GroupID, TransactionGroupConfig.InstanceID, TransactionGroupConfig.Rack, TransactionGroupConfig.Topics, TransactionGroupConfig.ResetOffset, TransactionGroupConfig.BalancePolicy, TransactionGroupConfig.MaxPollRecords, TransactionGroupConfig.MaxConcurrentFetches, TransactionGroupConfig.FetchMaxBytes, TransactionGroupConfig.FetchMaxPartitionBytes, TransactionGroupConfig.FetchMaxWait, TransactionGroupConfig.SessionTimeout, TransactionGroupConfig.RebalanceTimeout, TransactionGroupConfig.HeartbeatInterval, TransactionGroupConfig.ProcessingTimeout, TransactionOutputConfig.AllowedTopics, TransactionOutputConfig.KeyPolicy, TransactionOutputConfig.MaxBufferedRecords, TransactionOutputConfig.MaxBufferedBytes, TransactionOutputConfig.MaxBatchBytes, TransactionOutputConfig.MaxOutputRecords, TransactionOutputConfig.MaxOutputBytes, TransactionOutputConfig.RecordRetries, TransactionOutputConfig.RetryBackoffMin, TransactionOutputConfig.RetryBackoffMax, TransactionOutputConfig.DeliveryTimeout, TransactionOutputConfig.RequestTimeout, TransactionOutputConfig.Linger, TransactionOutputConfig.CompressionPreferences, TransactionOutputConfig.TransactionalID, TransactionOutputConfig.TransactionTimeout, TransactionOutputConfig.TransactionEndTimeout, TransactionProcessorConfig.Connection, TransactionProcessorConfig.Group, TransactionProcessorConfig.Output, TransactionProcessorConfig.Limits, TransactionProcessorConfig.Observers, TransactionProcessorConfig.ShutdownTimeout, TransactionHandler.Handle, TransactionPollResult.Polled, TransactionPollResult.Processed, TransactionPollResult.Published, TransactionPollResult.Committed |
| transaction_processor_test.go | kafka | retained 30 tests |
| workflow_example_test.go | kafka_test | retained 0 tests; ExampleProducer_PublishRecord; ExampleProducer_PublishBatch; ExampleProducer_PublishAsync; ExampleConsumer_Run; ExampleProducer_RunTransaction; ExampleNewTransactionProcessor; ExampleReplayReader_Replay |

## Investigation and gap disposition

The inventory's missing-member column is the prewrite observation, not a list of
remaining defects. All listed fields/methods plus generic legacy
ProducerOptions/ConsumerOptions fields received source-derived contracts.
Existing useful private, public, assurance and compatibility comments were kept.
Every tracked file was classified, and causal slices were traced through the
following owner boundaries; unmodified assertion bodies and frozen variants are
not presented as newly executed runtime proof.

| Owner / source slice | Investigated contract and retained evidence |
| --- | --- |
| producer.go, record.go | Pre-admission validation, deep-copy ownership, explicit partitions, input-ordered partial batch receipts, async admission versus completion, maintenance gates, callback-scoped transaction capabilities, uncertain commit receipts. producer_test.go, transaction_error_test.go and lifecycle_observer_test.go retain the cases. |
| consumer.go, consumer_assignment.go, consumer_parallel.go, consumer_rebalance.go, consumer_batch.go | Local epochs differ from broker generations; per-partition safe prefixes and whole-batch settlement differ; effects can precede failed commits; partial multi-partition settlement is not disproved by Committed zero. Drain/Shutdown join trusted callbacks cooperatively. consumer_test.go, consumer_parallel_test.go and rebalance observer cases retain the assertions. |
| consumer_failure.go, consumer_batch_failure.go | Bounded local retries, copied failure inputs, explicit versioned target topics and independent publication/source-commit effects. Package observations omit raw causes; formatting a public failure value or Cause is not a privacy boundary. Existing formatting claims were corrected without adding formatter behavior. |
| client.go, fetch_safety.go, protocol.go | Selective TLS copies with borrowed private signers, explicit plaintext, cooperative providers, per-connection root rotation, bounded bearer syntax not cryptographic verification; retained decoded capacity is charged after allocation, not a process-heap cap. KAFKA-DEC-001 and credential/fetch tests remain frozen. |
| replay.go, replay_parallel.go, replay_timestamp.go | Single-use opt-in reader, copied numeric ranges/checkpoints, exclusive end offsets, numeric gaps fail closed, timestamp queries are separate observations, local progress budgets and external checkpoint/effect ownership. KAFKA-DEC-006 and replay tests retain intended behavior; no gap restoration claim. |
| inspector.go, inspector_consumer_protocol.go | Bounded copied broker metadata, input-ordered partial failures, numeric lag versus retained work, visibility flags, distinct KIP-848 group/member/assignment epochs, local hysteresis and non-probing liveness. Close fences later admission but does not promise a package-managed join of all inspection calls. KAFKA-DEC-007 and metadata fuzz targets retain evidence. |
| observer.go, observer_franz.go | Inline ordered callbacks share one detached cooperative context; no forced timeout or rollback of already emitted events. Hook inputs exclude connections/endpoints/raw causes and normalize bounded metadata. KAFKA-DEC-009 and observer/reentry tests remain authoritative. |
| adapters/mskiam | Borrowed AWS providers, no adapter goroutines, refresh cohorts, credential-expiry caps, trusted signer and bounded token structure rather than independent SigV4 verification. KAFKA-MSKIAM-DEC-001 through 003 preserve pinned signer 1.0.4/SDK 1.43.0 and separate direct-service qualification. |
| adapters/otel, adapters/gotelemetry | Borrowed SDK lifecycle, copied allowlists, retrospective operation spans, SDK-owned sampling/export and W3C-only header copies. Convention 1.44.0 is separate from SDK 1.45.0. Cause exposure is not redacted by wrapper text. Legacy instrumentation name/type identities remain. |
| adapters/slog, adapters/golog | Explicit logger and exact identity allowlists, synchronous handler cooperation, payload-free package fields but no universal handler redaction. Legacy facade errors and deprecation remain unchanged. |
| adapters/service, kafkaservice | Inert resource wrappers, optional producer versus required consumer teardown transfer, start/stop admission/join/retry, callbacks outside locks, copied outbound records, borrowed inbound handlers, correlation trust not authentication. Canonical/legacy public identities remain distinct. |
| kafkatest, benchmarks/clients | Harness-owned fixture resources and selected assertions, not universal certificates. Tagged comparison workloads retain acknowledgement/isolation/setup policies and cleanup; they were inspected, not run. |

All 24 fuzz entrypoints and 43 benchmarks were inspected for input/timing and
assertion boundaries; missing useful intent comments were added without changing
bodies, seeds, retained crashes or corpus. The 146 assurance files include named
regressions, API checks, fixture helpers, examples and tagged service tests. No
new performance/conformance claim follows from source inspection.

### Frozen authorities and markers

Root decisions KAFKA-DEC-001 through 009 and three MSK IAM decisions, history
prefixes, official/peer manifests and normative expectations remain byte-frozen.
Franz-go 1.21.5, Kafka 3.7.2/4.3.1 fixture interpretation, signer/SDK pins and
historical Testcontainers references were not modernized as prose work. All seven
module dependency files, workspace files, catalogs and scripts are preserved.
The six binary API baselines, SQL, fixtures and immutable supply-chain inputs
also remain unchanged.

No generated source, go:generate or TODO/FIXME/HACK/NOTE/WARNING source marker was
found. Existing build, race/non-race, Unix and msk variants plus all Deprecated
markers are retained. No generator was executed solely for a structural check.

### Bounded native verification and remaining questions

Completed locally with Go 1.27.2: all 185 executable ASTs, raw noncomment tokens
and literals matched the base exactly (no formatter token deltas); every directive
and marker was retained. Formatting covers all 53 owned Go paths. The stdlib
parser rendered 1408 comment groups to HTML and checked 1266 named public
members plus ten canonical package comments. Native `go doc -all` rendered all
ten catalog packages, including the non-releasable benchmark package.

The actual native docs route was run once in root, gotelemetry, mskiam, otel and
service: `make -f verification/package.mk package-contract`. Legacy kafkaservice
has no configured docs target; `go test . -run '^Example' -count=1` compiled and
executed its existing output-bearing example separately. No Example body changed
and no native Example operation was duplicated. Benchmark clients was render-only.
The initial checks exposed an undocumented public protocol sentinel and refined
MSK error-sentinel formatting/matching methods; useful contracts were added.
Final all-source proof and the affected MSK render were refreshed afterward;
unchanged nine renders, native docs/example operations and typed decisions retain
their original passing identity rather than being described as newly executed.

The native typed SHA-256 procedure confirmed all twelve decision identities
unchanged. The current source-documentation checker passed all seven modules and
185 files with no findings. Local documentation and offline specification checks
passed through the repository-matching authenticated Tools 2.3.0 binary; the
specification checker validated two modules and twelve decisions. API baselines
remain binary apidiff inputs, not prose snapshots, and were not regenerated.

All-source preservation, rendered/native documentation, frozen specification
identities, relative links, default secret scans and ordinary complete-diff
truth/usefulness inspection support this source-only handoff. Explicit staging
and its final secret scan remain normal commit hygiene, not runtime proof. No full-suite, service or release status is implied. All three task-owned
Go caches, helpers, package renders and disposable gate outputs were removed on
both failed diagnostic paths and successful final verification. Parent-owned
verification binaries are retained; the original checkout is preserved.

Output-less examples compile but do not execute: all seven broker-dependent
root workflow examples, root logging facade examples, ordinary telemetry setup
and MSK IAM setup. Output-bearing root/propagation/service examples use owned
fixtures. MSK IAM's native docs route additionally selects three untagged local
signer/provider tests; two msk-named cases are tag-excluded, not passing evidence.
No broker, Docker, live AWS, race, active fuzz, benchmark, consumer or release
campaign is applicable to preserved behavior.

Questions retained at the application/qualification boundary: callback/SDK
cooperation and concurrency, network/ACL/topic policy, external-effect
idempotency, unknown Kafka receipts, runtime retention/offset holes, broker
version or managed-service support and exporter privacy require their actual
owners' evidence. This source pass does not silently repair any runtime defect,
rewrite selected expectations or establish those qualifications.

## Objective Enforcement

The checksum-pinned public Tools v2.4.0 verifier now supplies
`make source-docs-check` as a prerequisite of the existing local and CI Make
routes. Ordinary shared CI selects strict source-documentation checking and
requires its result. Native prerequisites, operation budgets, security and
release controls are retained. Hosted exact-head evidence is recorded by the
coordination ledger; configuration alone does not establish green delivery.
