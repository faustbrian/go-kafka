# Threat model v1: Kafka client package family

Reviewed 2026-09-30 against repository main `e3e49533d5e4141414ce35cf1df71a6fd2d12c62`.
This model covers the root client, its optional `mskiam`, `otel`, and `service`
adapters, and the compatibility facades. It is a source and local-fixture risk
disposition, not an assessment of any deployed Kafka cluster or IAM policy.

## Assets and trust boundaries

Assets are record keys and values, credentials and signed tokens, producer
acknowledgements, consumer offsets, transaction outcomes, replay checkpoints,
and operational metadata. Callers control configuration, secret providers,
handlers, and durable side effects. The package validates and copies bounded
inputs before passing them to franz-go. Kafka brokers, advertised addresses,
record contents, metadata responses, and SASL challenges are external inputs;
TLS authenticates the broker only under the caller's selected trust anchors.
Optional AWS credential providers and telemetry exporters cross separate
caller-owned boundaries. Broker authorization, storage durability, and network
policy are outside this repository.

## Threats, controls, and residual decisions

| Boundary / threat | Repository control and evidence | Residual owner and decision |
| --- | --- | --- |
| Broker impersonation, plaintext credential exposure, or hostile TLS options | Verified TLS 1.2+ is the default; hostname and chain checks cannot be disabled; plaintext needs an explicit development-only policy that rejects authentication. Static and rotating certificate/trust material is bounded and validated. `client_test.go` and `integration_security_test.go` cover these paths. | Operator must use an approved CA, protect private keys, and prove certificate rotation against its brokers. Development plaintext is not an approved production configuration. |
| Credential or token disclosure through providers, errors, or telemetry | Providers have bounded call contexts and validated outputs; public errors and security formatting redact material. MSK IAM signs through the AWS provider boundary and caps token validity at credential expiry. `client_test.go`, `adapters/mskiam/hardening_test.go`, and security fixture tests exercise redaction and rotation. | Caller providers must honor context, keep secrets out of identifiers/logs, use verified HTTPS for OAuth acquisition, and protect their own unwrap/debug output. A non-cooperative provider cannot be forcibly stopped by Go. Managed MSK compatibility needs direct service evidence. |
| Unauthorized publication, unbounded record admission, or ambiguous broker result | Producers copy a required topic allowlist and record bytes, cap sizes/buffers/retries/deadlines, request all-ISR acknowledgements, and classify lost delivery or transaction evidence as ambiguous. `producer_test.go` and broker fixtures cover denial, ordering, and ambiguity. | Broker ACLs, quotas, topic durability, and application deduplication remain deployment-owned. Never blindly resubmit an ambiguous result. |
| Malicious fetch size, handler failure, stale ownership, or offset loss | Consumer fetch/decompression, poll admission, and commit calls are bounded; auto-commit is off; settlement follows successful partition prefixes and assignment fences. Handler deadlines are cooperative because callbacks run synchronously. `consumer_test.go`, `fetch_safety_test.go`, and rebalance fixtures cover the package-controlled edges. | Application handlers must honor context, bound their own I/O, and be idempotent and concurrency-safe when configured. A blocked callback can keep a runner and drain open; broker may partially persist a failed multi-partition commit. A side effect is not atomic with its offset. Review this boundary when handler implementations change. |
| Replay misuse, retained-data loss, or duplicate side effects | Replay requires explicit ranges, opt-in side effects, checked broker bounds, no offset reset or group commit, progress timeouts between handler calls, and resumable per-partition checkpoints. `replay_safety_test.go` covers fail-closed gaps and cooperative cancellation. | Replay operator needs separate read ACLs, audited intent, current retention checks, and idempotent, context-cooperative handlers with bounded I/O. A blocked callback can stop checkpoint progress despite `HandlerTimeout`; checkpoints do not prove exactly-once processing. Review this boundary for every new replay handler. |
| Shutdown race or leaked in-flight work | Drain and shutdown fence new admission, retain incomplete producer work for retry, and report ambiguous outcomes. Consumer and replay cancellation prevent new admission but cannot preempt a synchronous callback already running. Lifecycle tests cover cooperative handlers and retries. | Application must bound handler work, budget shutdown time, observe incomplete results, and reconcile unresolved transactions before replacement. A non-cooperative callback can block Run/replay/drain past the configured deadline; review handler cancellation behavior during adoption. |
| Dependency or workflow compromise | Go module checksums, pinned shared CI workflow, online specification monitoring, static analysis, and repository security gates provide attributable checks. | Maintainers review changed authority feeds and new upstream releases before updating locks; passing CI is not evidence that a deployed broker or IAM policy is secure. |

No critical or high source finding was identified in this bounded review. The
conditional deployment responsibilities above are not accepted as proven-safe
production configurations. `docs/security.md`, `docs/guarantees.md`, and
`docs/operations.md` give the detailed contract and fixture scope; a material
source, dependency, broker, or credential-boundary change requires this model
to be reassessed.
