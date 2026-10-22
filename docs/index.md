# kafka documentation map

[Repository README](../README.md) · [Current Golib portal](https://github.com/faustbrian/go-library-tools/blob/main/docs/ecosystem/README.md) · [Published ecosystem snapshot](https://github.com/faustbrian/go-library-tools/blob/v1.8.4/docs/ecosystem/README.md)

This map links package-owned adoption and operational guidance. Start with
the repository README for purpose, installation, and ownership boundaries.
Published compatibility sets are frozen multi-package baselines; they do not
certify every current main revision or a deployed application.

## Module boundaries

Import only the modules needed by your application. A deprecated facade,
preview adapter, or internal harness is not interchangeable with a stable
root API; follow its own README and migration guidance before composing it.

| Module | Status | Owner detail |
| --- | --- | --- |
| `github.com/faustbrian/go-kafka/v2` | stable | [README](../README.md) |
| `github.com/faustbrian/go-kafka/adapters/mskiam` | stable; requires root v1 until its v2 major | [README](../adapters/mskiam/README.md) |
| `github.com/faustbrian/go-kafka/benchmarks/clients` | internal; non-releasable | [README](../benchmarks/clients/README.md) |
| `github.com/faustbrian/go-kafka/adapters/otel` | stable; requires root v1 until its v2 major | [README](../adapters/otel/README.md) |
| `github.com/faustbrian/go-kafka/adapters/service` | stable; requires root v1 until its v2 major | [README](../adapters/service/README.md) |

## Choose and adopt

- [Documentation](README.md).
- [API](api.md).
- [Architecture](architecture.md).
- [Amazon MSK and ECS deployment guidance](aws-msk-ecs.md).
- [Compatibility and execution matrix](compatibility.md).
- [Configuration reference](configuration.md).
- [Consumer groups and rebalances](consumer.md).
- [Production policy decision matrices](design/decision-matrices.md).
- [Guarantees and failure model](guarantees.md).
- [Inspection and dependency health](inspection.md).
- [Migration, deprecation, and upgrades](migration.md).
- [Observability hooks](observability.md).
- [Detailed reference](reference.md).
- [Replay](replay.md).
- [Consumer retry and dead-letter policy](retry-dead-letter.md).
- [Service integration](service-integration.md).
- [Topic, keying, partitioning, and durability design](topic-design.md).
- [Transactions](transactions.md).

## Operate and evaluate boundaries

- [Operations](operations.md).
- [Operational runbooks](runbooks.md).
- [Security](security.md).
- [Threat model v1: Kafka client package family](threat-model-v1.md).
- [Troubleshooting, FAQ, and glossary](troubleshooting.md).

## Owned adapters, examples, and harnesses

- [Kafka Amazon MSK IAM adapter](../adapters/mskiam/README.md).
- [Documentation](../adapters/mskiam/docs/README.md).
- [Kafka OpenTelemetry adapter](../adapters/otel/README.md).
- [Documentation](../adapters/otel/docs/README.md).
- [Kafka service integration](../adapters/service/README.md).
- [Documentation](../adapters/service/docs/README.md).
- [Kafka slog adapter](../adapters/slog/README.md).

## Maintainer, specification, and evidence references

- [Specification conformance matrix](../adapters/mskiam/specification/README.md).
- [Kafka client comparison benchmarks](../benchmarks/clients/README.md).
- [Public conformance suites](conformance.md).
- [Equivalent Kafka client captures, 2026-07-30](performance-results/2026-07-30/README.md).
- [Equivalent Kafka replay, inspection, rebalance, TLS, and resource captures, 2026-07-31](performance-results/2026-07-31/README.md).
- [Equivalent Kafka mTLS and SASL/PLAIN capture, 2026-08-03](performance-results/2026-08-03/README.md).
- [Equivalent Kafka mTLS, PLAIN, SCRAM, and OAuth capture, 2026-08-09](performance-results/2026-08-09/README.md).
- [Kafka policy-overhead capture, 2026-08-10](performance-results/2026-08-10/README.md).
- [Performance evidence](performance.md).
- [Source documentation audit](source-documentation-audit.md).
- [Specification decisions](specification-decisions.md).
- [Specification conformance matrix](../specification/README.md).

## Related next steps

[Contributor setup and checks](../CONTRIBUTING.md), [Compatibility policy](../COMPATIBILITY.md), [Release history](../CHANGELOG.md), [Private vulnerability reporting](../SECURITY.md), [Support](../SUPPORT.md).
