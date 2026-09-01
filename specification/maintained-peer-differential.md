# Maintained-peer differential evidence

The non-production `benchmarks/clients` module pins franz-go v1.21.5,
kafka-go v0.4.51, and Sarama v1.60.1 and exercises equivalent producer,
consumer, transaction, replay, inspection, TLS, rebalance, and resource
workloads against the same Kafka fixtures. Its source and retained results are
the practical differential evidence for `KAFKA-DEC-001` through
`KAFKA-DEC-009`.

Agreement is used only where observable contracts are equivalent. Exclusions
and disagreements are deliberate policy differences: client defaults,
partitioners, commit APIs, lifecycle cost, and error reporting are not votes
that weaken Kafka or package requirements. See
[`benchmarks/clients/README.md`](../benchmarks/clients/README.md) and
[`docs/performance.md`](../docs/performance.md) for the exact peer versions,
workload equivalence, exclusions, and retained result directories.
