// Package clients contains non-releasable Kafka client comparison tooling.
//
// The integration-tagged package compares policy, franz-go, kafka-go and Sarama
// against explicitly owned broker fixtures. Its TestMain cleans started Kafka
// and TLS resources; selecting that tag can create containers even when the
// service catalog is empty. The ordinary package contains no client runtime.
//
// Measurements depend on equivalent acknowledgements, isolation, corpus,
// partitions, setup and environment. verification/package.mk and retained
// performance artifacts define those comparison boundaries; this package alone
// does not establish production throughput, universal conformance or supported
// broker/client versions.
package clients
