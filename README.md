# kafka

[![CI](https://github.com/faustbrian/go-kafka/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/faustbrian/go-kafka/actions/workflows/ci.yml)
[![CodeQL](https://img.shields.io/badge/CodeQL-required-blue)](https://github.com/faustbrian/go-kafka/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/badge/coverage-100%25_required-blue)](CONTRIBUTING.md#verification)
[![Mutation](https://img.shields.io/badge/mutation-100%25_required-blue)](CONTRIBUTING.md#verification)
[![Documentation](https://img.shields.io/badge/docs-checked_in_CI-blue)](docs/)
[![Go Reference](https://pkg.go.dev/badge/github.com/faustbrian/go-kafka.svg)](https://pkg.go.dev/github.com/faustbrian/go-kafka)
[![Release](https://img.shields.io/github/v/release/faustbrian/go-kafka?sort=semver)](https://github.com/faustbrian/go-kafka/releases)
[![Go](https://img.shields.io/badge/go-1.26.6-00ADD8?logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

`kafka` provides bounded, explicit Apache Kafka producer, consumer, replay,
inspection, and transactional building blocks for Go services. It wraps
franz-go without hiding delivery outcomes, lifecycle ownership, topic policy,
or security configuration.

The package provides at-least-once building blocks. It does not make database
writes and Kafka publication atomic, make consumer side effects exactly once,
or own topic and broker configuration.

## Installation

```sh
go get github.com/faustbrian/go-kafka
```

## Quick start

Configuration can be validated during bootstrap without allocating a client or
dialing brokers:

```go
config := kafka.ProducerConfig{
	Brokers:       []string{"kafka.internal:9093"},
	ClientID:      "track-outbox",
	AllowedTopics: []string{"track.tracking-event.v1"},
}
if err := config.Validate(); err != nil {
	return err
}

producer, err := kafka.NewProducer(config)
if err != nil {
	return err
}
defer producer.Close()

return producer.Publish(ctx, kafka.Message{
	Topic: "track.tracking-event.v1",
	Key:   []byte(trackedItemID),
	Value: payload,
})
```

## Guarantees and limits

- Producers retain franz-go idempotence, require all in-sync replica
  acknowledgements, and bound retries, buffering, admission, and delivery.
- Consumers expose explicit acknowledgement, retry, dead-letter, rebalance,
  and shutdown behavior.
- Topic access is restricted through constructor-copied allowlists.
- TLS 1.2 or later is the default; authentication and broker compatibility are
  explicit deployment decisions.
- Ambiguous publish and commit outcomes remain distinguishable and require
  application reconciliation.
- Service, OpenTelemetry, and Amazon MSK IAM integrations remain optional
  modules with caller-owned lifecycle.

## Documentation

Start with the [documentation index](docs/README.md), [compatibility matrix](docs/compatibility.md),
[delivery guarantees](docs/guarantees.md), and [operations guide](docs/operations.md).
The [detailed reference](docs/reference.md) preserves the complete producer,
consumer, replay, inspection, and lifecycle contracts.

## Development

Run `make check` for the repository contract. Backend and managed-service
changes must also pass the applicable integration and interoperability gates.

## License

MIT. See [LICENSE](LICENSE).
