# Kafka service integration

`kafkaservice` is the independently versioned integration between
[`github.com/faustbrian/go-kafka`](..) and
`github.com/faustbrian/go-service`. The root Kafka production package
does not import service, correlation, or OpenTelemetry APIs.

The adapter keeps concrete Kafka resources visible. It owns only service
lifecycle composition, correlation record boundaries, and optional
caller-owned trace propagation. Topic, partition, delivery, retry, settlement,
transaction, replay, and dead-letter policy remain in the root Kafka package
or the application.

## Install

```sh
go get github.com/faustbrian/go-kafka/kafkaservice@v1
```

## Quick start

```go
adapter, err := kafkaservice.NewProducer(
	kafkaservice.ProducerOptions[*kafka.Producer]{
		Name:        "orders-producer",
		Resource:    producer,
		Correlation: correlationFactory,
		Publish: func(
			ctx context.Context,
			producer *kafka.Producer,
			record kafka.ProducerRecord,
		) (kafka.DeliveryResult, error) {
			result := producer.PublishRecord(ctx, record)
			return result, result.Err
		},
		Shutdown: func(
			ctx context.Context,
			producer *kafka.Producer,
		) error {
			return producer.Shutdown(ctx)
		},
	},
)
if err != nil {
	return err
}

component := adapter.Component()
```

The compiling examples in this module contain complete imports and setup.

## Guarantees and limitations

The [complete guide](docs/reference.md) defines ownership, failure semantics,
bounds, concurrency, security, and unsupported behavior. Do not infer
additional guarantees beyond the documented module boundary.

## Documentation

- [Documentation index](docs/README.md)
- [Complete technical guide](docs/reference.md)
- [Go API reference](https://pkg.go.dev/github.com/faustbrian/go-kafka/kafkaservice)
- [Parent package documentation](../docs/README.md)

## Compatibility and support

This module follows Semantic Versioning. Report vulnerabilities through the
[parent security policy](../SECURITY.md).

## License

MIT. See [LICENSE](LICENSE).
