# Kafka OpenTelemetry adapter

`gotelemetry` is the independently versioned OpenTelemetry adapter for
[`github.com/faustbrian/go-kafka`](../..). The root Kafka module remains
vendor-neutral and does not import OpenTelemetry.

Use this adapter when Kafka policy observations should become traces and
metrics. `Instrumentation` translates only completed, copied, payload-free
`kafka.Observation` values. The separate `TraceContextPropagation` policy can
copy bounded W3C Trace Context fields between explicit Kafka records and
contexts. Neither surface wraps `franz-go`, reimplements Kafka instrumentation,
or installs global OpenTelemetry state.

## Install

```sh
go get github.com/faustbrian/go-kafka/adapters/gotelemetry@v1
```

## Quick start

```go
instrumentation, err := gotelemetry.New(gotelemetry.Config{
    Runtime: telemetryRuntime,
    Attributes: gotelemetry.AttributePolicy{
        AllowedClientIDs:      []string{"orders-producer", "orders-consumer"},
        AllowedTopics:         []string{"orders"},
        AllowedConsumerGroups: []string{"fulfillment"},
    },
})
if err != nil {
    return err
}

observerPolicy := kafka.ObserverPolicy{
    Observers: []kafka.ObserverFunc{instrumentation.Observer()},
    FailureHandler: func(_ context.Context, failure kafka.ObservationFailure) {
        // Report a stable local failure category. Do not export failure.Cause()
        // without application-owned redaction.
    },
    Timeout: 100 * time.Millisecond,
}

producer, err := kafka.NewProducer(kafka.ProducerConfig{
    // Normal producer policy omitted.
    Observers: observerPolicy,
})
```

The compiling examples in this module contain complete imports and setup.

## Guarantees and limitations

The [complete guide](docs/reference.md) defines ownership, failure semantics,
bounds, concurrency, security, and unsupported behavior. Do not infer
additional guarantees beyond the documented module boundary.

## Documentation

- [Documentation index](docs/README.md)
- [Complete technical guide](docs/reference.md)
- [Specification decision register](docs/specification-decisions.md)
- [Go API reference](https://pkg.go.dev/github.com/faustbrian/go-kafka/adapters/gotelemetry)
- [Parent package documentation](../../docs/README.md)

## Compatibility and support

This module follows its [compatibility policy](COMPATIBILITY.md). Report vulnerabilities through the
[parent security policy](../../SECURITY.md).

See the versioned [Golib ecosystem index](https://github.com/faustbrian/go-library-tools/blob/v1.4.0/docs/ecosystem/README.md)
and [Integration and data movement family guidance](https://github.com/faustbrian/go-library-tools/blob/v1.4.0/docs/ecosystem/design-language.md#package-families-and-selection)
for package selection and companion integrations.

## License

MIT. See [LICENSE](LICENSE).
