# Kafka Amazon MSK IAM adapter

`mskiam` is the independently versioned Amazon MSK IAM authentication adapter
for [`github.com/faustbrian/go-kafka`](../..). The root Kafka module
remains AWS-independent.

Use this adapter only with Amazon MSK clusters whose IAM access control is
enabled. It generates the SASL/OAUTHBEARER token required by non-Java clients
through AWS's supported Go signer. It does not implement SigV4, Kafka SASL, or
credential discovery. It performs one bounded invalidation and retrieval when a
refresh-capable credential provider returns credentials too close to expiry.

## Install

```sh
go get github.com/faustbrian/go-kafka/adapters/mskiam@v1
```

## Quick start

```go
provider, err := mskiam.New(ctx, mskiam.Config{
    Region:       "eu-north-1",
    TokenTimeout: 5 * time.Second,
})
if err != nil {
    return err
}

producer, err := kafka.NewProducer(kafka.ProducerConfig{
    Brokers: []string{
        "b-1.example.kafka.eu-north-1.amazonaws.com:9098",
        "b-2.example.kafka.eu-north-1.amazonaws.com:9098",
    },
    ClientID:      "orders-producer",
    AllowedTopics: []string{"orders"},
    Security: kafka.ClientSecurity{
        Authentication: kafka.NewOAuthBearerAuthentication(provider),
    },
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
- [Go API reference](https://pkg.go.dev/github.com/faustbrian/go-kafka/adapters/mskiam)
- [Parent package documentation](../../docs/README.md)

## Compatibility and support

This module follows its [compatibility policy](COMPATIBILITY.md). Report vulnerabilities through the
[parent security policy](../../SECURITY.md).

## License

MIT. See [LICENSE](LICENSE).
