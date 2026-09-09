// Package gotelemetry preserves the original Kafka OpenTelemetry adapter path.
//
// Deprecated: use github.com/faustbrian/go-kafka/adapters/otel.
package gotelemetry

import (
	"context"
	"errors"
	"reflect"

	kafka "github.com/faustbrian/go-kafka"
	kafkaotel "github.com/faustbrian/go-kafka/adapters/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// MessagingSemanticConventionVersion identifies the selected messaging convention.
const MessagingSemanticConventionVersion = kafkaotel.MessagingSemanticConventionVersion

const legacyInstrumentationName = "github.com/faustbrian/go-kafka/adapters/gotelemetry"

var (
	// ErrRuntimeRequired reports a missing or incomplete telemetry runtime.
	ErrRuntimeRequired = errors.New("kafka/gotelemetry: runtime is required")
	// ErrInvalidAttributePolicy reports an invalid identity allowlist.
	ErrInvalidAttributePolicy = errors.New("kafka/gotelemetry: attribute policy is invalid")
	// ErrContextRequired reports a nil observer context.
	ErrContextRequired = errors.New("kafka/gotelemetry: context is required")
	// ErrInvalidObservation reports metadata outside the root observation contract.
	ErrInvalidObservation = errors.New("kafka/gotelemetry: observation is invalid")
	// ErrInstrumentCreation categorizes instrument construction failures.
	ErrInstrumentCreation = errors.New("kafka/gotelemetry: instrument creation failed")
	// ErrProviderPanic reports a contained provider panic.
	ErrProviderPanic = errors.New("kafka/gotelemetry: provider panicked")
)

// Runtime is the standard-provider surface needed by this adapter.
type Runtime interface {
	TracerProvider() trace.TracerProvider
	MeterProvider() metric.MeterProvider
}

// AttributePolicy explicitly bounds Kafka identities admitted to telemetry.
type AttributePolicy struct {
	AllowedClientIDs      []string
	AllowedTopics         []string
	AllowedConsumerGroups []string
}

// Validate reports whether every identity allowlist is bounded, unique, and safe.
func (policy AttributePolicy) Validate() error {
	return translate(policy.canonical().Validate())
}

func (policy AttributePolicy) canonical() kafkaotel.AttributePolicy {
	return kafkaotel.AttributePolicy{
		AllowedClientIDs:      policy.AllowedClientIDs,
		AllowedTopics:         policy.AllowedTopics,
		AllowedConsumerGroups: policy.AllowedConsumerGroups,
	}
}

// Config owns immutable adapter dependencies and cardinality policy.
type Config struct {
	Runtime    Runtime
	Attributes AttributePolicy
}

// Validate checks dependencies and cardinality policy without creating instruments.
func (config Config) Validate() error {
	return translate(config.canonical().Validate())
}

func (config Config) canonical() kafkaotel.Config {
	runtime := kafkaotel.Runtime(config.Runtime)
	if !nilInterface(config.Runtime) {
		runtime = legacyRuntime{Runtime: config.Runtime}
	}

	return kafkaotel.Config{
		Runtime:    runtime,
		Attributes: config.Attributes.canonical(),
	}
}

type legacyRuntime struct{ Runtime }

func (runtime legacyRuntime) TracerProvider() trace.TracerProvider {
	provider := runtime.Runtime.TracerProvider()
	if nilInterface(provider) {
		return nil
	}

	return legacyTracerProvider{TracerProvider: provider}
}

func (runtime legacyRuntime) MeterProvider() metric.MeterProvider {
	provider := runtime.Runtime.MeterProvider()
	if nilInterface(provider) {
		return nil
	}

	return legacyMeterProvider{MeterProvider: provider}
}

type legacyTracerProvider struct{ trace.TracerProvider }

func (provider legacyTracerProvider) Tracer(
	_ string,
	options ...trace.TracerOption,
) trace.Tracer {
	return provider.TracerProvider.Tracer(legacyInstrumentationName, options...)
}

type legacyMeterProvider struct{ metric.MeterProvider }

func (provider legacyMeterProvider) Meter(
	_ string,
	options ...metric.MeterOption,
) metric.Meter {
	return provider.MeterProvider.Meter(legacyInstrumentationName, options...)
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

// InstrumentError preserves a provider failure while returning a stable diagnostic.
type InstrumentError struct {
	cause error
}

// Error implements error without exposing provider diagnostics.
func (*InstrumentError) Error() string { return ErrInstrumentCreation.Error() }

// Unwrap preserves both the stable category and provider cause.
func (err *InstrumentError) Unwrap() []error {
	return []error{ErrInstrumentCreation, err.cause}
}

// Instrumentation is the compatibility facade for the canonical adapter.
type Instrumentation struct {
	canonical *kafkaotel.Instrumentation
}

// New validates and copies configuration before constructing instruments.
//
// Deprecated: use github.com/faustbrian/go-kafka/adapters/otel.New.
func New(config Config) (*Instrumentation, error) {
	instrumentation, err := kafkaotel.New(config.canonical())
	if err != nil {
		return nil, translate(err)
	}

	return &Instrumentation{canonical: instrumentation}, nil
}

// Observer returns the synchronous Kafka observer owned by this instrumentation.
func (instrumentation *Instrumentation) Observer() kafka.ObserverFunc {
	return func(ctx context.Context, observation kafka.Observation) error {
		if ctx == nil {
			return ErrContextRequired
		}
		if instrumentation == nil || instrumentation.canonical == nil {
			return ErrRuntimeRequired
		}

		return translate(instrumentation.canonical.Observer()(ctx, observation))
	}
}

func translate(err error) error {
	if err == nil {
		return nil
	}

	var instrumentError *kafkaotel.InstrumentError
	if errors.As(err, &instrumentError) {
		causes := instrumentError.Unwrap()
		cause := error(kafkaotel.ErrInstrumentCreation)
		if len(causes) > 1 {
			cause = causes[1]
		}

		return &InstrumentError{cause: cause}
	}

	switch {
	case errors.Is(err, kafkaotel.ErrRuntimeRequired):
		return ErrRuntimeRequired
	case errors.Is(err, kafkaotel.ErrInvalidAttributePolicy):
		return ErrInvalidAttributePolicy
	case errors.Is(err, kafkaotel.ErrContextRequired):
		return ErrContextRequired
	case errors.Is(err, kafkaotel.ErrInvalidObservation):
		return ErrInvalidObservation
	case errors.Is(err, kafkaotel.ErrInstrumentCreation):
		return ErrInstrumentCreation
	case errors.Is(err, kafkaotel.ErrProviderPanic):
		return ErrProviderPanic
	default:
		return err
	}
}
