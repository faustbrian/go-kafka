// Package kafkaservice preserves the original Kafka service adapter path.
//
// Constructors delegate to adapters/service while preserving legacy public
// type/error identities. Resource/callback ownership, cooperative start/stop,
// correlation trust and retryable teardown follow the canonical contracts.
// The facade is not a new supervisor or broker transaction boundary.
//
// Deprecated: use github.com/faustbrian/go-kafka/adapters/service.
package kafkaservice

import (
	"context"
	"errors"
	"fmt"

	"github.com/faustbrian/go-correlation"
	kafka "github.com/faustbrian/go-kafka"
	canonical "github.com/faustbrian/go-kafka/adapters/service"
	"github.com/faustbrian/go-service"
	"go.opentelemetry.io/otel/propagation"
)

// MaxNameBytes bounds component, task, and readiness identifiers.
const MaxNameBytes = canonical.MaxNameBytes

// CallbackOperation identifies one application callback boundary.
type CallbackOperation uint8

const (
	// CallbackStartup identifies borrowed startup validation.
	CallbackStartup CallbackOperation = iota + 1
	// CallbackReadiness identifies borrowed dependency evaluation.
	CallbackReadiness
	// CallbackPublish identifies publication/propagation callbacks.
	CallbackPublish
	// CallbackHandler identifies inbound application handling.
	CallbackHandler
	// CallbackRun identifies supervised intake.
	CallbackRun
	// CallbackShutdown identifies transferred resource cleanup.
	CallbackShutdown
)

var (
	// ErrInvalidOptions identifies rejected service adapter options.
	ErrInvalidOptions = errors.New("invalid kafka service options")
	// ErrUnavailable identifies a resource that is not admitted for use.
	ErrUnavailable = errors.New("kafka service resource unavailable")
	// ErrMissingCorrelation identifies publication without required correlation.
	ErrMissingCorrelation = errors.New("kafka service producer correlation missing")
	// ErrCallbackPanic identifies a contained application callback panic.
	ErrCallbackPanic = errors.New("kafka service callback panicked")
)

// OptionsError identifies one rejected option.
type OptionsError struct {
	// Field identifies a rejected option; caller-constructed errors remain caller-owned
	// data.
	Field string
	// Reason is the adapter-generated safe category; callers must not put secrets in
	// custom values.
	Reason string
}

// Error formats Field and Reason with ErrInvalidOptions; caller-constructed
// values must not contain confidential data.
func (err *OptionsError) Error() string {
	return fmt.Sprintf("%s: %s: %v", err.Field, err.Reason, ErrInvalidOptions)
}

// Unwrap returns ErrInvalidOptions.
func (err *OptionsError) Unwrap() error { return ErrInvalidOptions }

// CallbackPanicError identifies a recovered callback without retaining its value.
type CallbackPanicError struct {
	// Operation identifies the contained callback boundary; panic values are not
	// retained.
	Operation CallbackOperation
}

// Error identifies the callback boundary without retaining or formatting a
// panic value.
func (err *CallbackPanicError) Error() string {
	return fmt.Sprintf("kafka service %s callback panicked", callbackOperationName(err.Operation))
}

// Unwrap returns ErrCallbackPanic without retaining the panic value.
func (err *CallbackPanicError) Unwrap() error { return ErrCallbackPanic }

// CallbackError preserves an application callback failure without formatting it.
type CallbackError struct {
	// Operation identifies the failed callback boundary.
	Operation CallbackOperation
	// Err is the original unredacted callback cause available through Unwrap.
	Err error
}

// Error identifies the failed boundary without formatting Err; Unwrap still
// exposes that original cause.
func (err *CallbackError) Error() string {
	return fmt.Sprintf("kafka service %s callback failed", callbackOperationName(err.Operation))
}

// Unwrap exposes the original unredacted callback cause.
func (err *CallbackError) Unwrap() error { return err.Err }

// StartupError preserves validation and partial-cleanup failures.
type StartupError struct {
	// Validation is the startup failure and retains its original programmatic cause.
	Validation error
	// Cleanup optionally reports transferred-resource teardown failure; callers must
	// redact unwrapped causes.
	Cleanup error
}

// Error distinguishes validation failure from validation plus cleanup failure
// without formatting either cause.
func (err *StartupError) Error() string {
	if err.Cleanup != nil {
		return "kafka service startup validation and cleanup failed"
	}

	return "kafka service startup validation failed"
}

// Unwrap returns the original validation and optional cleanup causes; callers
// must redact them before rendering.
func (err *StartupError) Unwrap() []error {
	causes := []error{err.Validation}
	if err.Cleanup != nil {
		causes = append(causes, err.Cleanup)
	}

	return causes
}

// Startup is borrowed synchronous validation before resource admission.
type Startup[R any] func(context.Context, R) error

// Check is borrowed synchronous readiness policy, not lifecycle teardown.
type Check[R any] func(context.Context, R) error

// Publish is a borrowed synchronous concrete publication callback; its result
// is not atomic application settlement.
type Publish[R any] func(context.Context, R, kafka.ProducerRecord) (kafka.DeliveryResult, error)

// Shutdown transfers resource cleanup and must permit retry after an error.
type Shutdown[R any] func(context.Context, R) error

// Run owns intake and must join admitted callbacks before returning.
type Run[R any] func(context.Context, R, kafka.Handler) error

// ProducerOptions configure one Kafka producer lifecycle adapter.
type ProducerOptions[R any] struct {
	// Name is the bounded component/readiness identity; do not use confidential data.
	Name string
	// Resource is the exact borrowed concrete producer; shutdown ownership transfers
	// only when Shutdown is supplied.
	Resource R
	// Correlation is a borrowed factory creating a fresh request ID per produced record.
	Correlation *correlation.Factory
	// CorrelationCodec fixes bounded metadata field names and validation.
	CorrelationCodec correlation.CodecOptions
	// TracePropagator is optional borrowed propagation policy; trace metadata is not
	// business authorization.
	TracePropagator propagation.TextMapPropagator
	// MessageLimits defaults to kafka.DefaultMessageLimits and is applied before copying
	// and after propagation.
	MessageLimits kafka.MessageLimits
	// Startup is an optional borrowed validation callback, invoked outside the adapter
	// lock.
	Startup Startup[R]
	// Readiness is an opt-in borrowed dependency check after startup.
	Readiness Check[R]
	// Publish is the borrowed synchronous concrete publication callback.
	Publish Publish[R]
	// Shutdown optionally transfers cleanup ownership; successful completion is retained
	// and failures permit retry.
	Shutdown Shutdown[R]
}

// HandlerOptions configure a correlation-aware Kafka delivery boundary.
type HandlerOptions struct {
	// Correlation is a borrowed factory creating a fresh delivery-attempt request ID.
	Correlation *correlation.Factory
	// CorrelationCodec fixes bounded metadata field names and validation.
	CorrelationCodec correlation.CodecOptions
	// TrustedMetadata permits preserving valid inbound correlation; it does not
	// authenticate the sender.
	TrustedMetadata bool
	// RejectInvalidMetadata refuses malformed correlation instead of creating a new
	// local workflow.
	RejectInvalidMetadata bool
	// TracePropagator is optional borrowed trace extraction policy, separate from
	// correlation trust.
	TracePropagator propagation.TextMapPropagator
	// Handler is borrowed application work; it must cooperate with cancellation and
	// support configured concurrency.
	Handler kafka.Handler
}

// ConsumerOptions configure one Kafka consumer lifecycle adapter.
type ConsumerOptions[R any] struct {
	// Name is the bounded component/task/readiness identity.
	Name string
	// Resource is the exact concrete consumer whose cleanup ownership transfers through
	// required Shutdown.
	Resource R
	// Correlation is a borrowed factory creating a new request ID for each delivery.
	Correlation *correlation.Factory
	// CorrelationCodec fixes bounded metadata field names and validation.
	CorrelationCodec correlation.CodecOptions
	// TrustedMetadata permits valid inbound correlation, not sender authentication.
	TrustedMetadata bool
	// RejectInvalidMetadata refuses malformed correlation instead of replacing it.
	RejectInvalidMetadata bool
	// TracePropagator is optional borrowed trace extraction policy.
	TracePropagator propagation.TextMapPropagator
	// Handler is borrowed application work; it must cooperate with cancellation and
	// support configured concurrency.
	Handler kafka.Handler
	// Startup is optional borrowed validation before intake.
	Startup Startup[R]
	// Readiness is optional borrowed dependency policy after startup.
	Readiness Check[R]
	// Run owns synchronous intake and must join admitted handlers before returning.
	Run Run[R]
	// Shutdown is required transferred teardown; failures may be retried with later stop
	// contexts.
	Shutdown Shutdown[R]
}

// Producer retains the released type identity while delegating to the
// canonical adapter.
type Producer[R any] struct {
	canonical *canonical.Producer[R]
}

// Consumer retains the released type identity while delegating to the
// canonical adapter.
type Consumer[R any] struct {
	canonical *canonical.Consumer[R]
}

// NewProducer validates and constructs an inert producer adapter.
//
// Deprecated: use github.com/faustbrian/go-kafka/adapters/service.NewProducer.
func NewProducer[R any](options ProducerOptions[R]) (*Producer[R], error) {
	producer, err := canonical.NewProducer(canonical.ProducerOptions[R]{
		Name: options.Name, Resource: options.Resource,
		Correlation: options.Correlation, CorrelationCodec: options.CorrelationCodec,
		TracePropagator: options.TracePropagator, MessageLimits: options.MessageLimits,
		Startup: canonical.Startup[R](options.Startup), Readiness: canonical.Check[R](options.Readiness),
		Publish: canonical.Publish[R](options.Publish), Shutdown: canonical.Shutdown[R](options.Shutdown),
	})
	if err != nil {
		return nil, translate(err)
	}

	return &Producer[R]{canonical: producer}, nil
}

// NewConsumer validates and constructs an inert consumer adapter.
//
// Deprecated: use github.com/faustbrian/go-kafka/adapters/service.NewConsumer.
func NewConsumer[R any](options ConsumerOptions[R]) (*Consumer[R], error) {
	consumer, err := canonical.NewConsumer(canonical.ConsumerOptions[R]{
		Name: options.Name, Resource: options.Resource,
		Correlation: options.Correlation, CorrelationCodec: options.CorrelationCodec,
		TrustedMetadata: options.TrustedMetadata, RejectInvalidMetadata: options.RejectInvalidMetadata,
		TracePropagator: options.TracePropagator, Handler: options.Handler,
		Startup: canonical.Startup[R](options.Startup), Readiness: canonical.Check[R](options.Readiness),
		Run: canonical.Run[R](options.Run), Shutdown: canonical.Shutdown[R](options.Shutdown),
	})
	if err != nil {
		return nil, translate(err)
	}

	return &Consumer[R]{canonical: consumer}, nil
}

// Resource returns the exact caller-supplied resource; it does not clone or
// transfer new ownership.
func (producer *Producer[R]) Resource() R { return producer.canonical.Resource() }

// Component returns service lifecycle callbacks preserving canonical
// admission, join and retryable teardown behavior.
func (producer *Producer[R]) Component() service.Component {
	component := producer.canonical.Component()
	component.CloseAdmission = wrapNoContext(component.CloseAdmission)
	component.Start = wrapContext(component.Start)
	component.Stop = wrapContext(component.Stop)

	return component
}

// Readiness returns the opt-in dependency check and false when none is configured.
func (producer *Producer[R]) Readiness() (service.ReadinessCheck, bool) {
	check, ok := producer.canonical.Readiness()
	if ok {
		check.Run = wrapContext(check.Run)
	}

	return check, ok
}

// Resource returns the exact caller-supplied resource; it does not clone or
// transfer new ownership.
func (consumer *Consumer[R]) Resource() R { return consumer.canonical.Resource() }

// Plan returns canonical lifecycle/task/readiness wiring with legacy error
// translation, not an independently running supervisor.
func (consumer *Consumer[R]) Plan() service.Plan {
	plan := consumer.canonical.Plan()
	for index := range plan.Components {
		plan.Components[index].CloseAdmission = wrapNoContext(plan.Components[index].CloseAdmission)
		plan.Components[index].Start = wrapContext(plan.Components[index].Start)
		plan.Components[index].Stop = wrapContext(plan.Components[index].Stop)
	}
	for index := range plan.Tasks {
		plan.Tasks[index].Run = wrapContext(plan.Tasks[index].Run)
	}
	for index := range plan.Readiness {
		plan.Readiness[index].Run = wrapContext(plan.Readiness[index].Run)
	}

	return plan
}

// Publish delegates owned record/correlation propagation and delivery to the
// canonical adapter, translating wrapper errors without acknowledging a source.
func (producer *Producer[R]) Publish(
	ctx context.Context,
	record kafka.ProducerRecord,
) (correlation.Values, kafka.DeliveryResult, error) {
	values, delivery, err := producer.canonical.Publish(ctx, record)

	return values, delivery, translate(err)
}

// NewHandler wraps application work with a fresh delivery-attempt request ID.
//
// Deprecated: use github.com/faustbrian/go-kafka/adapters/service.NewHandler.
func NewHandler(options HandlerOptions) (kafka.Handler, error) {
	handler, err := canonical.NewHandler(canonical.HandlerOptions{
		Correlation: options.Correlation, CorrelationCodec: options.CorrelationCodec,
		TrustedMetadata: options.TrustedMetadata, RejectInvalidMetadata: options.RejectInvalidMetadata,
		TracePropagator: options.TracePropagator, Handler: options.Handler,
	})
	if err != nil {
		return nil, translate(err)
	}

	return kafka.HandlerFunc(func(ctx context.Context, record kafka.ConsumedMessage) error {
		return translate(handler.Handle(ctx, record))
	}), nil
}

func wrapNoContext(callback func() error) func() error {
	if callback == nil {
		return nil
	}

	return func() error { return translate(callback()) }
}

func wrapContext(callback func(context.Context) error) func(context.Context) error {
	if callback == nil {
		return nil
	}

	return func(ctx context.Context) error { return translate(callback(ctx)) }
}

func translate(err error) error {
	if err == nil {
		return nil
	}

	switch current := err.(type) {
	case *canonical.OptionsError:
		optionsError := current
		return &OptionsError{Field: optionsError.Field, Reason: optionsError.Reason}
	case *canonical.StartupError:
		startupError := current
		return &StartupError{
			Validation: translate(startupError.Validation),
			Cleanup:    translate(startupError.Cleanup),
		}
	case *canonical.CallbackPanicError:
		panicError := current
		return &CallbackPanicError{Operation: CallbackOperation(panicError.Operation)}
	case *canonical.CallbackError:
		callbackError := current
		return &CallbackError{
			Operation: CallbackOperation(callbackError.Operation),
			Err:       callbackError.Err,
		}
	}

	switch {
	case errors.Is(err, canonical.ErrInvalidOptions):
		return ErrInvalidOptions
	case errors.Is(err, canonical.ErrUnavailable):
		return ErrUnavailable
	case errors.Is(err, canonical.ErrMissingCorrelation):
		return ErrMissingCorrelation
	case errors.Is(err, canonical.ErrCallbackPanic):
		return ErrCallbackPanic
	default:
		return err
	}
}

func callbackOperationName(operation CallbackOperation) string {
	switch operation {
	case CallbackStartup:
		return "startup"
	case CallbackReadiness:
		return "readiness"
	case CallbackPublish:
		return "publish"
	case CallbackHandler:
		return "handler"
	case CallbackRun:
		return "run"
	case CallbackShutdown:
		return "shutdown"
	default:
		return "unknown"
	}
}
