// Package kafkaservice preserves the original Kafka service adapter path.
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
	CallbackStartup CallbackOperation = iota + 1
	CallbackReadiness
	CallbackPublish
	CallbackHandler
	CallbackRun
	CallbackShutdown
)

var (
	ErrInvalidOptions     = errors.New("invalid kafka service options")
	ErrUnavailable        = errors.New("kafka service resource unavailable")
	ErrMissingCorrelation = errors.New("kafka service producer correlation missing")
	ErrCallbackPanic      = errors.New("kafka service callback panicked")
)

// OptionsError identifies one rejected option.
type OptionsError struct {
	Field  string
	Reason string
}

func (err *OptionsError) Error() string {
	return fmt.Sprintf("%s: %s: %v", err.Field, err.Reason, ErrInvalidOptions)
}

func (err *OptionsError) Unwrap() error { return ErrInvalidOptions }

// CallbackPanicError identifies a recovered callback without retaining its value.
type CallbackPanicError struct {
	Operation CallbackOperation
}

func (err *CallbackPanicError) Error() string {
	return fmt.Sprintf("kafka service %s callback panicked", callbackOperationName(err.Operation))
}

func (err *CallbackPanicError) Unwrap() error { return ErrCallbackPanic }

// CallbackError preserves an application callback failure without formatting it.
type CallbackError struct {
	Operation CallbackOperation
	Err       error
}

func (err *CallbackError) Error() string {
	return fmt.Sprintf("kafka service %s callback failed", callbackOperationName(err.Operation))
}

func (err *CallbackError) Unwrap() error { return err.Err }

// StartupError preserves validation and partial-cleanup failures.
type StartupError struct {
	Validation error
	Cleanup    error
}

func (err *StartupError) Error() string {
	if err.Cleanup != nil {
		return "kafka service startup validation and cleanup failed"
	}

	return "kafka service startup validation failed"
}

func (err *StartupError) Unwrap() []error {
	causes := []error{err.Validation}
	if err.Cleanup != nil {
		causes = append(causes, err.Cleanup)
	}

	return causes
}

type Startup[R any] func(context.Context, R) error
type Check[R any] func(context.Context, R) error
type Publish[R any] func(context.Context, R, kafka.ProducerRecord) (kafka.DeliveryResult, error)
type Shutdown[R any] func(context.Context, R) error
type Run[R any] func(context.Context, R, kafka.Handler) error

// ProducerOptions configure one Kafka producer lifecycle adapter.
type ProducerOptions[R any] struct {
	Name             string
	Resource         R
	Correlation      *correlation.Factory
	CorrelationCodec correlation.CodecOptions
	TracePropagator  propagation.TextMapPropagator
	MessageLimits    kafka.MessageLimits
	Startup          Startup[R]
	Readiness        Check[R]
	Publish          Publish[R]
	Shutdown         Shutdown[R]
}

// HandlerOptions configure a correlation-aware Kafka delivery boundary.
type HandlerOptions struct {
	Correlation           *correlation.Factory
	CorrelationCodec      correlation.CodecOptions
	TrustedMetadata       bool
	RejectInvalidMetadata bool
	TracePropagator       propagation.TextMapPropagator
	Handler               kafka.Handler
}

// ConsumerOptions configure one Kafka consumer lifecycle adapter.
type ConsumerOptions[R any] struct {
	Name                  string
	Resource              R
	Correlation           *correlation.Factory
	CorrelationCodec      correlation.CodecOptions
	TrustedMetadata       bool
	RejectInvalidMetadata bool
	TracePropagator       propagation.TextMapPropagator
	Handler               kafka.Handler
	Startup               Startup[R]
	Readiness             Check[R]
	Run                   Run[R]
	Shutdown              Shutdown[R]
}

// Producer retains the released type identity while delegating to the canonical adapter.
type Producer[R any] struct {
	canonical *canonical.Producer[R]
}

// Consumer retains the released type identity while delegating to the canonical adapter.
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

func (producer *Producer[R]) Resource() R { return producer.canonical.Resource() }

func (producer *Producer[R]) Component() service.Component {
	component := producer.canonical.Component()
	component.CloseAdmission = wrapNoContext(component.CloseAdmission)
	component.Start = wrapContext(component.Start)
	component.Stop = wrapContext(component.Stop)

	return component
}

func (producer *Producer[R]) Readiness() (service.ReadinessCheck, bool) {
	check, ok := producer.canonical.Readiness()
	if ok {
		check.Run = wrapContext(check.Run)
	}

	return check, ok
}

func (consumer *Consumer[R]) Resource() R { return consumer.canonical.Resource() }

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
