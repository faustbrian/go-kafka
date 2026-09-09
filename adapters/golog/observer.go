// Package golog preserves the original Kafka slog adapter import path.
//
// Deprecated: use github.com/faustbrian/go-kafka/adapters/slog.
package golog

import (
	"context"
	"errors"
	"log/slog"

	kafka "github.com/faustbrian/go-kafka"
	kafkaslog "github.com/faustbrian/go-kafka/adapters/slog"
)

var (
	// ErrLoggerRequired reports a missing slog logger.
	ErrLoggerRequired = errors.New("kafka/golog: logger is required")
	// ErrInvalidIdentityPolicy reports an unbounded, duplicated, or invalid
	// logging identity allowlist.
	ErrInvalidIdentityPolicy = errors.New("kafka/golog: identity policy is invalid")
	// ErrContextRequired reports a nil observer context.
	ErrContextRequired = errors.New("kafka/golog: context is required")
	// ErrInvalidObservation reports metadata outside the root observation
	// contract.
	ErrInvalidObservation = errors.New("kafka/golog: observation is invalid")
	// ErrLoggerPanic identifies a contained slog handler panic without
	// retaining or rendering its potentially sensitive panic value.
	ErrLoggerPanic = errors.New("kafka/golog: logger panicked")
)

// IdentityPolicy explicitly bounds Kafka identities admitted to logs. Empty
// allowlists omit the corresponding identity. Values are matched exactly and
// copied during construction.
type IdentityPolicy struct {
	// AllowedClientIDs contains exact Kafka client IDs permitted in logs.
	AllowedClientIDs []string
	// AllowedTopics contains exact Kafka topic names permitted in logs.
	AllowedTopics []string
	// AllowedConsumerGroups contains exact Kafka consumer-group IDs permitted
	// in logs.
	AllowedConsumerGroups []string
}

// Validate reports whether every identity allowlist is bounded, unique, and
// safe to copy into structured logs.
func (policy IdentityPolicy) Validate() error {
	return translate(policy.canonical().Validate())
}

func (policy IdentityPolicy) canonical() kafkaslog.IdentityPolicy {
	return kafkaslog.IdentityPolicy{
		AllowedClientIDs:      policy.AllowedClientIDs,
		AllowedTopics:         policy.AllowedTopics,
		AllowedConsumerGroups: policy.AllowedConsumerGroups,
	}
}

// Config owns immutable adapter dependencies and identity policy.
type Config struct {
	// Logger is required and remains application-owned. New retains the
	// immutable logger pointer but does not modify its handler configuration.
	Logger *slog.Logger
	// Level is the level supplied to Logger. Its zero value is slog.LevelInfo.
	Level slog.Level
	// Identities is defensively copied by New. Its zero value denies every
	// client, topic, and consumer-group identity.
	Identities IdentityPolicy
}

// Validate checks the logger and identity policy without retaining either.
func (config Config) Validate() error {
	return translate(config.canonical().Validate())
}

func (config Config) canonical() kafkaslog.Config {
	return kafkaslog.Config{
		Logger:     config.Logger,
		Level:      config.Level,
		Identities: config.Identities.canonical(),
	}
}

// Adapter is the compatibility facade for the canonical slog adapter. It
// starts no goroutines and is safe for concurrent observer calls when the
// supplied slog handler satisfies slog.Handler's concurrency contract.
type Adapter struct {
	canonical *kafkaslog.Adapter
}

// New validates and defensively copies configuration.
//
// Deprecated: use github.com/faustbrian/go-kafka/adapters/slog.New.
func New(config Config) (*Adapter, error) {
	adapter, err := kafkaslog.New(config.canonical())
	if err != nil {
		return nil, translate(err)
	}
	return &Adapter{canonical: adapter}, nil
}

// Observer returns the synchronous Kafka observer owned by this adapter. The
// returned function does not retain callback contexts or observations.
func (adapter *Adapter) Observer() kafka.ObserverFunc {
	return func(ctx context.Context, observation kafka.Observation) error {
		if ctx == nil {
			return ErrContextRequired
		}
		if adapter == nil || adapter.canonical == nil {
			return ErrLoggerRequired
		}
		return translate(adapter.canonical.Observer()(ctx, observation))
	}
}

func translate(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, kafkaslog.ErrLoggerRequired):
		return ErrLoggerRequired
	case errors.Is(err, kafkaslog.ErrInvalidIdentityPolicy):
		return ErrInvalidIdentityPolicy
	case errors.Is(err, kafkaslog.ErrContextRequired):
		return ErrContextRequired
	case errors.Is(err, kafkaslog.ErrInvalidObservation):
		return ErrInvalidObservation
	case errors.Is(err, kafkaslog.ErrLoggerPanic):
		return ErrLoggerPanic
	default:
		return err
	}
}
