package golog

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	kafka "github.com/faustbrian/go-kafka"
	kafkaslog "github.com/faustbrian/go-kafka/adapters/slog"
)

func TestCompatibilityFacadeTranslatesEveryCanonicalError(t *testing.T) {
	t.Parallel()

	unknown := errors.New("unknown")
	tests := []struct {
		input error
		want  error
	}{
		{nil, nil},
		{kafkaslog.ErrLoggerRequired, ErrLoggerRequired},
		{kafkaslog.ErrInvalidIdentityPolicy, ErrInvalidIdentityPolicy},
		{kafkaslog.ErrContextRequired, ErrContextRequired},
		{kafkaslog.ErrInvalidObservation, ErrInvalidObservation},
		{kafkaslog.ErrLoggerPanic, ErrLoggerPanic},
		{unknown, unknown},
	}
	for _, test := range tests {
		if got := translate(test.input); got != test.want {
			t.Fatalf("translate(%v) = %v, want %v", test.input, got, test.want)
		}
	}
}

func TestCompatibilityFacadePreservesValidationAndObservation(t *testing.T) {
	t.Parallel()

	if err := (IdentityPolicy{AllowedTopics: []string{"duplicate", "duplicate"}}).Validate(); !errors.Is(err, ErrInvalidIdentityPolicy) {
		t.Fatalf("IdentityPolicy.Validate() error = %v", err)
	}
	if err := (Config{}).Validate(); !errors.Is(err, ErrLoggerRequired) {
		t.Fatalf("Config.Validate() error = %v", err)
	}

	var nilAdapter *Adapter
	if err := nilAdapter.Observer()(nil, kafka.Observation{}); !errors.Is(err, ErrContextRequired) {
		t.Fatalf("nil Observer()(nil) error = %v", err)
	}
	if err := nilAdapter.Observer()(context.Background(), kafka.Observation{}); !errors.Is(err, ErrLoggerRequired) {
		t.Fatalf("nil Observer() error = %v", err)
	}

	adapter, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := adapter.Observer()(nil, kafka.Observation{}); !errors.Is(err, ErrContextRequired) {
		t.Fatalf("Observer(nil) error = %v", err)
	}
	if err := adapter.Observer()(context.Background(), kafka.Observation{}); !errors.Is(err, ErrInvalidObservation) {
		t.Fatalf("Observer(invalid) error = %v", err)
	}
	if err := adapter.Observer()(context.Background(), kafka.Observation{
		Kind:        kafka.ObservationProduceRecord,
		StartedAt:   time.Now(),
		Duration:    time.Millisecond,
		RecordCount: 1,
		Succeeded:   true,
	}); err != nil {
		t.Fatalf("Observer(valid) error = %v", err)
	}
}
