package golog_test

import (
	"errors"
	"io"
	"log/slog"
	"testing"

	golog "github.com/faustbrian/go-kafka/adapters/golog"
	kafkaslog "github.com/faustbrian/go-kafka/adapters/slog"
)

func TestLegacyPathDelegatesToCanonicalSlogAdapter(t *testing.T) {
	t.Parallel()
	adapter, err := golog.New(golog.Config{})
	if adapter != nil || !errors.Is(err, golog.ErrLoggerRequired) {
		t.Fatalf("New() = %#v, %v", adapter, err)
	}
	if errors.Is(err, kafkaslog.ErrLoggerRequired) {
		t.Fatal("legacy path leaked the canonical error identity")
	}
	adapter, err = golog.New(golog.Config{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil || adapter == nil {
		t.Fatalf("New(valid) = %#v, %v", adapter, err)
	}
}
