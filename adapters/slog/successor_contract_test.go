package kafkaslog_test

import (
	"errors"
	"log/slog"
	"testing"

	kafkaslog "github.com/faustbrian/go-kafka/adapters/slog"
)

func TestCanonicalSlogAdapterExposesTheReleasedContract(t *testing.T) {
	t.Parallel()

	if _, err := kafkaslog.New(kafkaslog.Config{}); !errors.Is(err, kafkaslog.ErrLoggerRequired) {
		t.Fatalf("New() error = %v", err)
	}
	acceptLevel := func(slog.Level) {}
	acceptLevel(kafkaslog.Config{}.Level)
}
