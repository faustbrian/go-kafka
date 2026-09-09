package kafkaotel_test

import (
	"errors"
	"testing"

	kafkaotel "github.com/faustbrian/go-kafka/adapters/otel"
)

func TestCanonicalOTelAdapterExposesTheReleasedContract(t *testing.T) {
	t.Parallel()

	if _, err := kafkaotel.New(kafkaotel.Config{}); !errors.Is(err, kafkaotel.ErrRuntimeRequired) {
		t.Fatalf("New() error = %v", err)
	}
}
