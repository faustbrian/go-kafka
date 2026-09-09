package kafkaservice_test

import (
	"testing"

	kafkaservice "github.com/faustbrian/go-kafka/adapters/service"
)

func TestCanonicalServiceAdapterExposesTheReleasedContract(t *testing.T) {
	t.Parallel()

	var options kafkaservice.ProducerOptions[int]
	if options.Name != "" {
		t.Fatalf("zero ProducerOptions.Name = %q", options.Name)
	}
}
