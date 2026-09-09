package mskiam_test

import (
	"context"
	"errors"
	"testing"

	mskiam "github.com/faustbrian/go-kafka/adapters/mskiam"
)

func TestLoadIsTheContextFirstConstructionContract(t *testing.T) {
	t.Parallel()

	invalid := mskiam.Config{}
	if provider, err := mskiam.Load(context.Background(), invalid); provider != nil || !errors.Is(err, mskiam.ErrInvalidConfig) {
		t.Fatalf("Load(invalid) = %#v, %v", provider, err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	config := mskiam.Config{Region: "eu-north-1"}
	if provider, err := mskiam.Load(canceled, config); provider != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Load(canceled) = %#v, %v", provider, err)
	}
	if provider, err := mskiam.New(canceled, config); provider != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("New(canceled) = %#v, %v", provider, err)
	}
}
