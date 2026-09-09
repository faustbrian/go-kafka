package kafkaservice

import (
	"errors"
	"testing"

	canonical "github.com/faustbrian/go-kafka/adapters/service"
)

func TestCompatibilityFacadeTranslatesCanonicalErrors(t *testing.T) {
	t.Parallel()

	unknown := errors.New("unknown")
	tests := []struct {
		input error
		want  error
	}{
		{nil, nil},
		{canonical.ErrInvalidOptions, ErrInvalidOptions},
		{canonical.ErrUnavailable, ErrUnavailable},
		{canonical.ErrMissingCorrelation, ErrMissingCorrelation},
		{canonical.ErrCallbackPanic, ErrCallbackPanic},
		{unknown, unknown},
	}
	for _, test := range tests {
		if got := translate(test.input); got != test.want {
			t.Fatalf("translate(%v) = %v, want %v", test.input, got, test.want)
		}
	}
}

func TestCompatibilityFacadeRetainsLegacyErrorIdentity(t *testing.T) {
	t.Parallel()

	_, err := NewProducer(ProducerOptions[int]{})
	if !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("NewProducer() error = %v", err)
	}
	if errors.Is(err, canonical.ErrInvalidOptions) {
		t.Fatal("legacy path leaked the canonical error identity")
	}
}

func TestCompatibilityFacadeRetainsCallbackWrapperAndCause(t *testing.T) {
	t.Parallel()

	cause := &canonical.OptionsError{Field: "callback", Reason: "failed"}
	err := translate(&canonical.CallbackError{
		Operation: canonical.CallbackPublish,
		Err:       cause,
	})
	var callback *CallbackError
	if !errors.As(err, &callback) || callback.Operation != CallbackPublish {
		t.Fatalf("translate() error = %#v, want publish CallbackError", err)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("translate() discarded original callback cause = %#v", err)
	}
	var options *OptionsError
	if errors.As(err, &options) {
		t.Fatalf("translate() reclassified callback cause as legacy options error = %#v", err)
	}
}
