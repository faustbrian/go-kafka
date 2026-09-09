package gotelemetry

import (
	"context"
	"errors"
	"testing"
	"time"

	kafka "github.com/faustbrian/go-kafka"
	kafkaotel "github.com/faustbrian/go-kafka/adapters/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestCompatibilityFacadeTranslatesCanonicalErrors(t *testing.T) {
	t.Parallel()

	unknown := errors.New("unknown")
	tests := []struct {
		input error
		want  error
	}{
		{nil, nil},
		{kafkaotel.ErrRuntimeRequired, ErrRuntimeRequired},
		{kafkaotel.ErrInvalidAttributePolicy, ErrInvalidAttributePolicy},
		{kafkaotel.ErrContextRequired, ErrContextRequired},
		{kafkaotel.ErrInvalidObservation, ErrInvalidObservation},
		{kafkaotel.ErrInstrumentCreation, ErrInstrumentCreation},
		{kafkaotel.ErrProviderPanic, ErrProviderPanic},
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

	_, err := New(Config{})
	if !errors.Is(err, ErrRuntimeRequired) {
		t.Fatalf("New() error = %v", err)
	}
	if errors.Is(err, kafkaotel.ErrRuntimeRequired) {
		t.Fatal("legacy path leaked the canonical error identity")
	}
}

func TestCompatibilityFacadeRetainsLegacyInstrumentationScope(t *testing.T) {
	t.Parallel()

	spans := tracetest.NewSpanRecorder()
	reader := sdkmetric.NewManualReader()
	instrumentation, err := New(Config{Runtime: testRuntime{
		tracerProvider: sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans)),
		meterProvider:  sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)),
	}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := instrumentation.Observer()(context.Background(), kafka.Observation{
		Kind: kafka.ObservationProduceRecord, StartedAt: time.Unix(1, 0),
		Duration: time.Millisecond, RecordCount: 1, Succeeded: true,
	}); err != nil {
		t.Fatalf("Observer() error = %v", err)
	}

	ended := spans.Ended()
	if len(ended) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(ended))
	}
	const legacyScope = "github.com/faustbrian/go-kafka/adapters/gotelemetry"
	if got := ended[0].InstrumentationScope().Name; got != legacyScope {
		t.Fatalf("span instrumentation scope = %q, want %q", got, legacyScope)
	}
	var metrics metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &metrics); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	if len(metrics.ScopeMetrics) != 1 || metrics.ScopeMetrics[0].Scope.Name != legacyScope {
		t.Fatalf("metric instrumentation scopes = %#v, want %q", metrics.ScopeMetrics, legacyScope)
	}
}
