package kafkaotel

import (
	"context"
	"testing"

	"github.com/faustbrian/go-kafka/v2"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// Dashboards and alerts select spans and metrics by instrumentation scope, so
// the scope name is a public contract; it changes only with the module major.
func TestInstrumentationScopeIsStable(t *testing.T) {
	const scope = "github.com/faustbrian/go-kafka/adapters/otel/v2"
	spans := tracetest.NewSpanRecorder()
	reader := sdkmetric.NewManualReader()
	instrumentation, err := New(Config{Runtime: testRuntime{
		tracerProvider: sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans)),
		meterProvider:  sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)),
	}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := instrumentation.Observer()(context.Background(), validContractObservation(kafka.ObservationReadiness, 0)); err != nil {
		t.Fatalf("Observer() error = %v", err)
	}
	ended := spans.Ended()
	if len(ended) != 1 || ended[0].InstrumentationScope().Name != scope {
		t.Fatalf("span scopes = %v, want %q", ended, scope)
	}
	var metrics metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &metrics); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	if len(metrics.ScopeMetrics) == 0 {
		t.Fatal("no scope metrics collected")
	}
	for _, scopeMetrics := range metrics.ScopeMetrics {
		if scopeMetrics.Scope.Name != scope {
			t.Fatalf("metric scope = %q, want %q", scopeMetrics.Scope.Name, scope)
		}
	}
}
