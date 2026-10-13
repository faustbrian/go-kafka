package kafkaslog

import (
	"context"
	"io"
	"log/slog"
	"testing"
)

// BenchmarkObserver measures synchronous JSON-handler observation to io.Discard;
// logger construction is outside the timer, without storage or network writes.
func BenchmarkObserver(b *testing.B) {
	adapter, err := New(Config{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	if err != nil {
		b.Fatalf("New() error = %v", err)
	}
	ctx := context.Background()
	observation := validObservation()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := adapter.Observer()(ctx, observation); err != nil {
			b.Fatalf("Observer() error = %v", err)
		}
	}
}
