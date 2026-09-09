package kafkaslog_test

import (
	"context"
	"io"
	"log/slog"
	"time"

	kafka "github.com/faustbrian/go-kafka"
	kafkaslog "github.com/faustbrian/go-kafka/adapters/slog"
)

func Example() {
	adapter, err := kafkaslog.New(kafkaslog.Config{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Level:  slog.LevelInfo,
		Identities: kafkaslog.IdentityPolicy{
			AllowedClientIDs: []string{"orders-producer"},
			AllowedTopics:    []string{"orders"},
		},
	})
	if err != nil {
		panic(err)
	}

	observerPolicy := kafka.ObserverPolicy{
		Observers: []kafka.ObserverFunc{adapter.Observer()},
		FailureHandler: func(
			_ context.Context,
			_ kafka.ObservationFailure,
		) {
			// Report through an independent bounded failure path.
		},
		Timeout: 100 * time.Millisecond,
	}
	_ = observerPolicy
}
