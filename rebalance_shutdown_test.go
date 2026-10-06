package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestRebalanceShutdownWaitsForObserverCompletion(t *testing.T) {
	backend := &recordingConsumerBackend{}
	consumer := consumerWithBackend(backend, 10, time.Second, time.Second)
	entered := make(chan error, 1)
	release := make(chan struct{})
	done := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	policy, err := normalizeObserverPolicy(ObserverPolicy{
		Timeout: 5 * time.Second,
		Observers: []ObserverFunc{func(ctx context.Context, observation Observation) error {
			if observation.Kind != ObservationConsumePoll {
				return nil
			}
			entered <- consumer.Shutdown(ctx)
			<-release
			return nil
		}},
		FailureHandler: func(context.Context, ObservationFailure) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	consumer.observers = newObserverDispatcher(policy)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() {
		defer close(done)
		consumer.dispatchObservation(context.Background(), Observation{Kind: ObservationConsumePoll})
	}()
	t.Cleanup(func() {
		unblock()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("observer did not finish")
		}
	})
	select {
	case callbackErr := <-entered:
		if !errors.Is(callbackErr, ErrObserverReentry) {
			t.Fatalf("callback shutdown = %v", callbackErr)
		}
	case <-ctx.Done():
		t.Fatal("observer did not enter")
	}
	firstRejection := false
	err = ShutdownConsumerForRebalanceTest(ctx, func(attemptCtx context.Context) error {
		shutdownErr := consumer.Shutdown(attemptCtx)
		if !firstRejection {
			firstRejection = true
			if !errors.Is(shutdownErr, ErrObserverReentry) {
				t.Fatalf("external shutdown during observer = %v", shutdownErr)
			}
			if consumer.closing || consumer.closed || backend.closed != 0 {
				t.Fatal("rejected shutdown fenced or closed consumer")
			}
			unblock()
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("observer did not finish")
			}
		}
		return shutdownErr
	})
	if err != nil {
		t.Fatalf("rebalance shutdown after observer completion: %v", err)
	}
	if !consumer.closed || backend.closed != 1 {
		t.Fatalf("closed consumer/backend = %v/%d", consumer.closed, backend.closed)
	}
	if err := consumer.Shutdown(context.Background()); err != nil {
		t.Fatalf("idempotent shutdown = %v", err)
	}
	if backend.closed != 1 {
		t.Fatalf("backend closed %d times", backend.closed)
	}
}

func TestRebalanceShutdownRetainsTerminalErrors(t *testing.T) {
	terminal := errors.New("terminal shutdown failure")
	err := ShutdownConsumerForRebalanceTest(context.Background(), func(context.Context) error { return terminal })
	if !errors.Is(err, terminal) {
		t.Fatalf("terminal shutdown = %v", err)
	}
}

func TestRebalanceShutdownRetainsCancellationAndRetryCause(t *testing.T) {
	for _, retryCause := range []error{ErrObserverReentry, ErrConsumerShutdownIncomplete} {
		t.Run(retryCause.Error(), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			err := ShutdownConsumerForRebalanceTest(ctx, func(context.Context) error {
				cancel()
				return retryCause
			})
			if !errors.Is(err, retryCause) || !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled retry = %v", err)
			}
		})
	}
}

func TestRebalanceShutdownPreservesExistingDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := ShutdownConsumerForRebalanceTest(ctx, func(context.Context) error { return ErrObserverReentry })
	if !errors.Is(err, ErrObserverReentry) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired retry = %v", err)
	}
}

func TestRebalanceShutdownRetriesIncompleteWithinOneDeadline(t *testing.T) {
	backend := &recordingConsumerBackend{leaveErr: context.Canceled}
	consumer := consumerWithBackend(backend, 10, time.Second, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := ShutdownConsumerForRebalanceTest(ctx, func(attemptCtx context.Context) error {
		if backend.leaveCalls == 3 {
			backend.leaveErr = nil
		}
		return consumer.Shutdown(attemptCtx)
	})
	if err != nil {
		t.Fatalf("shutdown after transient group-leave failures = %v", err)
	}
	if !consumer.closed || backend.closed != 1 || backend.leaveCalls != 4 {
		t.Fatalf("consumer closed/backend close/leave = %v/%d/%d", consumer.closed, backend.closed, backend.leaveCalls)
	}
}
