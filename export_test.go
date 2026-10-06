package kafka

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// NewProducerWithDialerForTest constructs the production producer policy with
// a test-owned network dialer for broker fault injection.
func NewProducerWithDialerForTest(
	config ProducerConfig,
	dialer func(context.Context, string, string) (net.Conn, error),
) (*Producer, error) {
	return newProducer(config, func(options ...kgo.Opt) (*kgo.Client, error) {
		options = append(options, kgo.Dialer(dialer))

		return kgo.NewClient(options...)
	})
}

// NewTransactionProcessorWithDialerForTest constructs the production
// consume-transform-produce policy with a test-owned network dialer for broker
// fault injection.
func NewTransactionProcessorWithDialerForTest(
	config TransactionProcessorConfig,
	dialer func(context.Context, string, string) (net.Conn, error),
) (*TransactionProcessor, error) {
	return newTransactionProcessor(
		config,
		func(options ...kgo.Opt) (transactionProcessorBackend, error) {
			options = append(options, kgo.Dialer(dialer))

			return newFranzTransactionProcessorBackend(options...)
		},
	)
}

// BufferedConsumerRecordsForTest reports franz-go's current consumer buffer
// count for broker-test synchronization without adding it to the public API.
func BufferedConsumerRecordsForTest(consumer *Consumer) int64 {
	backend, ok := consumer.client.(interface {
		BufferedFetchRecords() int64
	})
	if !ok {
		return -1
	}

	return backend.BufferedFetchRecords()
}

// ShutdownConsumerForRebalanceTest applies the broker fixture shutdown policy
// without adding coordination helpers to the production API.
func ShutdownConsumerForRebalanceTest(
	ctx context.Context,
	shutdown func(context.Context) error,
) error {
	var shutdownErr error
	for {
		if ctx.Err() != nil {
			return errors.Join(shutdownErr, ctx.Err())
		}
		shutdownErr = shutdown(ctx)
		if shutdownErr == nil {
			return nil
		}
		if !errors.Is(shutdownErr, ErrConsumerShutdownIncomplete) &&
			!errors.Is(shutdownErr, ErrObserverReentry) {
			return shutdownErr
		}
		retryDelay := time.NewTimer(100 * time.Millisecond)
		select {
		case <-retryDelay.C:
		case <-ctx.Done():
			retryDelay.Stop()
			return errors.Join(shutdownErr, ctx.Err())
		}
	}
}
