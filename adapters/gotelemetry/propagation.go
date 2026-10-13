package gotelemetry

import (
	"context"

	kafka "github.com/faustbrian/go-kafka"
	kafkaotel "github.com/faustbrian/go-kafka/adapters/otel"
)

// TraceContextPropagation delegates the canonical owned W3C header policy,
// excluding baggage and global propagators; remote context is not authentication.
type TraceContextPropagation struct {
	canonical kafkaotel.TraceContextPropagation
}

// NewTraceContextPropagation validates the Kafka record limits used around trace headers.
func NewTraceContextPropagation(limits kafka.MessageLimits) (TraceContextPropagation, error) {
	policy, err := kafkaotel.NewTraceContextPropagation(limits)
	if err != nil {
		return TraceContextPropagation{}, err
	}

	return TraceContextPropagation{canonical: policy}, nil
}

// Inject returns an owned record with stale W3C trace fields replaced from ctx.
func (policy TraceContextPropagation) Inject(
	ctx context.Context,
	record kafka.ProducerRecord,
) (kafka.ProducerRecord, error) {
	return policy.canonical.Inject(ctx, record)
}

// Extract reads borrowed headers without retaining them. Invalid traceparent or
// case-insensitive duplicate W3C fields leave ctx unchanged; baggage is excluded.
func (policy TraceContextPropagation) Extract(
	ctx context.Context,
	record kafka.ConsumedRecord,
) (context.Context, error) {
	return policy.canonical.Extract(ctx, record)
}
