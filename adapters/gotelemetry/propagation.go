package gotelemetry

import (
	"context"

	kafka "github.com/faustbrian/go-kafka"
	kafkaotel "github.com/faustbrian/go-kafka/adapters/otel"
)

// TraceContextPropagation copies only W3C Trace Context fields between Kafka records.
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

// Extract returns a context containing a remote W3C trace context from record.
func (policy TraceContextPropagation) Extract(
	ctx context.Context,
	record kafka.ConsumedRecord,
) (context.Context, error) {
	return policy.canonical.Extract(ctx, record)
}
