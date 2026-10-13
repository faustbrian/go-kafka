// Package kafkatest provides public contract suites for Kafka policy
// implementations and compatible broker fixtures.
//
// BrokerHarness supplies caller-owned isolated topic creation, direct reads and
// authoritative offset observations. RunProducerConformance, consumer, replay,
// transaction and inspector suites exercise their selected assertions; they do
// not certify every protocol version, security policy or managed service.
//
// Authentication and observer suites use explicit provider/callback fixtures.
// Factories must isolate subtest state, honor cancellation and supply owned
// results where required. Suites may fail through testing.T and create clients;
// broker/resource cleanup and authorization remain the harness owner's job.
package kafkatest

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/faustbrian/go-kafka"
)

// ErrInvalidHarness reports that a conformance fixture is incomplete or
// cannot preserve the package's bounded Kafka semantics.
var ErrInvalidHarness = errors.New("kafkatest: invalid broker harness")

// ReadIsolation selects which transactional records a broker fixture exposes.
type ReadIsolation uint8

const (
	// ReadUncommitted exposes committed, pending, and aborted records.
	ReadUncommitted ReadIsolation = iota
	// ReadCommitted exposes only records from committed transactions.
	ReadCommitted
)

// ReadRequest describes one bounded direct-partition fixture read. StartOffset
// is inclusive and MaxRecords must be positive.
type ReadRequest struct {
	// Topic identifies the caller-owned fixture log to read.
	Topic string
	// Partition identifies a Kafka partition; it does not prove assignment ownership or
	// broker generation.
	Partition int32
	// StartOffset is inclusive; a broker validation is needed to establish retention at
	// observation time.
	StartOffset int64
	// MaxRecords is a positive fixture read bound; it does not permit an unbounded
	// broker scan.
	MaxRecords int
	// Isolation selects committed-only versus all transactional fixture visibility.
	Isolation ReadIsolation
}

// BrokerHarness supplies only infrastructure operations that the Kafka policy
// package deliberately does not expose: isolated topic creation, direct fixture
// reads, and authoritative committed-offset lookup.
type BrokerHarness struct {
	// Brokers identifies the caller-owned isolated fixture, not production broker
	// support evidence.
	Brokers []string
	// Security selects verified TLS by default and explicitly composed, borrowed
	// credential providers.
	Security kafka.ClientSecurity
	// NewTopic creates an isolated topic with the requested positive partition
	// count and returns its Kafka name.
	NewTopic func(*testing.T, int) string
	// ReadRecords directly reads one bounded partition range without joining a
	// consumer group or mutating offsets. Returned records and bytes must be
	// owned by the caller.
	ReadRecords func(context.Context, ReadRequest) ([]kafka.ConsumedRecord, error)
	// CommittedOffset returns the group's committed next offset for one topic
	// partition. A group with no commit returns -1.
	CommittedOffset func(context.Context, string, string, int32) (int64, error)
}

// Validate checks whether the harness can exercise every broker-backed
// conformance contract.
func (harness BrokerHarness) Validate() error {
	if len(harness.Brokers) == 0 || harness.NewTopic == nil ||
		harness.ReadRecords == nil || harness.CommittedOffset == nil {
		return ErrInvalidHarness
	}
	seen := make(map[string]struct{}, len(harness.Brokers))
	for _, broker := range harness.Brokers {
		if broker == "" || broker != strings.TrimSpace(broker) {
			return ErrInvalidHarness
		}
		if _, exists := seen[broker]; exists {
			return ErrInvalidHarness
		}
		seen[broker] = struct{}{}
	}
	if err := harness.Security.Validate(); err != nil {
		return errors.Join(ErrInvalidHarness, err)
	}

	return nil
}
