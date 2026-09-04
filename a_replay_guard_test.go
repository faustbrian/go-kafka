package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestReplayCriticalGuardsTerminateDeterministically(t *testing.T) {
	t.Run("shutdown waits for an active replay", func(t *testing.T) {
		runDone := make(chan struct{})
		prematureClose := make(chan struct{})
		backend := &replayShutdownOrderingBackend{
			runDone:        runDone,
			prematureClose: prematureClose,
		}
		reader := replayReaderWithBackend(backend, nil)
		reader.running = true
		reader.runDone = runDone

		waitObserved := make(chan struct{})
		shutdownContext, cancelShutdown := context.WithCancel(context.Background())
		ctx := &replayShutdownProbeContext{
			Context:      shutdownContext,
			waitObserved: waitObserved,
		}
		shutdownDone := make(chan error, 1)
		go func() {
			shutdownDone <- reader.Shutdown(ctx)
		}()
		runReleased := false
		shutdownJoined := false
		t.Cleanup(func() {
			if !runReleased {
				close(runDone)
			}
			cancelShutdown()
			if !shutdownJoined {
				select {
				case <-shutdownDone:
				case <-time.After(time.Second):
				}
			}
		})

		select {
		case <-waitObserved:
		case err := <-shutdownDone:
			shutdownJoined = true
			t.Fatalf("Shutdown() returned before waiting for active replay: %v", err)
		case <-time.After(time.Second):
			t.Fatal("Shutdown() did not enter its active-replay wait")
		}

		close(runDone)
		runReleased = true
		select {
		case err := <-shutdownDone:
			shutdownJoined = true
			if err != nil {
				t.Fatalf("Shutdown() error = %v", err)
			}
		case <-time.After(time.Second):
			cancelShutdown()
			t.Fatal("Shutdown() did not finish after active replay completed")
		}
		select {
		case <-prematureClose:
			t.Fatal("Shutdown() closed backend before active replay completed")
		default:
		}
		if backend.closed != 1 {
			t.Fatalf("backend close count = %d", backend.closed)
		}
	})

	t.Run("partition fetch bytes retain the one mebibyte minimum", func(t *testing.T) {
		config := validReplayConfig()
		config.FetchMaxBytes = 50 << 20
		config.FetchMaxPartitionBytes = 1<<20 - 1
		if _, err := normalizeReplayConfig(config); !errors.Is(
			err,
			ErrInvalidReplayConfig,
		) {
			t.Fatalf("partition fetch lower-bound error = %v", err)
		}
	})

	t.Run("remaining offsets include every range", func(t *testing.T) {
		result := ReplayResult{Ranges: []ReplayRangeResult{
			{ReplayRange: ReplayRange{EndOffset: 10}, NextOffset: 3},
			{ReplayRange: ReplayRange{EndOffset: 5}, NextOffset: 4},
		}}
		if remaining := replayResultRemaining(result); remaining != 8 {
			t.Fatalf("replay remaining = %d", remaining)
		}
	})

	t.Run("complete bounds are skipped while incomplete bounds are checked", func(t *testing.T) {
		reader := &ReplayReader{
			planningTimeout: time.Second,
			bounds: &recordingReplayBoundsBackend{bounds: map[replayPartition][2]int64{
				{topic: "events", partition: 0}: {0, 1},
			}},
		}
		progress := []ReplayRangeResult{
			{
				ReplayRange: ReplayRange{
					Topic: "complete", Partition: 0, EndOffset: 1,
				},
				NextOffset: 1,
				Complete:   true,
			},
			{
				ReplayRange: ReplayRange{
					Topic: "events", Partition: 0, EndOffset: 1,
				},
			},
		}
		if err := reader.validateReplayBounds(context.Background(), progress); err != nil {
			t.Fatalf("valid mixed replay bounds error = %v", err)
		}

		progress = []ReplayRangeResult{{
			ReplayRange: ReplayRange{
				Topic: "events", Partition: 0, EndOffset: 2,
			},
		}}
		if err := reader.validateReplayBounds(
			context.Background(),
			progress,
		); !errors.Is(err, ErrReplayOffsetOutOfRange) {
			t.Fatalf("out-of-range replay bounds error = %v", err)
		}
	})
}

type replayShutdownProbeContext struct {
	context.Context
	waitObserved chan struct{}
	once         sync.Once
}

type replayShutdownOrderingBackend struct {
	recordingReplayBackend
	runDone        <-chan struct{}
	prematureClose chan struct{}
}

func (backend *replayShutdownOrderingBackend) Close() {
	select {
	case <-backend.runDone:
	default:
		close(backend.prematureClose)
	}

	backend.recordingReplayBackend.Close()
}

func (ctx *replayShutdownProbeContext) Done() <-chan struct{} {
	ctx.once.Do(func() {
		close(ctx.waitObserved)
	})

	return ctx.Context.Done()
}

func TestReplayTimestampCriticalGuardsTerminateDeterministically(t *testing.T) {
	t.Run("partition response limit is cumulative across topics", func(t *testing.T) {
		inspector := &Inspector{maxMetadataPartitions: 2}
		partitions := []TopicPartition{
			{Topic: "accounts", Partition: 0},
			{Topic: "audit", Partition: 0},
			{Topic: "events", Partition: 0},
		}
		offsets := listedTimestampOffsets(
			timestampListedOffset("accounts", 0, 0, -1),
			timestampListedOffset("audit", 0, 0, -1),
			timestampListedOffset("events", 0, 0, -1),
		)
		if err := inspector.validateReplayTimestampOffsets(
			-2,
			partitions,
			offsets,
		); !errors.Is(err, ErrInspectionResponseTooLarge) {
			t.Fatalf("cumulative partition-limit error = %v", err)
		}
	})

	t.Run("offset response identity fields are independently validated", func(t *testing.T) {
		tests := map[string]struct {
			partitions []TopicPartition
			offsets    kadm.ListedOffsets
		}{
			"unexpected partition": {
				partitions: []TopicPartition{{Topic: "events", Partition: 0}},
				offsets: listedTimestampOffsets(
					timestampListedOffset("other", 0, 0, -1),
				),
			},
			"negative partition": {
				partitions: []TopicPartition{{Topic: "events", Partition: -1}},
				offsets: listedTimestampOffsets(
					timestampListedOffset("events", -1, 0, -1),
				),
			},
			"topic identity": {
				partitions: []TopicPartition{{Topic: "events", Partition: 0}},
				offsets: kadm.ListedOffsets{
					"events": {
						0: {
							Topic: "other", Partition: 0,
							Offset: 0, Timestamp: -1,
						},
					},
				},
			},
			"partition identity": {
				partitions: []TopicPartition{{Topic: "events", Partition: 0}},
				offsets: kadm.ListedOffsets{
					"events": {
						0: {
							Topic: "events", Partition: 1,
							Offset: 0, Timestamp: -1,
						},
					},
				},
			},
		}
		inspector := &Inspector{maxMetadataPartitions: 10}
		for name, test := range tests {
			if err := inspector.validateReplayTimestampOffsets(
				-2,
				test.partitions,
				test.offsets,
			); !errors.Is(err, ErrInvalidInspectionResponse) {
				t.Fatalf("%s response error = %v", name, err)
			}
		}
	})

	t.Run("earliest and latest responses require retained offsets", func(t *testing.T) {
		if validListedReplayTimestampOffset(
			-2,
			kadm.ListedOffset{Offset: -1, Timestamp: -1},
		) {
			t.Fatal("missing retained offset was accepted")
		}
		if !validListedReplayTimestampOffset(
			-2,
			kadm.ListedOffset{Offset: 0, Timestamp: -1},
		) {
			t.Fatal("retained offset zero was rejected")
		}
	})

	t.Run("typed nil list-offset responses are rejected", func(t *testing.T) {
		var response *kmsg.ListOffsetsResponse
		offsets, err := parseReplayTimestampShards(
			[]kgo.ResponseShard{{Resp: response}},
			[]TopicPartition{{Topic: "events", Partition: 0}},
		)
		if !errors.Is(err, ErrInvalidInspectionResponse) || offsets != nil {
			t.Fatalf("typed-nil response = %#v/%v", offsets, err)
		}
	})
}
