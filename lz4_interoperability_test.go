package kafka

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"reflect"
	"sync"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func referenceLZ4Plaintext(name string) []byte {
	var overlap []byte
	for _, size := range []int{3, 5, 7, 15, 31} {
		pattern := make([]byte, size)
		for i := range pattern {
			pattern[i] = byte('A' + i%26)
		}
		overlap = append(overlap, bytes.Repeat(pattern, 512)...)
	}
	switch name {
	case "empty":
		return nil
	case "overlap":
		return overlap
	case "multiblock":
		return bytes.Repeat(overlap, 5)
	case "stored":
		var result []byte
		for i := uint32(0); i < 256; i++ {
			var counter [4]byte
			binary.LittleEndian.PutUint32(counter[:], i)
			digest := sha256.Sum256(counter[:])
			result = append(result, digest[:]...)
		}
		return result
	default:
		panic("unknown reference fixture")
	}
}

func referenceLZ4Frame(t testing.TB, name string) []byte {
	t.Helper()
	frame, err := os.ReadFile("testdata/lz4/" + name + ".lz4")
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func TestLZ4IndependentModernFramesPreservePayloadAndBounds(t *testing.T) {
	for _, name := range []string{"empty", "overlap", "multiblock", "stored"} {
		t.Run(name, func(t *testing.T) {
			frame := referenceLZ4Frame(t, name)
			plain := referenceLZ4Plaintext(name)
			for _, maximum := range []int{len(plain), len(plain) + 1} {
				decoded, err := newBoundedDecompressor(maximum).Decompress(frame, kgo.CodecLz4)
				if err != nil || !bytes.Equal(decoded, plain) {
					t.Fatalf("maximum %d: decoded payload differs, error %v", maximum, err)
				}
			}
			if len(plain) > 0 {
				decoded, err := newBoundedDecompressor(len(plain)-1).Decompress(frame, kgo.CodecLz4)
				if decoded != nil || !errors.Is(err, ErrFetchBatchTooLarge) {
					t.Fatalf("oversize decode = (%d bytes, %v)", len(decoded), err)
				}
			}
		})
	}
}

func TestLZ4IndependentFramesRejectCorruptionWithoutReservingBudget(t *testing.T) {
	frame := referenceLZ4Frame(t, "overlap")
	headerChecksum := 6
	if frame[4]&8 != 0 {
		headerChecksum += 8
	}
	variants := map[string][]byte{
		"header checksum":   bytes.Clone(frame),
		"content checksum":  bytes.Clone(frame),
		"truncated trailer": bytes.Clone(frame[:len(frame)-3]),
		"block checksum":    bytes.Clone(frame),
	}
	variants["header checksum"][headerChecksum] ^= 1
	variants["content checksum"][len(frame)-1] ^= 1
	blockStart := headerChecksum + 1
	blockBytes := int(binary.LittleEndian.Uint32(frame[blockStart:]) & 0x7fffffff)
	variants["block checksum"][blockStart+4+blockBytes] ^= 1
	for name, damaged := range variants {
		t.Run(name, func(t *testing.T) {
			decoder, budget := newFetchDecompressionPolicy(1<<20, 1<<20)
			decoded, err := decoder.Decompress(damaged, kgo.CodecLz4)
			if decoded != nil || !errors.Is(err, ErrFetchBatchMalformed) ||
				err.Error() != ErrFetchBatchMalformed.Error() || budget.activeBytes.Load() != 0 {
				t.Fatalf("damaged decode = (%d bytes, %v), retained %d", len(decoded), err, budget.activeBytes.Load())
			}
			decoded, err = decoder.Decompress(frame, kgo.CodecLz4)
			if err != nil || !bytes.Equal(decoded, referenceLZ4Plaintext("overlap")) {
				t.Fatalf("valid decode after corruption = (%d bytes, %v)", len(decoded), err)
			}
			if budget.activeBytes.Load() != int64(cap(decoded)) {
				t.Fatal("successful decode did not reserve its capacity exactly once")
			}
			budget.PutDecompressBytes(decoded)
			if budget.activeBytes.Load() != 0 {
				t.Fatal("recycling retained decoded bytes")
			}
		})
	}
}

func TestLZ4ReaderReuseAfterEarlyLimitAndConcurrentReads(t *testing.T) {
	frame := referenceLZ4Frame(t, "overlap")
	plain := referenceLZ4Plaintext("overlap")
	decoder, budget := newFetchDecompressionPolicy(int64(len(plain)), 1<<20)
	oversized := referenceLZ4Frame(t, "multiblock")
	for range 3 {
		decoded, err := decoder.Decompress(oversized, kgo.CodecLz4)
		if decoded != nil || !errors.Is(err, ErrFetchBatchTooLarge) || budget.activeBytes.Load() != 0 {
			t.Fatalf("early rejection = (%d bytes, %v), retained %d", len(decoded), err, budget.activeBytes.Load())
		}
		decoded, err = decoder.Decompress(frame, kgo.CodecLz4)
		if err != nil || !bytes.Equal(decoded, plain) {
			t.Fatalf("decode after early rejection = (%d bytes, %v)", len(decoded), err)
		}
		budget.PutDecompressBytes(decoded)
	}
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			decoded, err := decoder.Decompress(frame, kgo.CodecLz4)
			if err != nil || !bytes.Equal(decoded, plain) {
				t.Errorf("concurrent payload differs, error %v", err)
				return
			}
			budget.PutDecompressBytes(decoded)
		})
	}
	workers.Wait()
	if budget.activeBytes.Load() != 0 {
		t.Fatal("concurrent decoding retained bytes")
	}
}

func TestLZ4ReferenceRecordBatchPreservesFramingAndRecycling(t *testing.T) {
	frame := referenceLZ4Frame(t, "kafka-records")
	batch := kmsg.RecordBatch{
		FirstOffset: 40, PartitionLeaderEpoch: 1, Magic: 2,
		Attributes: int16(kgo.CodecLz4), LastOffsetDelta: 1,
		ProducerID: -1, ProducerEpoch: -1, FirstSequence: -1,
		NumRecords: 2, Records: frame,
	}
	raw := batch.AppendTo(nil)
	batch.Length = int32(len(raw) - 12)
	raw = batch.AppendTo(nil)
	batch.CRC = int32(crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli)))
	raw = batch.AppendTo(nil)
	decoder, budget := newFetchDecompressionPolicy(52, 1<<20)
	partition, next := kgo.ProcessFetchPartition(
		kgo.ProcessFetchPartitionOpts{
			Offset: 40, Topic: "events", Partition: 3, Pools: []kgo.Pool{budget},
		},
		&kmsg.FetchResponseTopicPartition{Partition: 3, RecordBatches: raw},
		decoder, nil,
	)
	if partition.Err != nil || len(partition.Records) != 2 || next != 42 {
		t.Fatalf("reference batch = (%#v, next %d)", partition, next)
	}
	for i, record := range partition.Records {
		if record.Topic != "events" || record.Partition != 3 || record.Offset != 40+int64(i) ||
			string(record.Key) != "key" || string(record.Value) != []string{"alpha", "bravo"}[i] ||
			!reflect.DeepEqual(record.Headers, []kgo.RecordHeader{{Key: "hdr", Value: []byte("header")}}) {
			t.Fatalf("reference record %d differs: %#v", i, record)
		}
	}
	if budget.activeBytes.Load() == 0 {
		t.Fatal("parsed records did not retain their decoded buffer")
	}
	recycleFetchedRecords(partition.Records)
	if budget.activeBytes.Load() != 0 {
		t.Fatal("recycled parsed records retained decoded bytes")
	}
	decoder, budget = newFetchDecompressionPolicy(51, 1<<20)
	partition, next = kgo.ProcessFetchPartition(
		kgo.ProcessFetchPartitionOpts{
			Offset: 40, Topic: "events", Partition: 3, Pools: []kgo.Pool{budget},
		},
		&kmsg.FetchResponseTopicPartition{Partition: 3, RecordBatches: raw},
		decoder, nil,
	)
	if !errors.Is(partition.Err, ErrFetchBatchTooLarge) || len(partition.Records) != 0 ||
		next != 40 || budget.activeBytes.Load() != 0 {
		t.Fatalf("oversized batch delivered or advanced: (%#v, next %d), retained %d", partition, next, budget.activeBytes.Load())
	}
}
