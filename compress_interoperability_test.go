package kafka

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"reflect"
	"sync"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Cross-version frames cannot be hidden by a matching candidate encoder bug.
func TestCompressBaselineFramesPreserveLimitsAndReuse(t *testing.T) {
	for _, tc := range []struct {
		name  string
		codec kgo.CompressionCodecType
	}{
		{"snappy", kgo.CodecSnappy}, {"zstd", kgo.CodecZstd},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frame, err := os.ReadFile("testdata/compress/baseline-" + tc.name + ".bin")
			if err != nil {
				t.Fatal(err)
			}
			want := bytes.Repeat([]byte("Kafka-compatible record payload\n"), 4096)
			decoder := newBoundedDecompressor(len(want))
			for range 4 {
				got, err := decoder.Decompress(frame, tc.codec)
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("baseline frame = (%d bytes, %v), want exact payload", len(got), err)
				}
				got, err = decoder.Decompress([]byte("secret-malformed-frame"), tc.codec)
				if got != nil || !errors.Is(err, ErrFetchBatchMalformed) {
					t.Fatalf("malformed = (%d bytes, %v)", len(got), err)
				}
				if bytes.Contains([]byte(err.Error()), []byte("secret-malformed-frame")) {
					t.Fatal("malformed diagnostic leaks input")
				}
			}
			got, err := newBoundedDecompressor(len(want)-1).Decompress(frame, tc.codec)
			if got != nil || !errors.Is(err, ErrFetchBatchTooLarge) {
				t.Fatalf("over limit = (%d bytes, %v)", len(got), err)
			}
			var wg sync.WaitGroup
			failures := make(chan error, 8)
			for range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range 8 {
						got, err := decoder.Decompress(frame, tc.codec)
						if err != nil || !bytes.Equal(got, want) {
							failures <- fmt.Errorf("concurrent baseline decode: %d bytes, %v", len(got), err)
							return
						}
					}
				}()
			}
			wg.Wait()
			close(failures)
			for err := range failures {
				t.Error(err)
			}
		})
	}
}

func TestCompressKafkaRecordFramingAndRecycling(t *testing.T) {
	for _, tc := range []struct {
		name  string
		codec kgo.CompressionCodec
	}{
		{"snappy", kgo.SnappyCompression()}, {"zstd", kgo.ZstdCompression()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Literal Kafka zigzag-length records retain an independent wire oracle.
			plaintext := []byte{
				0x32, 0x00, 0x00, 0x00, 0x06, 0x6b, 0x65, 0x79,
				0x0a, 0x61, 0x6c, 0x70, 0x68, 0x61, 0x02, 0x06,
				0x68, 0x64, 0x72, 0x0c, 0x68, 0x65, 0x61, 0x64,
				0x65, 0x72, 0x32, 0x00, 0x00, 0x02, 0x06, 0x6b,
				0x65, 0x79, 0x0a, 0x62, 0x72, 0x61, 0x76, 0x6f,
				0x02, 0x06, 0x68, 0x64, 0x72, 0x0c, 0x68, 0x65,
				0x61, 0x64, 0x65, 0x72,
			}
			compressor, err := kgo.DefaultCompressor(tc.codec)
			if err != nil {
				t.Fatal(err)
			}
			var destination bytes.Buffer
			frame, codec := compressor.Compress(&destination, plaintext)
			if codec == kgo.CodecError {
				t.Fatal("producer codec failed")
			}
			batch := kmsg.RecordBatch{FirstOffset: 40, PartitionLeaderEpoch: 1, Magic: 2, Attributes: int16(codec), LastOffsetDelta: 1, ProducerID: -1, ProducerEpoch: -1, FirstSequence: -1, NumRecords: 2, Records: frame}
			raw := batch.AppendTo(nil)
			batch.Length = int32(len(raw) - 12)
			raw = batch.AppendTo(nil)
			batch.CRC = int32(crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli)))
			raw = batch.AppendTo(nil)
			// Kafka Zstd frames reserve a codec window as well as decoded bytes.
			// Inclusive payload limits are tested separately with fixed baseline frames.
			for _, limit := range []int{1 << 20, len(plaintext) - 1} {
				decoder, budget := newFetchDecompressionPolicy(int64(limit), 1<<20)
				partition, next := kgo.ProcessFetchPartition(kgo.ProcessFetchPartitionOpts{Offset: 40, Topic: "events", Partition: 3, Pools: []kgo.Pool{budget}}, &kmsg.FetchResponseTopicPartition{Partition: 3, RecordBatches: raw}, decoder, nil)
				if limit < len(plaintext) {
					if !errors.Is(partition.Err, ErrFetchBatchTooLarge) || len(partition.Records) != 0 || next != 40 || budget.activeBytes.Load() != 0 {
						t.Fatalf("rejected batch delivered or advanced: %#v, next %d, budget %d", partition, next, budget.activeBytes.Load())
					}
					continue
				}
				if partition.Err != nil || len(partition.Records) != 2 || next != 42 {
					t.Fatalf("valid batch: %#v, next %d", partition, next)
				}
				for i, record := range partition.Records {
					if record.Topic != "events" || record.Partition != 3 || record.Offset != 40+int64(i) || string(record.Key) != "key" || string(record.Value) != []string{"alpha", "bravo"}[i] || !reflect.DeepEqual(record.Headers, []kgo.RecordHeader{{Key: "hdr", Value: []byte("header")}}) {
						t.Fatalf("record %d differs: %#v", i, record)
					}
				}
				if budget.activeBytes.Load() == 0 {
					t.Fatal("record buffers not accounted")
				}
				recycleFetchedRecords(partition.Records)
				if budget.activeBytes.Load() != 0 {
					t.Fatal("record buffers not released")
				}
			}
		})
	}
}
