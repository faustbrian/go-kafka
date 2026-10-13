package kafka

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	// ErrInvalidTransactionProcessorConfig reports that transaction processor
	// configuration is invalid.
	ErrInvalidTransactionProcessorConfig = errors.New(
		"kafka: transaction processor configuration is invalid",
	)
	// ErrTransactionHandlerRequired reports that transaction processor handler is
	// required.
	ErrTransactionHandlerRequired = errors.New(
		"kafka: transaction processor handler is required",
	)
	// ErrTransactionNotCommitted reports that consume-transform-produce
	// transaction was not committed.
	ErrTransactionNotCommitted = errors.New(
		"kafka: consume-transform-produce transaction was not committed",
	)
	// ErrTooManyTransactionOutputRecords reports that transaction output record
	// count exceeds configured limit.
	ErrTooManyTransactionOutputRecords = errors.New(
		"kafka: transaction output record count exceeds configured limit",
	)
	// ErrTransactionOutputTooLarge reports that transaction output bytes exceed
	// configured limit.
	ErrTransactionOutputTooLarge = errors.New(
		"kafka: transaction output bytes exceed configured limit",
	)
	// ErrTransactionProcessorBusy reports that transaction processor runner is
	// already active.
	ErrTransactionProcessorBusy = errors.New(
		"kafka: transaction processor runner is already active",
	)
	// ErrTransactionProcessorClosing reports that transaction processor is
	// shutting down.
	ErrTransactionProcessorClosing = errors.New(
		"kafka: transaction processor is shutting down",
	)
	// ErrTransactionProcessorClosed reports that transaction processor is closed.
	ErrTransactionProcessorClosed = errors.New(
		"kafka: transaction processor is closed",
	)
	// ErrTransactionProcessorShutdownActive reports that transaction processor
	// shutdown is already active.
	ErrTransactionProcessorShutdownActive = errors.New(
		"kafka: transaction processor shutdown is already active",
	)
	// ErrTransactionProcessorShutdownIncomplete reports that transaction
	// processor shutdown is incomplete.
	ErrTransactionProcessorShutdownIncomplete = errors.New(
		"kafka: transaction processor shutdown is incomplete",
	)
	// ErrTransactionProcessorFatal reports that transaction processor entered a
	// fatal state.
	ErrTransactionProcessorFatal = errors.New(
		"kafka: transaction processor entered a fatal state",
	)
)

// TransactionConnectionConfig defines shared broker, identity, protocol, and
// security policy for one consume-transform-produce client.
type TransactionConnectionConfig struct {
	// Brokers supplies 1 through 32 distinct, bounded seed strings; validation does not
	// resolve addresses or enforce a network allowlist.
	Brokers []string
	// ClientID is a required bounded UTF-8 client identity, not a credential or safe
	// arbitrary telemetry label.
	ClientID string
	// Protocol selects the pinned client negotiation floor; it is not a maximum version
	// or broker-support claim.
	Protocol ProtocolPolicy
	// DialTimeout bounds cooperative connection attempts; zero selects ten seconds.
	DialTimeout time.Duration
	// Security selects verified TLS by default and explicitly composed, borrowed
	// credential providers.
	Security ClientSecurity
}

// TransactionGroupConfig defines the bounded read-committed consumer-group
// side of one consume-transform-produce client.
type TransactionGroupConfig struct {
	// GroupID is the required bounded group identity; external ACL policy remains
	// caller-owned.
	GroupID string
	// InstanceID is an optional static member identity; it must be unique among live
	// members of the group.
	InstanceID string
	// Rack is optional bounded rack metadata; preferred-replica behavior depends on
	// broker topology.
	Rack string
	// Topics is the required set of at most 64 distinct valid subscription names.
	Topics []string
	// ResetOffset explicitly selects earliest or latest when a usable group offset is
	// absent; zero is invalid.
	ResetOffset OffsetPolicy
	// BalancePolicy defaults to cooperative sticky; eager migration requires the
	// documented two-rollout policy.
	BalancePolicy GroupBalancePolicy
	// MaxPollRecords bounds a fetched processing unit, defaulting to 100 and capped at
	// 1000.
	MaxPollRecords int
	// MaxConcurrentFetches bounds broker fetch concurrency, defaulting to four and
	// capped at 64.
	MaxConcurrentFetches int
	// FetchMinBytes is the minimum encoded record bytes a broker tries to
	// collect before answering a fetch. Zero defaults to one byte, values must
	// not exceed FetchMaxBytes, and FetchMaxWait bounds the wait.
	FetchMinBytes int32
	// FetchMaxBytes selects the encoded fetch response allowance, defaulting to 50 MiB;
	// separate hard read/decompression guards apply.
	FetchMaxBytes int32
	// FetchMaxPartitionBytes selects an encoded per-partition fetch allowance,
	// defaulting to 1 MiB and no greater than FetchMaxBytes.
	FetchMaxPartitionBytes int32
	// BrokerMaxReadBytes is the hard maximum encoded Kafka response accepted
	// from one broker connection. It must be at least FetchMaxBytes.
	BrokerMaxReadBytes int32
	// MaxDecompressedBatchBytes is the hard decoded-byte limit for one Kafka
	// source record batch.
	MaxDecompressedBatchBytes int64
	// MaxBufferedDecompressedBytes bounds decoded compressed-batch memory held
	// across active and prefetched source responses.
	// Capacity is charged after decoding; transient allocations and codec-none
	// buffers are not part of this retained-buffer budget.
	MaxBufferedDecompressedBytes int64
	// FetchMaxWait bounds broker collection wait; zero selects 500 milliseconds.
	FetchMaxWait time.Duration
	// SessionTimeout selects the group failure-detection window, defaulting to 45
	// seconds.
	SessionTimeout time.Duration
	// RebalanceTimeout bounds cooperative poll settlement; heartbeat, handler/processing
	// and commit budgets must fit its configured policy.
	RebalanceTimeout time.Duration
	// HeartbeatInterval defaults to three seconds and must be below SessionTimeout.
	HeartbeatInterval time.Duration
	// ProcessingTimeout is one cooperative budget for the complete poll, not an
	// independent allowance per record.
	ProcessingTimeout time.Duration
}

// TransactionOutputConfig defines bounded transactional production policy.
// TransactionalID must be unique to one live processor instance.
type TransactionOutputConfig struct {
	// AllowedTopics is a required, copied set of at most 64 distinct valid topic names
	// checked before production.
	AllowedTopics []string
	// KeyPolicy requires a nonempty key by default; explicit partition selection does
	// not waive that policy.
	KeyPolicy KeyPolicy
	// MaxBufferedRecords bounds queued production, defaulting to 1000; it is distinct
	// from transaction-total output admission.
	MaxBufferedRecords int
	// MaxBufferedBytes bounds the client produce buffer, defaulting to 64 MiB and at
	// least MaxBatchBytes.
	MaxBufferedBytes int
	// MaxBatchBytes bounds a Kafka producer batch, defaulting to 1 MiB and large enough
	// for the permitted record policy.
	MaxBatchBytes int32
	// MaxOutputRecords bounds admitted outputs, including failed deliveries,
	// independently of current buffers; zero selects 1000, maximum 100000.
	MaxOutputRecords int
	// MaxOutputBytes bounds conservative admitted-output bytes, defaults to 10 MiB
	// and is capped at 1 GiB; ignored publish failures still abort the poll.
	MaxOutputBytes int64
	// RecordRetries bounds client retry attempts; zero selects ten rather than disabling
	// retries.
	RecordRetries int
	// RetryBackoffMin is the positive initial retry bound; zero selects 250
	// milliseconds.
	RetryBackoffMin time.Duration
	// RetryBackoffMax caps jittered retry growth; zero selects one second and must not
	// precede RetryBackoffMin.
	RetryBackoffMax time.Duration
	// DeliveryTimeout defaults to 30 seconds; package delivery contexts add
	// RetryBackoffMax and do not use record event time.
	DeliveryTimeout time.Duration
	// RequestTimeout bounds cooperative broker requests; zero selects ten seconds.
	RequestTimeout time.Duration
	// Linger batches production briefly; zero selects five milliseconds rather than
	// immediate dispatch.
	Linger time.Duration
	// CompressionPreferences is an ordered, copied, distinct codec list; zero selects
	// Snappy then none, and none may appear only last.
	CompressionPreferences []CompressionCodec
	// TransactionalID is required and must be unique to one live processor instance.
	TransactionalID string
	// TransactionTimeout selects the broker transaction lifetime, defaulting to 30
	// seconds when transactions are enabled.
	TransactionTimeout time.Duration
	// TransactionEndTimeout bounds detached commit/abort contexts, defaulting to 30
	// seconds; it cannot force borrowed code to stop.
	TransactionEndTimeout time.Duration
}

// TransactionProcessorConfig composes shared connection, source group, output,
// record-limit, and lifecycle policy for Kafka-only consume-transform-produce.
type TransactionProcessorConfig struct {
	// Connection supplies the shared identity, transport and provider policy.
	Connection TransactionConnectionConfig
	// Group defines read-committed source membership and the all-or-nothing poll
	// boundary.
	Group TransactionGroupConfig
	// Output supplies bounded Kafka-only transaction production and total-output
	// admission policy.
	Output TransactionOutputConfig
	// Limits bounds admitted record fields; its zero value selects DefaultMessageLimits,
	// not an aggregate process budget.
	Limits MessageLimits
	// Observers supplies copied ordered synchronous callbacks; timeout and reentry
	// controls are cooperative, not detached background execution.
	Observers ObserverPolicy
	// ShutdownTimeout bounds cooperative shutdown waits, not the execution of arbitrary
	// borrowed callbacks or backend Close.
	ShutdownTimeout time.Duration
}

// Validate reports whether the complete processor policy is bounded and
// internally consistent without constructing a client or dialing brokers.
func (config TransactionProcessorConfig) Validate() error {
	_, err := normalizeTransactionProcessorConfig(config)

	return err
}

// TransactionHandler processes one borrowed source record and may publish
// records through the transaction capability. Returning an error aborts the
// complete poll and leaves every source offset unsettled. Handlers must cooperate
// with cancellation before effects: the processor checks cancellation after the
// call returns, but cannot preempt application work or roll back external effects.
type TransactionHandler interface {
	// Handle receives a borrowed record and callback-scoped Transaction; honor ctx
	// before effects and return only after owned publication work stops.
	Handle(context.Context, ConsumedRecord, Transaction) error
}

// TransactionHandlerFunc adapts a function to TransactionHandler.
type TransactionHandlerFunc func(context.Context, ConsumedRecord, Transaction) error

// Handle invokes handler.
func (handler TransactionHandlerFunc) Handle(
	ctx context.Context,
	record ConsumedRecord,
	transaction Transaction,
) error {
	return handler(ctx, record, transaction)
}

// TransactionPollResult summarizes one all-or-nothing source poll.
// Published counts broker-acknowledged output records inside the transaction;
// those records are visible to read-committed consumers only when Committed is
// true.
type TransactionPollResult struct {
	// Polled counts records returned to this bounded processing operation.
	Polled int
	// Processed counts successful handler completions, not exactly-once external
	// effects.
	Processed int
	// Published counts broker-acknowledged output inside the transaction, not
	// read-committed visibility.
	Published int
	// Committed reports confirmed atomic Kafka offsets/output settlement; false alone
	// does not resolve an unknown commit receipt.
	Committed bool
}

// TransactionProcessorDiagnostic is a payload-free local snapshot of one
// consume-transform-produce processor. It does not probe Kafka, describe the
// transaction coordinator, or prove a transaction outcome. Lifecycle fields
// are captured together under the processor lifecycle lock; buffered counts
// are a separate concurrent sample from franz-go and may change immediately.
type TransactionProcessorDiagnostic struct {
	// Accepting reports whether a new Run or RunOnce call can currently start.
	Accepting bool
	// Running reports package-local ownership of one Run or RunOnce call.
	Running bool
	// TransactionActive reports a locally begun transaction attempt that has
	// not reached the package's commit, abort, or client-termination boundary.
	TransactionActive bool
	// Closing reports that shutdown has fenced new runs but has not completed.
	Closing bool
	// ShutdownActive reports that one Shutdown call currently owns lifecycle
	// completion.
	ShutdownActive bool
	// Closed reports that graceful shutdown completed.
	Closed bool
	// Fatal reports that the processor retained an unrecoverable lifecycle error.
	Fatal bool
	// ClientTerminated reports that the underlying client was forcefully closed
	// to bound an unsafe or ambiguous transactional output.
	ClientTerminated bool
	// FatalCategory is the redacted stable category of the retained fatal error,
	// or ErrorUnknown when Fatal is false.
	FatalCategory ErrorCategory
	// BufferedRecords is franz-go's current transactional-output record count.
	BufferedRecords int64
	// BufferedBytes is franz-go's current sum of buffered output key, value, and
	// header bytes, excluding Kafka framing.
	BufferedBytes int64
}

type transactionProcessorBackend interface {
	PollRecords(context.Context, int) kgo.Fetches
	Begin() error
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
	BufferedProduceRecords() int64
	BufferedProduceBytes() int64
	End(context.Context, kgo.TransactionEndTry) (bool, error)
	LeaveGroupContext(context.Context) error
	Close()
}

type transactionProcessorFactory func(
	...kgo.Opt,
) (transactionProcessorBackend, error)

type franzTransactionProcessorBackend struct {
	session *kgo.GroupTransactSession
}

func newFranzTransactionProcessorBackend(
	options ...kgo.Opt,
) (transactionProcessorBackend, error) {
	session, err := kgo.NewGroupTransactSession(options...)
	if err != nil {
		return nil, err
	}

	return &franzTransactionProcessorBackend{session: session}, nil
}

func (backend *franzTransactionProcessorBackend) PollRecords(
	ctx context.Context,
	maxRecords int,
) kgo.Fetches {
	return backend.session.PollRecords(ctx, maxRecords)
}

func (backend *franzTransactionProcessorBackend) Begin() error {
	return backend.session.Begin()
}

func (backend *franzTransactionProcessorBackend) ProduceSync(
	ctx context.Context,
	records ...*kgo.Record,
) kgo.ProduceResults {
	return backend.session.ProduceSync(ctx, records...)
}

func (backend *franzTransactionProcessorBackend) BufferedProduceRecords() int64 {
	return backend.session.Client().BufferedProduceRecords()
}

func (backend *franzTransactionProcessorBackend) BufferedProduceBytes() int64 {
	return backend.session.Client().BufferedProduceBytes()
}

func (backend *franzTransactionProcessorBackend) End(
	ctx context.Context,
	try kgo.TransactionEndTry,
) (bool, error) {
	return backend.session.End(ctx, try)
}

func (backend *franzTransactionProcessorBackend) LeaveGroupContext(
	ctx context.Context,
) error {
	return backend.session.Client().LeaveGroupContext(ctx)
}

func (backend *franzTransactionProcessorBackend) Close() {
	backend.session.Close()
}

// TransactionProcessor owns one read-committed group member and transactional
// producer. One Run or RunOnce call may be active at a time. An ambiguous
// transactional output deadline closes and permanently fences the processor.
type TransactionProcessor struct {
	client              transactionProcessorBackend
	clientID            string
	groupID             string
	limits              MessageLimits
	maxPollRecords      int
	processingTimeout   time.Duration
	transactionEndTime  time.Duration
	shutdownTimeout     time.Duration
	deliveryWaitTimeout time.Duration
	keyRequired         bool
	allowedTopics       map[string]struct{}
	maxOutputRecords    int
	maxOutputBytes      int64
	staticMembership    bool

	lifecycleMu       sync.Mutex
	running           bool
	transactionActive bool
	runDone           chan struct{}
	closing           bool
	closed            bool
	shutdownActive    bool
	fatalErr          error
	fatalCategory     ErrorCategory
	clientTerminated  bool
	observerCallbacks int
	observers         observerDispatcher
	cancelClient      context.CancelFunc
	closeOnce         sync.Once
}

// NewTransactionProcessor constructs a Kafka-only consume-transform-produce
// runner. The franz-go client establishes broker connections lazily.
func NewTransactionProcessor(
	config TransactionProcessorConfig,
) (*TransactionProcessor, error) {
	return newTransactionProcessor(config, newFranzTransactionProcessorBackend)
}

func newTransactionProcessor(
	config TransactionProcessorConfig,
	factory transactionProcessorFactory,
) (*TransactionProcessor, error) {
	config, err := normalizeTransactionProcessorConfig(config)
	if err != nil {
		return nil, err
	}

	resetOffset := kgo.NewOffset().AtStart()
	if config.Group.ResetOffset == OffsetLatest {
		resetOffset = kgo.NewOffset().AtEnd()
	}
	clientCtx, cancelClient := context.WithCancel(context.Background())
	decompressor, decompressionBudget := newFetchDecompressionPolicy(
		config.Group.MaxDecompressedBatchBytes,
		config.Group.MaxBufferedDecompressedBytes,
	)
	options := []kgo.Opt{
		kgo.WithContext(clientCtx),
		kgo.SeedBrokers(config.Connection.Brokers...),
		kgo.AlwaysRetryEOF(),
		kgo.ClientID(config.Connection.ClientID),
		kgo.ConsumerGroup(config.Group.GroupID),
		kgo.ConsumeTopics(config.Group.Topics...),
		kgo.ConsumeResetOffset(resetOffset),
		kgo.DisableAutoCommit(),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.Balancers(consumerGroupBalancers(config.Group.BalancePolicy)...),
		kgo.MaxConcurrentFetches(config.Group.MaxConcurrentFetches),
		kgo.FetchMinBytes(config.Group.FetchMinBytes),
		kgo.FetchMaxBytes(config.Group.FetchMaxBytes),
		kgo.FetchMaxPartitionBytes(config.Group.FetchMaxPartitionBytes),
		kgo.BrokerMaxReadBytes(config.Group.BrokerMaxReadBytes),
		kgo.WithDecompressor(decompressor),
		kgo.WithPools(decompressionBudget),
		kgo.FetchMaxWait(config.Group.FetchMaxWait),
		kgo.SessionTimeout(config.Group.SessionTimeout),
		kgo.RebalanceTimeout(config.Group.RebalanceTimeout),
		kgo.HeartbeatInterval(config.Group.HeartbeatInterval),
		kgo.RecordPartitioner(newPolicyPartitioner(
			kgo.UniformBytesPartitioner(defaultPartitionerBatchBytes, true, true, nil),
		)),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.StopProducerOnDataLossDetected(),
		kgo.MaxBufferedRecords(config.Output.MaxBufferedRecords),
		kgo.MaxBufferedBytes(config.Output.MaxBufferedBytes),
		kgo.ProducerBatchMaxBytes(config.Output.MaxBatchBytes),
		kgo.RecordRetries(config.Output.RecordRetries),
		kgo.RetryBackoffFn(newProducerRetryBackoff(
			config.Connection.ClientID,
			config.Output.RetryBackoffMin,
			config.Output.RetryBackoffMax,
		)),
		kgo.MetadataMinAge(producerMetadataMinAge(
			config.Output.RetryBackoffMin,
		)),
		// franz-go measures RecordDeliveryTimeout from Record.Timestamp, which is
		// Kafka event time rather than package admission time. Transactional
		// delivery is bounded by the processor-owned wait and client interruption.
		kgo.ProduceRequestTimeout(config.Output.RequestTimeout),
		kgo.ProducerLinger(config.Output.Linger),
		kgo.ProducerBatchCompression(
			franzCompressionCodecs(config.Output.CompressionPreferences)...,
		),
		kgo.TransactionalID(config.Output.TransactionalID),
		kgo.TransactionTimeout(config.Output.TransactionTimeout),
		kgo.DialTimeout(config.Connection.DialTimeout),
	}
	if config.Group.InstanceID != "" {
		options = append(options, kgo.InstanceID(config.Group.InstanceID))
	}
	if config.Group.Rack != "" {
		options = append(options, kgo.Rack(config.Group.Rack))
	}
	options = append(options, clientProtocolOptions(config.Connection.Protocol)...)
	options = append(options, clientSecurityOptions(
		config.Connection.Security,
		config.Connection.DialTimeout,
	)...)

	dispatcher := newObserverDispatcher(config.Observers)
	allowedTopics := make(map[string]struct{}, len(config.Output.AllowedTopics))
	for _, topic := range config.Output.AllowedTopics {
		allowedTopics[topic] = struct{}{}
	}
	processor := &TransactionProcessor{
		clientID:           strings.Clone(config.Connection.ClientID),
		groupID:            strings.Clone(config.Group.GroupID),
		limits:             config.Limits,
		maxPollRecords:     config.Group.MaxPollRecords,
		processingTimeout:  config.Group.ProcessingTimeout,
		transactionEndTime: config.Output.TransactionEndTimeout,
		shutdownTimeout:    config.ShutdownTimeout,
		deliveryWaitTimeout: config.Output.DeliveryTimeout +
			config.Output.RetryBackoffMax,
		keyRequired:      config.Output.KeyPolicy == KeyRequired,
		allowedTopics:    allowedTopics,
		maxOutputRecords: config.Output.MaxOutputRecords,
		maxOutputBytes:   config.Output.MaxOutputBytes,
		staticMembership: config.Group.InstanceID != "",
		observers:        dispatcher,
		cancelClient:     cancelClient,
	}
	if dispatcher.enabled() {
		observerHook := newFranzObserverHook(
			config.Connection.ClientID,
			config.Group.GroupID,
			config.Connection.Security.Authentication.Method(),
			dispatcher,
		)
		observerHook.before = processor.beginObservation
		observerHook.after = processor.finishObservation
		options = append(options, kgo.WithHooks(observerHook))
	}

	client, err := factory(options...)
	if err != nil {
		cancelClient()

		return nil, err
	}
	processor.client = client

	return processor, nil
}

func normalizeTransactionProcessorConfig(
	config TransactionProcessorConfig,
) (TransactionProcessorConfig, error) {
	observers, err := normalizeObserverPolicy(config.Observers)
	if err != nil {
		return TransactionProcessorConfig{}, errors.Join(
			ErrInvalidTransactionProcessorConfig,
			err,
		)
	}
	config.Observers = observers
	if config.Connection.Protocol.MinimumVersion == "" {
		config.Connection.Protocol.MinimumVersion = "2.5"
	}
	if config.Output.TransactionTimeout == 0 {
		config.Output.TransactionTimeout = 60 * time.Second
	}
	if config.Output.TransactionEndTimeout == 0 {
		config.Output.TransactionEndTimeout = 10 * time.Second
	}

	producer, err := normalizeProducerConfig(ProducerConfig{
		Brokers:                config.Connection.Brokers,
		ClientID:               config.Connection.ClientID,
		Protocol:               config.Connection.Protocol,
		AllowedTopics:          config.Output.AllowedTopics,
		KeyPolicy:              config.Output.KeyPolicy,
		Limits:                 config.Limits,
		MaxBufferedRecords:     config.Output.MaxBufferedRecords,
		MaxBufferedBytes:       config.Output.MaxBufferedBytes,
		MaxBatchBytes:          config.Output.MaxBatchBytes,
		RecordRetries:          config.Output.RecordRetries,
		RetryBackoffMin:        config.Output.RetryBackoffMin,
		RetryBackoffMax:        config.Output.RetryBackoffMax,
		DeliveryTimeout:        config.Output.DeliveryTimeout,
		RequestTimeout:         config.Output.RequestTimeout,
		DialTimeout:            config.Connection.DialTimeout,
		Linger:                 config.Output.Linger,
		CompressionPreferences: config.Output.CompressionPreferences,
		TransactionalID:        config.Output.TransactionalID,
		TransactionTimeout:     config.Output.TransactionTimeout,
		TransactionEndTimeout:  config.Output.TransactionEndTimeout,
		Security:               config.Connection.Security,
	})
	if err != nil {
		return TransactionProcessorConfig{}, errors.Join(
			ErrInvalidTransactionProcessorConfig,
			err,
		)
	}
	if !kafkaReleaseAtLeast(producer.Protocol.MinimumVersion, 2, 5) {
		return TransactionProcessorConfig{}, ErrInvalidTransactionProcessorConfig
	}
	maxOutputRecords := config.Output.MaxOutputRecords
	if maxOutputRecords == 0 {
		maxOutputRecords = 1_000
	}
	maxOutputBytes := config.Output.MaxOutputBytes
	if maxOutputBytes == 0 {
		maxOutputBytes = 10 << 20
	}
	if maxOutputRecords < 1 ||
		maxOutputRecords > 100_000 ||
		maxOutputBytes < maximumRecordPolicyBytes(producer.Limits) ||
		maxOutputBytes > 1<<30 {
		return TransactionProcessorConfig{}, ErrInvalidTransactionProcessorConfig
	}
	consumer, err := normalizeConsumerConfig(ConsumerConfig{
		Brokers:                      config.Connection.Brokers,
		ClientID:                     config.Connection.ClientID,
		Protocol:                     config.Connection.Protocol,
		GroupID:                      config.Group.GroupID,
		InstanceID:                   config.Group.InstanceID,
		Rack:                         config.Group.Rack,
		Topics:                       config.Group.Topics,
		ResetOffset:                  config.Group.ResetOffset,
		BalancePolicy:                config.Group.BalancePolicy,
		Limits:                       config.Limits,
		MaxPollRecords:               config.Group.MaxPollRecords,
		MaxConcurrentFetches:         config.Group.MaxConcurrentFetches,
		FetchMinBytes:                config.Group.FetchMinBytes,
		FetchMaxBytes:                config.Group.FetchMaxBytes,
		FetchMaxPartitionBytes:       config.Group.FetchMaxPartitionBytes,
		BrokerMaxReadBytes:           config.Group.BrokerMaxReadBytes,
		MaxDecompressedBatchBytes:    config.Group.MaxDecompressedBatchBytes,
		MaxBufferedDecompressedBytes: config.Group.MaxBufferedDecompressedBytes,
		FetchMaxWait:                 config.Group.FetchMaxWait,
		SessionTimeout:               config.Group.SessionTimeout,
		RebalanceTimeout:             config.Group.RebalanceTimeout,
		HeartbeatInterval:            config.Group.HeartbeatInterval,
		HandlerTimeout:               config.Group.ProcessingTimeout,
		CommitTimeout:                producer.TransactionEndTimeout,
		ShutdownTimeout:              config.ShutdownTimeout,
		DialTimeout:                  config.Connection.DialTimeout,
		Security:                     config.Connection.Security,
	})
	if err != nil {
		return TransactionProcessorConfig{}, errors.Join(
			ErrInvalidTransactionProcessorConfig,
			err,
		)
	}
	if consumer.HandlerTimeout+producer.TransactionEndTimeout >=
		producer.TransactionTimeout {
		return TransactionProcessorConfig{}, ErrInvalidTransactionProcessorConfig
	}
	sourceTopics := make(map[string]struct{}, len(consumer.Topics))
	for _, topic := range consumer.Topics {
		sourceTopics[topic] = struct{}{}
	}
	for _, topic := range producer.AllowedTopics {
		if _, source := sourceTopics[topic]; source {
			return TransactionProcessorConfig{}, ErrInvalidTransactionProcessorConfig
		}
	}

	config.Connection = TransactionConnectionConfig{
		Brokers:     append([]string(nil), consumer.Brokers...),
		ClientID:    consumer.ClientID,
		Protocol:    consumer.Protocol,
		DialTimeout: consumer.DialTimeout,
		Security:    consumer.Security,
	}
	config.Group = TransactionGroupConfig{
		GroupID:                      consumer.GroupID,
		InstanceID:                   consumer.InstanceID,
		Rack:                         consumer.Rack,
		Topics:                       append([]string(nil), consumer.Topics...),
		ResetOffset:                  consumer.ResetOffset,
		BalancePolicy:                consumer.BalancePolicy,
		MaxPollRecords:               consumer.MaxPollRecords,
		MaxConcurrentFetches:         consumer.MaxConcurrentFetches,
		FetchMinBytes:                consumer.FetchMinBytes,
		FetchMaxBytes:                consumer.FetchMaxBytes,
		FetchMaxPartitionBytes:       consumer.FetchMaxPartitionBytes,
		BrokerMaxReadBytes:           consumer.BrokerMaxReadBytes,
		MaxDecompressedBatchBytes:    consumer.MaxDecompressedBatchBytes,
		MaxBufferedDecompressedBytes: consumer.MaxBufferedDecompressedBytes,
		FetchMaxWait:                 consumer.FetchMaxWait,
		SessionTimeout:               consumer.SessionTimeout,
		RebalanceTimeout:             consumer.RebalanceTimeout,
		HeartbeatInterval:            consumer.HeartbeatInterval,
		ProcessingTimeout:            consumer.HandlerTimeout,
	}
	config.Output = TransactionOutputConfig{
		AllowedTopics:      append([]string(nil), producer.AllowedTopics...),
		KeyPolicy:          producer.KeyPolicy,
		MaxBufferedRecords: producer.MaxBufferedRecords,
		MaxBufferedBytes:   producer.MaxBufferedBytes,
		MaxBatchBytes:      producer.MaxBatchBytes,
		MaxOutputRecords:   maxOutputRecords,
		MaxOutputBytes:     maxOutputBytes,
		RecordRetries:      producer.RecordRetries,
		RetryBackoffMin:    producer.RetryBackoffMin,
		RetryBackoffMax:    producer.RetryBackoffMax,
		DeliveryTimeout:    producer.DeliveryTimeout,
		RequestTimeout:     producer.RequestTimeout,
		Linger:             producer.Linger,
		CompressionPreferences: append(
			[]CompressionCodec(nil),
			producer.CompressionPreferences...,
		),
		TransactionalID:       producer.TransactionalID,
		TransactionTimeout:    producer.TransactionTimeout,
		TransactionEndTimeout: producer.TransactionEndTimeout,
	}
	config.Limits = producer.Limits
	config.Observers = observers
	config.ShutdownTimeout = consumer.ShutdownTimeout

	return config, nil
}

// RunOnce polls at most the configured source-record limit. Every fetched
// record must complete successfully before output records and all source
// offsets are committed in one Kafka transaction. Source poll and group-join
// failures return a ConsumerError with ConsumerOperationPoll.
func (processor *TransactionProcessor) RunOnce(
	ctx context.Context,
	handler TransactionHandler,
) (TransactionPollResult, error) {
	if ctx == nil {
		return TransactionPollResult{}, ErrContextRequired
	}
	if isObserverContext(ctx) {
		return TransactionPollResult{}, ErrObserverReentry
	}
	if handler == nil {
		return TransactionPollResult{}, ErrTransactionHandlerRequired
	}
	if err := processor.beginRun(); err != nil {
		return TransactionPollResult{}, err
	}
	defer processor.endRun()

	return processor.runOnce(ctx, handler)
}

func (processor *TransactionProcessor) runOnce(
	ctx context.Context,
	handler TransactionHandler,
) (TransactionPollResult, error) {
	fetches := processor.client.PollRecords(ctx, processor.maxPollRecords)
	records := fetches.Records()
	defer recycleFetchedRecords(records)
	result := TransactionPollResult{Polled: len(records)}
	if err := fetches.Err(); err != nil {
		pollErr := newConsumerError(ConsumerOperationPoll, err)
		if len(records) == 0 {
			return TransactionPollResult{}, pollErr
		}
		if transactionErr := processor.beginTransaction(ctx); transactionErr != nil {
			processor.fence(transactionErr, transactionProcessorFatalCategory(transactionErr))

			return result, errors.Join(pollErr, transactionErr)
		}
		abortErr := processor.abortTransaction(ctx)
		if abortErr != nil {
			processor.fence(abortErr, transactionProcessorFatalCategory(abortErr))
		}

		return result, errors.Join(pollErr, abortErr)
	}
	if len(records) == 0 {
		return TransactionPollResult{}, nil
	}
	if transactionErr := processor.beginTransaction(ctx); transactionErr != nil {
		processor.fence(transactionErr, transactionProcessorFatalCategory(transactionErr))

		return result, transactionErr
	}

	publisher := &processorTransactionPublisher{
		client:          processor.client,
		interruptClient: processor.interruptClient,
		waitTimeout:     processor.deliveryWaitTimeout,
		limits:          processor.limits,
		keyRequired:     processor.keyRequired,
		allowedTopics:   processor.allowedTopics,
		maxOutputCount:  processor.maxOutputRecords,
		maxOutputBytes:  processor.maxOutputBytes,
	}
	workCtx, cancelWork := context.WithTimeout(ctx, processor.processingTimeout)
	var processingErr error
	for _, record := range records {
		message, err := consumedMessageWithinLimits(record, processor.limits)
		if err == nil {
			session := &transactionSession{publisher: publisher}
			err = callTransactionHandler(
				workCtx,
				handler,
				message,
				Transaction{session: session},
			)
			session.closeAndWait()
			if publishErr := publisher.failure(); publishErr != nil {
				err = errors.Join(err, publishErr)
			}
			if cause := context.Cause(workCtx); cause != nil {
				err = errors.Join(err, cause)
			}
		}
		if err != nil {
			processingErr = err

			break
		}
		result.Processed++
	}
	cancelWork()
	result.Published = publisher.publishedCount()
	if processingErr != nil {
		if terminal, category := publisher.terminalFailure(); terminal {
			processor.terminate(processingErr, category)

			return result, errors.Join(processingErr, processor.fatalError())
		}
		abortErr := processor.abortTransaction(ctx)
		if abortErr != nil {
			processor.fence(abortErr, transactionProcessorFatalCategory(abortErr))
		}

		return result, errors.Join(processingErr, abortErr)
	}

	_, err := processor.commitTransaction(ctx)
	if err != nil {
		if !errors.Is(err, ErrTransactionNotCommitted) {
			processor.fence(err, transactionProcessorFatalCategory(err))
		}

		return result, err
	}
	result.Committed = true

	return result, nil
}

func callTransactionHandler(
	ctx context.Context,
	handler TransactionHandler,
	record ConsumedRecord,
	transaction Transaction,
) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrHandlerPanic
		}
	}()

	return handler.Handle(ctx, record, transaction)
}

type processorTransactionPublisher struct {
	client           transactionProcessorBackend
	interruptClient  func()
	waitTimeout      time.Duration
	limits           MessageLimits
	keyRequired      bool
	allowedTopics    map[string]struct{}
	maxOutputCount   int
	maxOutputBytes   int64
	mu               sync.Mutex
	published        int
	reservedCount    int
	reservedBytes    int64
	err              error
	terminalErr      error
	terminalCategory ErrorCategory
	terminal         bool
}

func (publisher *processorTransactionPublisher) publish(
	ctx context.Context,
	record ProducerRecord,
) error {
	if err := record.validate(publisher.limits); err != nil {
		publisher.recordFailure(err)

		return err
	}
	if _, allowed := publisher.allowedTopics[record.Topic]; !allowed {
		publisher.recordFailure(ErrTopicNotAllowed)

		return ErrTopicNotAllowed
	}
	if publisher.keyRequired && len(record.Key) == 0 {
		publisher.recordFailure(ErrKeyRequired)

		return ErrKeyRequired
	}
	if err := ctx.Err(); err != nil {
		deliveryErr := newDeliveryError(errors.Join(err, context.Cause(ctx)))
		publisher.recordFailure(deliveryErr)

		return deliveryErr
	}
	if err := publisher.reserve(record); err != nil {
		return err
	}
	deliveries, cause := transactionalProduceSync(
		ctx,
		publisher.waitTimeout,
		publisher.interruptClient,
		publisher.client,
		franzRecord(record.owned()),
	)
	if cause != nil {
		err := newDeliveryError(cause)
		publisher.recordTerminalFailure(err, err.Category())

		return err
	}
	if len(deliveries) != 1 {
		err := newDeliveryError(ErrDeliveryResultMissing)
		publisher.recordFailure(err)

		return err
	}
	if deliveries[0].Record == nil {
		err := newDeliveryError(ErrDeliveryResultMissing)
		publisher.recordFailure(err)

		return err
	}
	if deliveries[0].Err != nil {
		err := newDeliveryError(deliveries[0].Err)
		publisher.recordFailure(err)

		return err
	}
	publisher.mu.Lock()
	publisher.published++
	publisher.mu.Unlock()

	return nil
}

func (publisher *processorTransactionPublisher) reserve(
	record ProducerRecord,
) error {
	size := recordSize(record)
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	var err error
	if publisher.reservedCount >= publisher.maxOutputCount {
		err = ErrTooManyTransactionOutputRecords
	} else if size > publisher.maxOutputBytes-publisher.reservedBytes {
		err = ErrTransactionOutputTooLarge
	}
	if err != nil {
		if publisher.err == nil {
			publisher.err = err
		}

		return err
	}
	publisher.reservedCount++
	publisher.reservedBytes += size

	return nil
}

func (publisher *processorTransactionPublisher) recordFailure(err error) {
	publisher.mu.Lock()
	if publisher.err == nil {
		publisher.err = err
	}
	publisher.mu.Unlock()
}

func (publisher *processorTransactionPublisher) recordTerminalFailure(
	err error,
	category ErrorCategory,
) {
	publisher.mu.Lock()
	if publisher.terminalErr == nil {
		publisher.terminalErr = err
		publisher.terminalCategory = category
	}
	publisher.terminal = true
	publisher.mu.Unlock()
}

func (publisher *processorTransactionPublisher) terminalFailure() (
	bool,
	ErrorCategory,
) {
	publisher.mu.Lock()
	defer publisher.mu.Unlock()

	return publisher.terminal, publisher.terminalCategory
}

func (publisher *processorTransactionPublisher) failure() error {
	publisher.mu.Lock()
	defer publisher.mu.Unlock()

	if publisher.err == nil {
		return publisher.terminalErr
	}
	if publisher.terminalErr == nil {
		return publisher.err
	}

	return errors.Join(publisher.err, publisher.terminalErr)
}

func (publisher *processorTransactionPublisher) publishedCount() int {
	publisher.mu.Lock()
	defer publisher.mu.Unlock()

	return publisher.published
}

func (processor *TransactionProcessor) beginTransaction(ctx context.Context) error {
	var startedAt time.Time
	if processor.observers.enabled() {
		startedAt = time.Now()
	}
	err := newTransactionError(
		TransactionOperationBegin,
		processor.client.Begin(),
		false,
		true,
	)
	if err == nil {
		processor.lifecycleMu.Lock()
		processor.transactionActive = true
		processor.lifecycleMu.Unlock()
	}
	processor.observeTransaction(
		ctx,
		ObservationTransactionBegin,
		startedAt,
		err,
	)

	return err
}

func (processor *TransactionProcessor) commitTransaction(
	ctx context.Context,
) (bool, error) {
	defer processor.finishTransaction()
	var startedAt time.Time
	if processor.observers.enabled() {
		startedAt = time.Now()
	}
	endCtx, cancel := processor.transactionEndContext(ctx)
	committed, endErr := processor.client.End(endCtx, kgo.TryCommit)
	cancel()

	var err error
	if endErr != nil {
		err = transactionProcessorEndError(TransactionOperationCommit, endErr)
	} else if !committed {
		err = newTransactionErrorWithCategory(
			TransactionOperationCommit,
			ErrTransactionNotCommitted,
			ErrorRetryable,
			true,
			true,
		)
	}
	processor.observeTransaction(
		ctx,
		ObservationTransactionCommit,
		startedAt,
		err,
	)

	return committed, err
}

func (processor *TransactionProcessor) abortTransaction(ctx context.Context) error {
	defer processor.finishTransaction()
	var startedAt time.Time
	if processor.observers.enabled() {
		startedAt = time.Now()
	}
	endCtx, cancel := processor.transactionEndContext(ctx)
	committed, endErr := processor.client.End(endCtx, kgo.TryAbort)
	cancel()

	var err error
	if endErr != nil {
		err = transactionProcessorEndError(TransactionOperationAbort, endErr)
	} else if committed {
		err = newTransactionError(
			TransactionOperationAbort,
			ErrTransactionOutcomeUnknown,
			false,
			false,
		)
	}
	processor.observeTransaction(
		ctx,
		ObservationTransactionAbort,
		startedAt,
		err,
	)

	return err
}

func transactionProcessorEndError(
	operation TransactionOperation,
	err error,
) error {
	category := classifyError(err)
	if category == ErrorAuthorization ||
		category == ErrorFenced ||
		category == ErrorFatal {
		return newTransactionError(operation, err, false, true)
	}
	if errors.Is(err, kerr.TransactionAbortable) ||
		errors.Is(err, kerr.OperationNotAttempted) {
		return newTransactionError(operation, err, true, true)
	}

	return newTransactionError(
		operation,
		errors.Join(ErrTransactionOutcomeUnknown, err),
		false,
		false,
	)
}

func (processor *TransactionProcessor) transactionEndContext(
	ctx context.Context,
) (context.Context, context.CancelFunc) {
	return context.WithTimeout(
		context.WithoutCancel(ctx),
		processor.transactionEndTime,
	)
}

// Run executes bounded all-or-nothing polls until cancellation or the first
// source-poll, processing, or transaction failure. Caller cancellation is a
// clean stop only after the active transaction aborts successfully; cleanup
// failure is returned and fences the processor.
func (processor *TransactionProcessor) Run(
	ctx context.Context,
	handler TransactionHandler,
) error {
	if ctx == nil {
		return ErrContextRequired
	}
	if isObserverContext(ctx) {
		return ErrObserverReentry
	}
	if handler == nil {
		return ErrTransactionHandlerRequired
	}
	if err := processor.beginRun(); err != nil {
		return err
	}
	defer processor.endRun()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		if _, err := processor.runOnce(ctx, handler); err != nil {
			if ctx.Err() != nil {
				processor.lifecycleMu.Lock()
				fatal := processor.fatalErr != nil
				processor.lifecycleMu.Unlock()
				if fatal {
					return err
				}

				return nil
			}

			return err
		}
	}
}

func (processor *TransactionProcessor) beginRun() error {
	processor.lifecycleMu.Lock()
	defer processor.lifecycleMu.Unlock()
	if processor.closed {
		return ErrTransactionProcessorClosed
	}
	if processor.closing {
		return ErrTransactionProcessorClosing
	}
	if processor.fatalErr != nil {
		return processor.fatalErr
	}
	if processor.running {
		return ErrTransactionProcessorBusy
	}
	processor.running = true
	processor.runDone = make(chan struct{})

	return nil
}

// Diagnostic returns a bounded, payload-free snapshot of local processor
// state without performing Kafka I/O, exposing the retained fatal error, or
// invoking methods on it.
func (processor *TransactionProcessor) Diagnostic() TransactionProcessorDiagnostic {
	bufferedRecords := processor.client.BufferedProduceRecords()
	bufferedBytes := processor.client.BufferedProduceBytes()

	processor.lifecycleMu.Lock()
	defer processor.lifecycleMu.Unlock()

	return TransactionProcessorDiagnostic{
		Accepting: processor.fatalErr == nil && !processor.running &&
			!processor.closing && !processor.closed,
		Running:           processor.running,
		TransactionActive: processor.transactionActive,
		Closing:           processor.closing && !processor.closed,
		ShutdownActive:    processor.shutdownActive,
		Closed:            processor.closed,
		Fatal:             processor.fatalErr != nil,
		ClientTerminated:  processor.clientTerminated,
		FatalCategory:     processor.fatalCategory,
		BufferedRecords:   bufferedRecords,
		BufferedBytes:     bufferedBytes,
	}
}

func (processor *TransactionProcessor) finishTransaction() {
	processor.lifecycleMu.Lock()
	processor.transactionActive = false
	processor.lifecycleMu.Unlock()
}

func transactionProcessorFatalCategory(err error) ErrorCategory {
	transactionErr, ok := err.(*TransactionError)
	if !ok || !validErrorCategory(transactionErr.category) {
		return ErrorFatal
	}

	return transactionErr.category
}

func (processor *TransactionProcessor) fence(err error, category ErrorCategory) {
	processor.lifecycleMu.Lock()
	if processor.fatalErr == nil {
		processor.fatalErr = errors.Join(ErrTransactionProcessorFatal, err)
		processor.fatalCategory = category
	}
	processor.lifecycleMu.Unlock()
}

func (processor *TransactionProcessor) fatalError() error {
	processor.lifecycleMu.Lock()
	defer processor.lifecycleMu.Unlock()

	return processor.fatalErr
}

func (processor *TransactionProcessor) terminate(err error, category ErrorCategory) {
	processor.lifecycleMu.Lock()
	if processor.fatalErr == nil {
		processor.fatalErr = errors.Join(ErrTransactionProcessorFatal, err)
		processor.fatalCategory = category
	}
	processor.clientTerminated = true
	processor.transactionActive = false
	processor.lifecycleMu.Unlock()
	processor.interruptClient()
}

func (processor *TransactionProcessor) interruptClient() {
	cancelClient := processor.cancelClient
	if cancelClient != nil {
		cancelClient()
	}
	processor.closeOnce.Do(processor.client.Close)
}

func (processor *TransactionProcessor) endRun() {
	processor.lifecycleMu.Lock()
	done := processor.runDone
	processor.runDone = nil
	processor.running = false
	processor.lifecycleMu.Unlock()
	close(done)
}

// Shutdown fences new runs, waits for the active runner, leaves a dynamic
// group, and closes the client. An incomplete shutdown remains retryable.
func (processor *TransactionProcessor) Shutdown(ctx context.Context) (err error) {
	if ctx == nil {
		return ErrContextRequired
	}
	if isObserverContext(ctx) {
		return ErrObserverReentry
	}
	processor.lifecycleMu.Lock()
	if processor.observerCallbacks != 0 {
		processor.lifecycleMu.Unlock()

		return ErrObserverReentry
	}
	if processor.closed {
		processor.lifecycleMu.Unlock()

		return nil
	}
	if processor.shutdownActive {
		processor.lifecycleMu.Unlock()

		return ErrTransactionProcessorShutdownActive
	}
	processor.closing = true
	processor.shutdownActive = true
	done := processor.runDone
	staticMembership := processor.staticMembership
	processor.lifecycleMu.Unlock()
	var startedAt time.Time
	if processor.observers.enabled() {
		startedAt = time.Now()
		defer func() {
			processor.observeShutdown(ctx, startedAt, err)
		}()
	}

	complete := false
	defer func() {
		processor.lifecycleMu.Lock()
		processor.shutdownActive = false
		if complete {
			processor.closed = true
		}
		processor.lifecycleMu.Unlock()
	}()
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return errors.Join(
				ErrTransactionProcessorShutdownIncomplete,
				ctx.Err(),
			)
		}
	}
	processor.lifecycleMu.Lock()
	clientTerminated := processor.clientTerminated
	processor.lifecycleMu.Unlock()
	if clientTerminated {
		complete = true

		return nil
	}
	if !staticMembership {
		if leaveErr := processor.client.LeaveGroupContext(ctx); leaveErr != nil {
			return errors.Join(
				ErrTransactionProcessorShutdownIncomplete,
				leaveErr,
			)
		}
	}
	if processor.cancelClient != nil {
		processor.cancelClient()
	}
	processor.closeOnce.Do(processor.client.Close)
	complete = true

	return nil
}

// Close performs a bounded graceful shutdown using the configured timeout.
func (processor *TransactionProcessor) Close() error {
	processor.lifecycleMu.Lock()
	if processor.observerCallbacks != 0 {
		processor.lifecycleMu.Unlock()

		return ErrObserverReentry
	}
	processor.lifecycleMu.Unlock()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		processor.shutdownTimeout,
	)
	defer cancel()

	return processor.Shutdown(ctx)
}

func (processor *TransactionProcessor) observeShutdown(
	ctx context.Context,
	startedAt time.Time,
	err error,
) {
	observation := Observation{
		Kind:      ObservationTransactionProcessorShutdown,
		StartedAt: startedAt,
		Duration:  time.Since(startedAt),
		ClientID:  processor.clientID,
		GroupID:   processor.groupID,
		Succeeded: err == nil,
	}
	if err != nil {
		observation.Category = classifyError(err)
	}
	processor.dispatchObservation(ctx, observation)
}

func (processor *TransactionProcessor) observeTransaction(
	ctx context.Context,
	kind ObservationKind,
	startedAt time.Time,
	err error,
) {
	if !processor.observers.enabled() {
		return
	}
	observation := Observation{
		Kind:      kind,
		StartedAt: startedAt,
		Duration:  time.Since(startedAt),
		ClientID:  processor.clientID,
		GroupID:   processor.groupID,
		Succeeded: err == nil,
	}
	if err != nil {
		observation.Category = transactionObservationCategory(err)
	}
	processor.dispatchObservation(ctx, observation)
}

func (processor *TransactionProcessor) dispatchObservation(
	ctx context.Context,
	observation Observation,
) {
	processor.beginObservation()
	defer processor.finishObservation()

	processor.observers.observe(ctx, observation)
}

func (processor *TransactionProcessor) beginObservation() {
	processor.lifecycleMu.Lock()
	processor.observerCallbacks++
	processor.lifecycleMu.Unlock()
}

func (processor *TransactionProcessor) finishObservation() {
	processor.lifecycleMu.Lock()
	processor.observerCallbacks--
	processor.lifecycleMu.Unlock()
}
