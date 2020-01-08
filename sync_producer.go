package sarama

import "sync"

// SyncProducer publishes Kafka messages, blocking until they have been acknowledged. It routes messages to the correct
// broker, refreshing metadata as appropriate, and parses responses for errors. You must call Close() on a producer
// to avoid leaks, it may not be garbage-collected automatically when it passes out of scope.
//
// The SyncProducer comes with two caveats: it will generally be less efficient than the AsyncProducer, and the actual
// durability guarantee provided when a message is acknowledged depend on the configured value of `Producer.RequiredAcks`.
// There are configurations where a message acknowledged by the SyncProducer can still sometimes be lost.
//
// For implementation reasons, the SyncProducer requires `Producer.Return.Errors` and `Producer.Return.Successes` to
// be set to true in its configuration.
type SyncProducer interface {

	// SendMessage produces a given message, and returns only when it either has
	// succeeded or failed to produce. It will return the partition and the offset
	// of the produced message, or an error if the message failed to produce.
	SendMessage(msg *ProducerMessage) (partition int32, offset int64, err error)

	// SendMessages produces a given set of messages, and returns only when all
	// messages in the set have either succeeded or failed. Note that messages
	// can succeed and fail individually; if some succeed and some fail,
	// SendMessages will return an error.
	SendMessages(msgs []*ProducerMessage) error

	// Close shuts down the producer and waits for any buffered messages to be
	// flushed. You must call this function before a producer object passes out of
	// scope, as it may otherwise leak memory. You must call this before calling
	// Close on the underlying client.
	Close() error
}

type syncProducer struct {
	/*同步发送本质也是用了异步发送，但是可能加了锁，等待结果*/
	producer *asyncProducer
	wg       sync.WaitGroup
}

// NewSyncProducer creates a new SyncProducer using the given broker addresses and configuration.
func NewSyncProducer(addrs []string, config *Config) (SyncProducer, error) {
	if config == nil {
		config = NewConfig()
		config.Producer.Return.Successes = true
	}

	if err := verifyProducerConfig(config); err != nil {
		return nil, err
	}

	p, err := NewAsyncProducer(addrs, config)
	if err != nil {
		/*异常才会在这里返回*/
		return nil, err
	}
	return newSyncProducerFromAsyncProducer(p.(*asyncProducer)), nil
}

// NewSyncProducerFromClient creates a new SyncProducer using the given client. It is still
// necessary to call Close() on the underlying client when shutting down this producer.
func NewSyncProducerFromClient(client Client) (SyncProducer, error) {
	if err := verifyProducerConfig(client.Config()); err != nil {
		return nil, err
	}

	p, err := NewAsyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}
	return newSyncProducerFromAsyncProducer(p.(*asyncProducer)), nil
}

/*一般都会走到这个函数*/
func newSyncProducerFromAsyncProducer(p *asyncProducer) *syncProducer {
	sp := &syncProducer{producer: p}

	/*如果这里add 2 那么是不是一次性的*/
	sp.wg.Add(2)
	go withRecover(sp.handleSuccesses)
	go withRecover(sp.handleErrors)

	return sp
}

func verifyProducerConfig(config *Config) error {
	if !config.Producer.Return.Errors {
		return ConfigurationError("Producer.Return.Errors must be true to be used in a SyncProducer")
	}
	if !config.Producer.Return.Successes {
		return ConfigurationError("Producer.Return.Successes must be true to be used in a SyncProducer")
	}
	return nil
}

func (sp *syncProducer) SendMessage(msg *ProducerMessage) (partition int32, offset int64, err error) {
	expectation := make(chan *ProducerError, 1)
	msg.expectation = expectation
	/*本质是用了异步发送，，，，，
	哪里在死循环读这个input*/
	sp.producer.Input() <- msg

	/*可能强行等待异常，这么做的同步
	哪里在写这个expectation*/
	if err := <-expectation; err != nil {
		return -1, -1, err.Err
	}

	return msg.Partition, msg.Offset, nil
}

func (sp *syncProducer) SendMessages(msgs []*ProducerMessage) error {
	expectations := make(chan chan *ProducerError, len(msgs))
	go func() {
		for _, msg := range msgs {
			expectation := make(chan *ProducerError, 1)
			msg.expectation = expectation
			sp.producer.Input() <- msg
			expectations <- expectation
		}
		close(expectations)
	}()

	var errors ProducerErrors
	for expectation := range expectations {
		if err := <-expectation; err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return errors
	}
	return nil
}

func (sp *syncProducer) handleSuccesses() {
	defer sp.wg.Done()
	for msg := range sp.producer.Successes() {
		expectation := msg.expectation
		expectation <- nil
	}
}

func (sp *syncProducer) handleErrors() {
	defer sp.wg.Done()
	for err := range sp.producer.Errors() {
		expectation := err.Msg.expectation
		expectation <- err
	}
}

func (sp *syncProducer) Close() error {
	sp.producer.AsyncClose()

	/*关闭的时候才会等待？？*/
	sp.wg.Wait()
	return nil
}
