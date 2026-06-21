//go:build windows

package kafkaclient

import (
	"errors"
	"log"
)

var errWindowsKafkaClient = errors.New("Kafka client is not available in the native Windows build; run the service with Docker or WSL")

// KafkaMessage is the internal message type produced by the consumer.
type KafkaMessage struct {
	Topic   string
	Payload []byte
	Key     string
}

// Consumer is a Windows compile-time stub for the librdkafka-backed consumer.
type Consumer struct{}

func NewConsumer(brokers, groupID, clientID string, topics []string) (*Consumer, error) {
	return nil, errWindowsKafkaClient
}

func NewManualCommitConsumer(brokers, groupID, clientID string, topics []string) (*Consumer, error) {
	return nil, errWindowsKafkaClient
}

func (c *Consumer) Run(outCh chan<- KafkaMessage, doneCh <-chan struct{}) {
	<-doneCh
}

func (c *Consumer) Close() {}

func (c *Consumer) Commit() error {
	return nil
}

// Producer is a Windows compile-time stub for the librdkafka-backed producer.
type Producer struct{}

func NewProducer(brokers string) (*Producer, error) {
	return nil, errWindowsKafkaClient
}

func (p *Producer) Produce(topic, key string, value []byte) error {
	return errWindowsKafkaClient
}

func (p *Producer) ProduceJSON(topic, key string, v interface{}) error {
	return errWindowsKafkaClient
}

func ProduceExactlyOnce(brokers, topic, key string, value []byte) error {
	return errWindowsKafkaClient
}

func (p *Producer) ProduceToRaw(playerID string, payload []byte) error {
	return errWindowsKafkaClient
}

func (p *Producer) Flush(timeoutMs int) {
	log.Printf("[kafka-windows-stub] Flush ignored: %v", errWindowsKafkaClient)
}

func (p *Producer) Close() {}
