//go:build !windows

// Package kafkaclient - consumer wraps confluent-kafka-go consumer.
// One Consumer reads all subscribed topics and emits to a shared channel.
package kafkaclient

import (
	"log"

	"ring-of-the-middle-earth/internal/avrowire"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// KafkaMessage is the internal message type produced by the consumer.
// Converted to api.Event by the EventRouter in the api package.
// This type exists to avoid circular imports between kafka and api packages.
type KafkaMessage struct {
	Topic   string
	Payload []byte
	Key     string
}

// Consumer wraps a Kafka consumer for the game engine.
type Consumer struct {
	c          *kafka.Consumer
	clientID   string
	groupID    string
	avro       *avrowire.Registry
	autoCommit bool
}

// NewConsumer creates a consumer subscribed to the given topics.
func NewConsumer(brokers, groupID, clientID string, topics []string) (*Consumer, error) {
	return newConsumer(brokers, groupID, clientID, topics, true)
}

// NewManualCommitConsumer creates a consumer that commits offsets explicitly.
func NewManualCommitConsumer(brokers, groupID, clientID string, topics []string) (*Consumer, error) {
	return newConsumer(brokers, groupID, clientID, topics, false)
}

func newConsumer(brokers, groupID, clientID string, topics []string, autoCommit bool) (*Consumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":       brokers,
		"group.id":                groupID,
		"client.id":               clientID,
		"auto.offset.reset":       "earliest",
		"enable.auto.commit":      autoCommit,
		"auto.commit.interval.ms": 5000,
		"session.timeout.ms":      30000,
		"max.poll.interval.ms":    300000,
	})
	if err != nil {
		return nil, err
	}
	if err := c.SubscribeTopics(topics, rebalanceLogger(clientID, groupID)); err != nil {
		return nil, err
	}
	return &Consumer{c: c, clientID: clientID, groupID: groupID, avro: avrowire.NewRegistry(), autoCommit: autoCommit}, nil
}

// Run polls Kafka and sends events to outCh until doneCh is closed.
func (c *Consumer) Run(outCh chan<- KafkaMessage, doneCh <-chan struct{}) {
	for {
		select {
		case <-doneCh:
			return
		default:
		}

		msg, err := c.c.ReadMessage(100) // 100ms poll timeout
		if err != nil {
			if kerr, ok := err.(kafka.Error); ok && kerr.Code() == kafka.ErrTimedOut {
				continue // normal timeout, keep polling
			}
			log.Printf("[kafka-consumer] read error: %v", err)
			continue
		}

		decoded, err := c.avro.Decode(*msg.TopicPartition.Topic, msg.Value)
		if err != nil {
			log.Printf("[kafka-consumer] decode error on %s: %v", *msg.TopicPartition.Topic, err)
			continue
		}

		outCh <- KafkaMessage{
			Topic:   *msg.TopicPartition.Topic,
			Payload: decoded,
			Key:     string(msg.Key),
		}
	}
}

// Close shuts down the consumer.
func (c *Consumer) Close() {
	if err := c.c.Close(); err != nil {
		log.Printf("[kafka-consumer] close error: %v", err)
	}
}

// Commit flushes the current consumer position when auto-commit is disabled.
func (c *Consumer) Commit() error {
	if c.autoCommit {
		return nil
	}
	_, err := c.c.Commit()
	return err
}

func rebalanceLogger(clientID, groupID string) kafka.RebalanceCb {
	return func(c *kafka.Consumer, event kafka.Event) error {
		switch ev := event.(type) {
		case kafka.AssignedPartitions:
			log.Printf("[kafka-consumer] client=%s group=%s assigned=%v", clientID, groupID, ev.Partitions)
			return c.Assign(ev.Partitions)
		case kafka.RevokedPartitions:
			log.Printf("[kafka-consumer] client=%s group=%s revoked=%v", clientID, groupID, ev.Partitions)
			return c.Unassign()
		default:
			return nil
		}
	}
}
