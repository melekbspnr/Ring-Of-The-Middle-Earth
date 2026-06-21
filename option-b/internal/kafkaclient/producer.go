//go:build !windows

// Package kafkaclient - producer wraps confluent-kafka-go producer.
// GameOver events are produced with enable.idempotence=true (exactly-once, Section 13).
package kafkaclient

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"ring-of-the-middle-earth/internal/avrowire"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Producer wraps a Kafka producer.
type Producer struct {
	p    *kafka.Producer
	avro *avrowire.Registry
}

// NewProducer creates a standard producer for game events.
// K6: enable.idempotence=true ensures exactly-once delivery semantics
// even for the standard producer path, not just the GameOver transaction.
func NewProducer(brokers string) (*Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":                     brokers,
		"acks":                                  "all", // wait for all in-sync replicas
		"enable.idempotence":                    true,  // K6: exactly-once idempotent producer
		"max.in.flight.requests.per.connection": 5,     // safe with idempotence enabled
		"retries":                               10,
		"linger.ms":                             5,
	})
	if err != nil {
		return nil, err
	}
	// Start delivery report handler goroutine
	go func() {
		for e := range p.Events() {
			if msg, ok := e.(*kafka.Message); ok {
				if msg.TopicPartition.Error != nil {
					log.Printf("[kafka-producer] delivery failed: %v", msg.TopicPartition.Error)
				}
			}
		}
	}()
	return &Producer{p: p, avro: avrowire.NewRegistry()}, nil
}

// Produce sends a message to a topic with the given key and value.
func (p *Producer) Produce(topic, key string, value []byte) error {
	encoded, err := p.avro.Encode(topic, value)
	if err != nil {
		return err
	}
	return p.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:   []byte(key),
		Value: encoded,
	}, nil)
}

// ProduceJSON marshals v to JSON and produces to topic.
func (p *Producer) ProduceJSON(topic, key string, v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return p.Produce(topic, key, b)
}

// ProduceExactlyOnce produces a message in a Kafka transaction.
// Used for GameOver to approximate exactly-once delivery semantics more closely.
func ProduceExactlyOnce(brokers, topic, key string, value []byte) error {
	transactionalID := fmt.Sprintf("rotr-%s-%s", topic, key)
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":                     brokers,
		"enable.idempotence":                    true, // exactly-once guarantee
		"acks":                                  "all",
		"retries":                               10,
		"max.in.flight.requests.per.connection": 1,
		"transactional.id":                      transactionalID,
	})
	if err != nil {
		return err
	}
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	encoded, err := avrowire.NewRegistry().Encode(topic, value)
	if err != nil {
		return err
	}

	if err := p.InitTransactions(ctx); err != nil {
		return err
	}
	if err := p.BeginTransaction(); err != nil {
		return err
	}

	deliveryCh := make(chan kafka.Event, 1)
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:   []byte(key),
		Value: encoded,
	}, deliveryCh)
	if err != nil {
		return err
	}

	e := <-deliveryCh
	msg := e.(*kafka.Message)
	if msg.TopicPartition.Error != nil {
		_ = p.AbortTransaction(ctx)
		return msg.TopicPartition.Error
	}
	if err := p.CommitTransaction(ctx); err != nil {
		return err
	}
	log.Printf("[kafka] exactly-once produce OK: topic=%s key=%s", topic, key)
	return nil
}

// ProduceToRaw publishes a raw order to game.orders.raw.
// Partition key is the playerId.
func (p *Producer) ProduceToRaw(playerID string, payload []byte) error {
	return p.Produce("game.orders.raw", playerID, payload)
}

// Flush waits for all outstanding deliveries.
func (p *Producer) Flush(timeoutMs int) {
	p.p.Flush(timeoutMs)
}

// Close shuts down the producer.
func (p *Producer) Close() {
	p.p.Flush(5000)
	p.p.Close()
}
