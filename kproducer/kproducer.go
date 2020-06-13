package kproducer

import (
	"fmt"
	"github.com/google/uuid"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"os"
)

func Start() {
	topic := "email"
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "127.0.0.1:9092,127.0.0.1:9093",
		"client.id": "KProducer_" + uuid.New().String(),
		"acks": "1",
	})
	if err != nil {
		fmt.Printf("Failed to create producer: %v\n", err)
		os.Exit(1)
	}
	// Go-routine to handle message delivery reports and
	// possibly other event types (errors, stats, etc)
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Successfully produced recored to topic %s partion [%d] @ offset %v\n", *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			}
		}
	}()

	for i := 0; i < 10; i++ {
		recordKey := "alice"
		recordValue := fmt.Sprintf("This is message %d", i)
		fmt.Printf("Preparing to produce record: %s\t%s\n", recordKey, recordValue)
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key: []byte(recordKey),
			Value: []byte(recordValue),
		}, nil)
	}

	// Wait for all messages to be delivered
	p.Flush(15*1000)
	fmt.Printf("10 messages were produced to topic: %s!\n", topic)
	p.Close()
}
