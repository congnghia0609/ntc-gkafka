package kconsumer

import (
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func Start() {
	run := true
	topics := []string {"email"}
	MIN_COMMIT_COUNT := 1
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers" : "127.0.0.1:9092",
		"group.id": "worker",
		"auto.offset.reset": "smallest",
		"go.application.rebalance.enable": true,
	})
	if err != nil {
		log.Fatalf("kafka.NewConsumer fail: %v\n", err)
	}

	err = consumer.SubscribeTopics(topics, nil)
	if err != nil {
		log.Fatalf("consumer.SubscribeTopics fail: %v\n", err)
	}

	// Set up a channel for handling Ctrl-C, etc
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	msg_count := 0
	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := consumer.Poll(100)
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				consumer.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				consumer.Unassign()
			case *kafka.Message:
				msg_count += 1
				if msg_count % MIN_COMMIT_COUNT == 0 {
					consumer.Commit()
				}
				fmt.Printf("%% Message on %s:\n%s\n", e.TopicPartition, string(e.Value))

			case kafka.PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
				run = false
			default:
				//fmt.Printf("Ignored %v\n", e)
			}
		}
	}
	fmt.Printf("Closing consumer\n")
	consumer.Close()
}

