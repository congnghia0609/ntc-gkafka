package kconsumer

import (
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"log"
	"ntc-gkafka/kconfig"
	"os"
)

type KConsumer struct {
	name    string
	groupId string
	conn    *kafka.Consumer
	isRun   bool
	poll    int
}

var MinCommit = 1

func NewKConsumer(name string) *KConsumer {
	if len(name) == 0 {
		return nil
	}
	topics := kconfig.GetConsumeTopics(name, "")
	cconf := kconfig.GetConsumeConfig(name)
	groupId, _ := cconf.Get("group.id", "")
	poll := kconfig.GetConsumePoll(name, 500)

	consumer, err := kafka.NewConsumer(cconf)
	if err != nil {
		log.Fatalf("kafka.NewConsumer fail: %v\n", err)
	}
	err = consumer.SubscribeTopics(topics, nil)
	if err != nil {
		log.Fatalf("consumer.SubscribeTopics fail: %v\n", err)
	}
	return &KConsumer{name: name, groupId: groupId.(string), conn: consumer, isRun: true, poll: poll}
}

func (kc *KConsumer) Start(processChan chan *kafka.Message) error {
	msgCount := 0
	for kc.isRun == true {
		select {
		default:
			ev := kc.conn.Poll(kc.poll)
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				kc.conn.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				kc.conn.Unassign()
			case *kafka.Message:
				processChan <- e
				msgCount += 1
				if msgCount%MinCommit == 0 {
					kc.conn.Commit()
				}
				//fmt.Printf("%% Message on %s:\n%s\n", e.TopicPartition, string(e.Value))

			case kafka.PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
				kc.isRun = false
			default:
				//fmt.Printf("Ignored %v\n", e)
			}
		}
	}
	fmt.Println("Start end function...")
	return nil
}

func (kc *KConsumer) Stop() {
	fmt.Printf("Closing consumer\n")
	kc.isRun = false
	kc.conn.Close()
}
