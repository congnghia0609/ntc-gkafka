package kconsumer

import (
	"fmt"
	"github.com/google/uuid"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"log"
	"ntc-gkafka/kconfig"
	"os"
)

var MinCommit = 1

type KConsumer struct {
	Name    string
	Id      string
	GroupId string
	Conn    *kafka.Consumer
	IsRun   bool
	Poll    int
}

func NewKConsumer(name string) *KConsumer {
	if len(name) == 0 {
		return nil
	}
	id := name + "_KConsumer_" + uuid.New().String()
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
	return &KConsumer{Name: name, Id: id, GroupId: groupId.(string), Conn: consumer, IsRun: true, Poll: poll}
}

func (kc *KConsumer) Start(processChan chan *kafka.Message) error {
	msgCount := 0
	for kc.IsRun == true {
		select {
		default:
			ev := kc.Conn.Poll(kc.Poll)
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				kc.Conn.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				kc.Conn.Unassign()
			case *kafka.Message:
				processChan <- e
				msgCount += 1
				if msgCount%MinCommit == 0 {
					kc.Conn.Commit()
				}
				//fmt.Printf("%% Message on %s:\n%s\n", e.TopicPartition, string(e.Value))

			case kafka.PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
				kc.IsRun = false
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
	kc.IsRun = false
	kc.Conn.Close()
}
