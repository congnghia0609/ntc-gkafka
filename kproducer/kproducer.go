package kproducer

import (
	"fmt"
	"golang.org/x/net/context"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"ntc-gkafka/kconfig"
	"sync"
	"time"
)

type KProducer struct {
	Name         string
	Id           string
	Conn         *kafka.Producer
	DeliveryChan chan kafka.Event
}

var mKP sync.Mutex
var mapInstanceKP = map[string]*KProducer{}

func NewKProducer(name string) *KProducer {
	if len(name) == 0 {
		return nil
	}
	deliveryChan := make(chan kafka.Event)
	pconf := kconfig.GetProduceConfig(name)
	id, _ := pconf.Get("client.id", "")
	producer, err := kafka.NewProducer(pconf)
	if err != nil {
		fmt.Errorf("Failed to create producer: %v\n", err)
	}
	// Go-routine to handle message delivery reports and
	// possibly other event types (errors, stats, etc)
	go func() {
		for {
			select {
			case e := <-deliveryChan:
				switch ev := e.(type) {
				case *kafka.Message:
					if ev.TopicPartition.Error != nil {
						fmt.Printf("Producer[%s] Failed to deliver message: %v\n", id, ev.TopicPartition)
					} else {
						fmt.Printf("Producer[%s] successfully produced recored to topic %s partion [%d] @ offset %v\n", id, *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
					}
				}
			}
		}
	}()
	return &KProducer{Name: name, Id: id.(string), Conn: producer, DeliveryChan: deliveryChan}
}

func GetInstance(name string) *KProducer {
	instance := mapInstanceKP[name]
	if instance == nil {
		mKP.Lock()
		defer mKP.Unlock()
		instance = mapInstanceKP[name]
		if instance == nil {
			instance = NewKProducer(name)
			mapInstanceKP[name] = instance
		}
	}
	return instance
}

func (kp *KProducer) Close() {
	kp.Close()
}

func (kp *KProducer) SendRecord(topic string, msg string) error {
	return kp.Conn.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            nil,
		Value:          []byte(msg),
	}, kp.DeliveryChan)
}

func (kp *KProducer) SendRecordKV(topic string, key string, value string) error {
	return kp.Conn.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          []byte(value),
	}, kp.DeliveryChan)
}

func (kp *KProducer) SendRecordByte(topic string, msg []byte) error {
	return kp.Conn.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            nil,
		Value:          msg,
	}, kp.DeliveryChan)
}

func (kp *KProducer) SendRecordKVByte(topic string, key []byte, value []byte) error {
	return kp.Conn.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          value,
	}, kp.DeliveryChan)
}

func (kp *KProducer) CreateTopic(topic string, partitions int, replications int) {
	a, err := kafka.NewAdminClientFromProducer(kp.Conn)
	if err != nil {
		fmt.Errorf("Failed to create new admin client from producer: %s", err)
	}

	// Contexts are used to abort or limit the amount of time
	// the Admin call blocks waiting for a result.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Create topics on cluster.
	// Set Admin options to wait up to 60s for the operation to finish on the remote cluster
	maxDur := 60 * time.Second
	numPart := 1
	if partitions > numPart {
		numPart = partitions
	}
	repl := 1
	if replications > repl {
		repl = replications
	}
	results, err := a.CreateTopics(
		ctx,
		// Multiple topics can be created simultaneously
		// by providing more TopicSpecification structs here.
		[]kafka.TopicSpecification{{
			Topic:             topic,
			NumPartitions:     numPart,
			ReplicationFactor: repl,
		}},
		// Admin options
		kafka.SetAdminOperationTimeout(maxDur))
	if err != nil {
		fmt.Errorf("Admin Client request error: %v\n", err)
	}
	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError && result.Error.Code() != kafka.ErrTopicAlreadyExists {
			fmt.Errorf("Failed to create topic: %v\n", result.Error)
		}
		fmt.Printf("%v\n", result)
	}
	a.Close()
}

func SendRecord(name string, topic string, msg string) error {
	kp := GetInstance(name)
	return kp.Conn.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            nil,
		Value:          []byte(msg),
	}, kp.DeliveryChan)
}

func SendRecordKV(name string, topic string, key string, value string) error {
	kp := GetInstance(name)
	return kp.Conn.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          []byte(value),
	}, kp.DeliveryChan)
}

func SendRecordByte(name string, topic string, msg []byte) error {
	kp := GetInstance(name)
	return kp.Conn.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            nil,
		Value:          msg,
	}, kp.DeliveryChan)
}

func SendRecordKVByte(name string, topic string, key []byte, value []byte) error {
	kp := GetInstance(name)
	return kp.Conn.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          value,
	}, kp.DeliveryChan)
}

func CreateTopic(p *kafka.Producer, topic string, partitions int, replications int) {
	a, err := kafka.NewAdminClientFromProducer(p)
	if err != nil {
		fmt.Errorf("Failed to create new admin client from producer: %s", err)
	}

	// Contexts are used to abort or limit the amount of time
	// the Admin call blocks waiting for a result.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Create topics on cluster.
	// Set Admin options to wait up to 60s for the operation to finish on the remote cluster
	maxDur := 60 * time.Second
	numPart := 1
	if partitions > 1 {
		numPart = partitions
	}
	repl := 1
	if replications > 1 {
		repl = replications
	}
	results, err := a.CreateTopics(
		ctx,
		// Multiple topics can be created simultaneously
		// by providing more TopicSpecification structs here.
		[]kafka.TopicSpecification{{
			Topic:             topic,
			NumPartitions:     numPart,
			ReplicationFactor: repl,
		}},
		// Admin options
		kafka.SetAdminOperationTimeout(maxDur))
	if err != nil {
		fmt.Errorf("Admin Client request error: %v\n", err)
	}
	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError && result.Error.Code() != kafka.ErrTopicAlreadyExists {
			fmt.Errorf("Failed to create topic: %v\n", result.Error)
		}
		fmt.Printf("%v\n", result)
	}
	a.Close()
}
