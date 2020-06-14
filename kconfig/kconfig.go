package kconfig

import (
	"github.com/congnghia0609/ntc-gconf/nconf"
	"github.com/google/uuid"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"strings"
)

const PRODUCER_PREFIX = ".kafka.producer."
const COMSUMER_PREFIX = ".kafka.consumer."
const STREAM_PREFIX = ".kafka.stream."

func GetProduceConfig(name string) *kafka.ConfigMap {
	if len(name) == 0 {
		return nil
	}
	c := nconf.GetConfig()
	servers := c.GetString(name + PRODUCER_PREFIX + "bootstrap.servers")
	acks := c.GetString(name + PRODUCER_PREFIX + "acks")
	return &kafka.ConfigMap{
		"bootstrap.servers": servers,
		"client.id":         name + "_KProducer_" + uuid.New().String(),
		"acks":              acks,
	}
}

func GetProduceTopic(name string, defval string) string {
	if len(name) == 0 {
		return defval
	}
	c := nconf.GetConfig()
	topic := c.GetString(name + PRODUCER_PREFIX + "topic")
	if len(topic) == 0 {
		topic = defval
	}
	return topic
}

func GetConsumeConfig(name string) *kafka.ConfigMap {
	if len(name) == 0 {
		return nil
	}
	c := nconf.GetConfig()
	servers := c.GetString(name + COMSUMER_PREFIX + "bootstrap.servers")
	groupId := c.GetString(name + COMSUMER_PREFIX + "group.id")
	return &kafka.ConfigMap{
		"bootstrap.servers":               servers,
		"group.id":                        groupId,
		"auto.offset.reset":               "smallest",
		"go.application.rebalance.enable": true,
	}
}

func GetConsumePoll(name string, defval int) int {
	if len(name) == 0 {
		if defval > 0 {
			return defval
		}
		return 500
	}
	c := nconf.GetConfig()
	poll := c.GetInt(name + COMSUMER_PREFIX + "poll")
	//fmt.Printf("GetConsumeConfig poll: %v\n", poll)
	if poll <= 0 {
		if defval > 0 {
			return defval
		}
		return 500
	}
	return poll
}

func GetConsumeTopics(name string, defval string) []string {
	if len(name) == 0 {
		if len(defval) == 0 {
			return nil
		}
		return []string{defval}
	}
	c := nconf.GetConfig()
	topics := c.GetString(name + COMSUMER_PREFIX + "topics")
	if len(topics) == 0 {
		if len(defval) == 0 {
			return nil
		}
		return []string{defval}
	}
	return strings.Split(topics, ",")
}
