package kconfig

import "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"

var config,_ = kafka.NewProducer(&kafka.ConfigMap{})

