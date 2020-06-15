package main

import (
	"fmt"
	"github.com/congnghia0609/ntc-gconf/nconf"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"ntc-gkafka/kconsumer"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
)

func InitNConf2() {
	_, b, _, _ := runtime.Caller(0)
	wdir := filepath.Dir(b)
	fmt.Println("wdir:", wdir)
	nconf.Init(wdir)
}

func main() {
	// Init NConf
	InitNConf2()

	// Consumer
	//kconsumer.Start()

	name := "worker"
	kc := kconsumer.NewKConsumer(name)
	processChan := make(chan *kafka.Message)
	go kc.Start(processChan)

	// Set up a channel for handling Ctrl-C, etc
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Printf("Sub start...\n")
	run := true
	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			kc.Stop()
			run = false
			break
		case e := <-processChan:
			// Process message in here.
			fmt.Printf("##### Sub Message on %s:\n%s\n", e.TopicPartition, string(e.Value))
		}
	}
	fmt.Printf("Main Sub end task and stop\n")

	// Producer
	//kproducer.Start()
}
