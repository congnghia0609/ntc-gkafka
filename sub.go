package main

import (
	"fmt"
	"github.com/congnghia0609/ntc-gconf/nconf"
	"github.com/congnghia0609/ntc-gkafka/kconsumer"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
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
	name := "worker"
	kc := kconsumer.NewKConsumer(name)
	processChan := make(chan *kafka.Message)
	go kc.Start(processChan)
	// Go-routine to process message.
	go func() {
		for {
			select {
			case e := <-processChan:
				// Process message in here.
				fmt.Printf("##### KConsumer[%s] Message on %s:\n%s\n", kc.Id, e.TopicPartition, string(e.Value))
			}
		}
	}()
	fmt.Printf("KConsumer[%s] start...\n", kc.Id)

	// Hang thread Main.
	c := make(chan os.Signal, 1)
	// We'll accept graceful shutdowns when quit via SIGINT (Ctrl+C) SIGKILL, SIGQUIT or SIGTERM (Ctrl+/) will not be caught.
	signal.Notify(c, os.Interrupt)
	// Block until we receive our signal.
	<-c
	log.Println("################# End Main #################")
}
