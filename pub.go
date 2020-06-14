package main

import (
	"fmt"
	"github.com/congnghia0609/ntc-gconf/nconf"
	"ntc-gkafka/kconfig"
	"ntc-gkafka/kproducer"
	"path/filepath"
	"runtime"
	"time"
)

func GetWDir3() string {
	_, b, _, _ := runtime.Caller(0)
	return filepath.Dir(b)
}

func InitNConf3() {
	wdir := GetWDir3()
	fmt.Println("wdir:", wdir)
	nconf.Init(wdir)
}

func main() {
	// Init NConf
	InitNConf3()

	// Consumer
	//kconsumer.Start()

	// Producer
	//kproducer.Start()

	name := "worker"
	topic := kconfig.GetProduceTopic(name, "streams-plaintext-input") // streams-plaintext-input

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("Key_%d", i)
		msg := fmt.Sprintf("This is message %d", i)
		fmt.Printf("Preparing to produce record: %s\n", msg)
		kproducer.SendRecordKV(name, topic, key, msg)
	}

	time.Sleep(3 * time.Second)
	fmt.Printf("10 messages were produced to topic: %s!\n", topic)
}