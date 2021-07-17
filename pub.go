package main

import (
	"fmt"
	"github.com/congnghia0609/ntc-gconf/nconf"
	"github.com/congnghia0609/ntc-gkafka/kconfig"
	"github.com/congnghia0609/ntc-gkafka/kproducer"
	"path/filepath"
	"runtime"
	"time"
)

func InitNConf3() {
	_, b, _, _ := runtime.Caller(0)
	wdir := filepath.Dir(b)
	fmt.Println("wdir:", wdir)
	nconf.Init(wdir)
}

func main() {
	// Init NConf
	InitNConf3()

	// Consumer
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
