# ntc-gkafka
ntc-gkafka is a module golang kafka client.

## Install
```bash
go get -u github.com/congnghia0609/ntc-gkafka
```

## Usage
```go
func InitNConf() {
	_, b, _, _ := runtime.Caller(0)
	wdir := filepath.Dir(b)
	fmt.Println("wdir:", wdir)
	nconf.Init(wdir)
}

func main() {
	// Init NConf
	InitNConf()

	// Consumer
	StartSimpleConsumer()

	time.Sleep(2 * time.Second)

	// Producer
	//StartSimpleProducer()
	StartSimpleProducer2()


	// Hang thread Main.
	c := make(chan os.Signal, 1)
	// We'll accept graceful shutdowns when quit via SIGINT (Ctrl+C) SIGKILL, SIGQUIT or SIGTERM (Ctrl+/) will not be caught.
	signal.Notify(c, os.Interrupt)
	// Block until we receive our signal.
	<-c
	log.Println("################# End Main #################")
}

func StartSimpleConsumer() {
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
				fmt.Printf("##### Sub[%s] Message on %s:\n%s\n", kc.GetId(), e.TopicPartition, string(e.Value))
			}
		}
	}()
	fmt.Printf("SimpleConsumer[%s] start...\n", kc.GetId())
}

func StartSimpleProducer() {
	name := "worker"
	topic := kconfig.GetProduceTopic(name, "streams-plaintext-input") // streams-plaintext-input
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("Key_%d", i)
		msg := fmt.Sprintf("This is message %d", i)
		fmt.Printf("Preparing to produce record: %s\n", msg)
		kproducer.SendRecordKV(name, topic, key, msg)
	}
	fmt.Printf("SimpleProducer has completely produced to topic: %s!\n", topic)
}

func StartSimpleProducer2() {
	name := "worker"
	topic := kconfig.GetProduceTopic(name, "streams-plaintext-input") // streams-plaintext-input
	kp := kproducer.GetInstance(name)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("Key_%d", i)
		msg := fmt.Sprintf("This is message %d", i)
		fmt.Printf("Producer[%s] preparing to produce record: %s\n", kp.GetId(), msg)
		kp.SendRecordKV(topic, key, msg)
	}
	fmt.Printf("SimpleProducer[%s] has completely produced to topic: %s!\n", kp.GetId(), topic)
}
```

## License
This code is under the [Apache License v2](https://www.apache.org/licenses/LICENSE-2.0).
