package main

import (
	"fmt"
	"github.com/congnghia0609/ntc-gconf/nconf"
	"ntc-gkafka/kproducer"
	"path/filepath"
	"runtime"
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
	kproducer.Start()
}
