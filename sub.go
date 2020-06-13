package main

import (
	"fmt"
	"github.com/congnghia0609/ntc-gconf/nconf"
	"ntc-gkafka/kconsumer"
	"path/filepath"
	"runtime"
)

func GetWDir2() string {
	_, b, _, _ := runtime.Caller(0)
	return filepath.Dir(b)
}

func InitNConf2() {
	wdir := GetWDir2()
	fmt.Println("wdir:", wdir)
	nconf.Init(wdir)
}

func main() {
	// Init NConf
	InitNConf2()

	// Consumer
	kconsumer.Start()

	// Producer
	//kproducer.Start()
}