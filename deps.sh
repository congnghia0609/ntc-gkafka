#!/bin/bash
# Author:       nghiatc
# Email:        congnghia0609@gmail.com

#source /etc/profile

echo "Install library dependencies..."

go get -u github.com/tools/godep
go get -u github.com/congnghia0609/ntc-gconf
go get -u gopkg.in/confluentinc/confluent-kafka-go.v1/kafka
go get -u github.com/natefinch/lumberjack
go get -u github.com/google/uuid

echo "Install dependencies complete..."