package util

import (
	"github.com/nsqio/go-nsq"
	"fmt"
)

func CreateNSQConsumer(topicName, channelName string, lookupAddressList []string,
	messageHandler nsq.Handler) *nsq.Consumer {
	configObj := nsq.NewConfig()
	consumerInstance, err := nsq.NewConsumer(topicName, channelName, configObj)
	if err != nil {
		content := fmt.Sprintf("Create  NSQ  Consumer  err:%s topic:%s channel:%s",
			err, topicName, channelName)
		panic(content)
	}
	consumerInstance.AddHandler(messageHandler)
	consumerInstance.ChangeMaxInFlight(1000)
	if err := consumerInstance.ConnectToNSQLookupds(lookupAddressList); err != nil {
		content := fmt.Sprintf("connectToNSQLookupds err:%s topic:%s channel:%s",
			err, topicName, channelName)
		panic(content)
	}
	return consumerInstance
}

func CreateProducer(nsqdAddress string) *nsq.Producer {
	config := nsq.NewConfig()
	producer, err := nsq.NewProducer(nsqdAddress, config)
	if err != nil {
		fmt.Println("CreateProducer  Error...", nsqdAddress, err, producer)
	} else {
		producer.Ping()
	}
	return producer
}
