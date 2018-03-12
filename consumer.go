package main

import (
	"fmt"

	"github.com/Shopify/sarama"
)

func subscribe(topic string, consumer sarama.Consumer, producer sarama.SyncProducer) {
	partitionList, err := consumer.Partitions(topic) // get all partitions
	if err != nil {
		fmt.Println("Error retrieving partitionList ", err)
	}

	initialOffset := sarama.OffsetOldest // get offset for the oldest member

	for _, partition := range partitionList {
		pc, _ := consumer.ConsumePartition(topic, partition, initialOffset)

		go func(pc sarama.PartitionConsumer) {
			for message := range pc.Messages() {
				messageReceived(message)

				msg := massageMessage(message)
				partition, offset, err := producer.SendMessage(msg)
				if err != nil {
					fmt.Printf("%s error occured.", err.Error())
				} else {
					fmt.Printf("Message was saved to partion: %d.\nMessage offset is: %d.\n", partition, offset)
				}
			}
		}(pc)
	}
}

func messageReceived(message *sarama.ConsumerMessage) {
	saveMessage(string(message.Value))
}

func massageMessage(message *sarama.ConsumerMessage) *sarama.ProducerMessage {
	val := sarama.StringEncoder(message.Value) + " : Modified by your local Golang Gopher"
	msg := &sarama.ProducerMessage{
		Topic:     "ALL",
		Partition: -1,
		Value:     sarama.StringEncoder(val),
	}

	return msg
}
