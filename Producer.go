package main

import (
	"fmt"
	"log"

	"github.com/Shopify/sarama"
)

func main() {
	brokerList := []string{"20.25.91.229:9092"} // Replace with your Kafka broker's address
	topic := "GuardianOfGalaxyVol1-topic"       // Replace with the topic you want to produce to

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		fmt.Println("Failed to create producer:", err)
		return
	}

	defer producer.Close()

	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalf("Failed to close producer: %v", err)
		}
	}()

	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder("Hello, World!"), // Replace with the message you want to produce
	}

	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		log.Fatalf("Failed to produce message: %v", err)
	}

	fmt.Printf("Message sent successfully! Partition: %d, Offset: %d\n", partition, offset)
}
