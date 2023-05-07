package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
)

func main() {
	// Set up config
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = sarama.V2_3_0_0 // Use a specific Kafka version

	// Create consumer
	consumer, err := sarama.NewConsumer([]string{"kafka.example.com:9092"}, config)
	if err != nil {
		log.Fatalln("Failed to create consumer:", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln("Error closing consumer:", err)
		}
	}()

	// Subscribe to topic
	topic := "GuardianOfGalaxyVol1-topic"
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		log.Fatalln("Failed to get partitions:", err)
	}

	for _, partition := range partitions {
		pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Fatalf("Failed to start consumer for partition %d: %s\n", partition, err)
		}
		defer func() {
			if err := pc.Close(); err != nil {
				log.Fatalln("Error closing partition consumer:", err)
			}
		}()

		// Consume messages
		for msg := range pc.Messages() {
			fmt.Printf("Partition: %d, Offset: %d, Key: %s, Value: %s\n",
				msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
		}
	}
}
