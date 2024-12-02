package main

import (
	"Kafka_3/consumer"
	"log"
)

func main() {
	err := consumer.StartConsumer()
	if err != nil {
		log.Fatalf("Failed to start consumer: %v", err)
	}
}
