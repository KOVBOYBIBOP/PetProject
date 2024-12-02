package main

import (
	"Kafka_3/producer"
	"log"
)

func main() {
	err := producer.StartProducer()
	if err != nil {
		log.Fatalf("Failed to start producer: %v", err)
	}
}
