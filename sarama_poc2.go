package main

import (
    "fmt"
    "log"

    "github.com/IBM/sarama"
)

func main() {
    brokers := []string{"localhost:9092"}
    topic := "quickstart-events"

    config := sarama.NewConfig()
    config.Consumer.Return.Errors = true
	config.Version, _ = sarama.ParseKafkaVersion("4.2.0")

    // Create consumer
    consumer, err := sarama.NewConsumer(brokers, config)
    if err != nil {
        log.Fatalf("Error creating consumer: %v", err)
    }
    defer consumer.Close()

    // Consume from partition 0, starting at the oldest offset
    partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
    if err != nil {
        log.Fatalf("Error starting partition consumer: %v", err)
    }
    defer partitionConsumer.Close()

    fmt.Println("Reading historic messages…")

    for msg := range partitionConsumer.Messages() {
        fmt.Printf("Offset %d: %s\n", msg.Offset, string(msg.Value))
    }
}
