package main

import (
	"fmt"
	kafka2 "github.com/williamtrevisan/codelivery/application/kafka"
	"github.com/williamtrevisan/codelivery/infra/kafka"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
	"log"
)

func init() {
	err := godotenv.Load();
	if err != nil {
		log.Fatal("Error loading .env file");
	}
}

func main() {
	msgChan := make(chan *ckafka.Message);

	consumer := kafka.NewKafkaConsumer(msgChan);

	go consumer.Consume();

	for msg := range msgChan {
		fmt.Println(string(msg.Value));
		go kafka2.Produce(msg);
	}
}