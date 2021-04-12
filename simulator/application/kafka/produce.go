package kafka

import (
	"encoding/json"
	route2 "github.com/williamtrevisan/codelivery/application/route"
	"github.com/williamtrevisan/codelivery/infra/kafka"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"os"
	"time"
)

// Example of a json request:
//{"clientId":"1","routeId":"1"}
//{"clientId":"a","routeId":"1"}
//{"clientId":"2","routeId":"2"}
//{"clientId":"b","routeId":"2"}
//{"clientId":"3","routeId":"3"}
//{"clientId":"c","routeId":"3"}
func Produce(msg *ckafka.Message) {
	producer := kafka.NewKafkaProducer();

	route := route2.NewRoute();

	json.Unmarshal(msg.Value, &route);

	route.LoadPositions();

	positions, err := route.ExportJsonPositions();
	if err != nil {
		log.Println(err.Error());
	}

	for _, p := range positions {
		kafka.Publish(p, os.Getenv("KafkaProduceTopic"), producer);
		time.Sleep(time.Millisecond * 500);
	}
}