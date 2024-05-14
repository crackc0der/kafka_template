package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	a := struct {
		Name string
		Age  int
	}{
		Name: "Pavel",
		Age:  34,
	}
	b, e := json.Marshal(a)
	if e != nil {
		fmt.Println(e)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "test-topic",
		Balancer: &kafka.LeastBytes{},
	})
	defer writer.Close()

	for i := 0; i != 10; i++ {
		err := writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte("Text"),
			Value: b,
		})
		if err != nil {
			fmt.Println("Failed to write messages: ", err)
			return
		}

		fmt.Println("Message sent succesfully")
	}
}
