package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
)

func main() {
	user := struct {
		Name string
		Age  int
	}{}
	config := kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-topic",
		GroupID: "test-group",
	}

	reader := kafka.NewReader(config)

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			break
		}
		e := json.Unmarshal(msg.Value, &user)
		if e != nil {
			fmt.Println(e)
		}
		fmt.Println(user.Name)
		fmt.Printf("message at offset %d: %s = %s\n", msg.Offset, string(msg.Key), string(msg.Value))
	}
}
