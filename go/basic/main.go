package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	DEAFULT_KAFKA_ADDRESS   = "localhost"
	DEFAULT_PARTITION       = 0
	DEFAULT_DEADLINE_SECOND = time.Second * 10
)

type Kafka struct {
}

func main() {
	go producer()
	go consumer()
	time.Sleep(30 * time.Second)
}

func producer() {
	topic := "my-topic"
	partition := 0

	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	defer conn.Close()

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	index := 0
	for {
		message := fmt.Sprintf("[%d] producer message ", index)
		_, err = conn.WriteMessages(
			kafka.Message{Value: []byte(message)},
		)
		if err != nil {
			log.Fatal(err.Error())
		}
		index += 1
		time.Sleep(time.Second * 1)
	}
}

func consumer() {
	topic := "my-topic"
	partition := 0

	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	defer conn.Close()

	// conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	batch := conn.ReadBatch(10e3, 1e6) // fetch 10KB min, 1MB max

	defer batch.Close()

	b := make([]byte, 10e3) // 10KB max per message
	for {
		_, err := batch.Read(b)
		if err != nil {
			break
		}
		fmt.Println(string(b))
	}
}
