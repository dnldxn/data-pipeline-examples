package main

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

// Kafka topic and broker address are initialized as constants
const (
	brokerAddress = "localhost:9092"
	ivrTopic      = "ivr-csv"
	civTopic      = "agent-csv"
	deviceTopic   = "device-csv"
	letters       = "abcdefghijklmnopqrstuvwxyz"
)

func main() {

	// create a new context
	ctx := context.Background()

	//
	go produce_ivr_and_civ(ctx)
	produce_device(ctx)
}

func generate_random_activity() string {
	num_activities := rand.Intn(10) // random integer
	for i := range num_activities {
		activity := make([]rune, 3)
		for j := range activity {
			activity[j] = letters[rand.Intn(len(letters))]
		}
	}
	return "abc"
}

func produce_ivr_and_civ(ctx context.Context) {

	// intialize the writer with the broker addresses, and the topic
	ivr_writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{brokerAddress},
		Topic:   ivrTopic,
	})

	civ_writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{brokerAddress},
		Topic:   civTopic,
	})

	// initialize a counter
	i := 0

	for {

		// Create the Kafka message (comma delimited)
		currentTime := time.Now().Format("2006-01-02 15:04:05")
		custID := rand.Intn(50) // generate random customer number between 1 and 100
		msg := fmt.Sprintf("%s,%d,ghi|def", currentTime, custID)

		// Emit the IVR event to the queue
		err := ivr_writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(msg),
		})

		// Handle error
		if err != nil {
			panic("could not write message " + err.Error())
		}

		// Emit the Agent event to the queue
		err = civ_writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(msg),
		})

		// Handle error
		if err != nil {
			panic("could not write message " + err.Error())
		}

		i++

		// sleep for some time
		n := rand.Intn(5) // random integer between 1 and 10
		time.Sleep(time.Duration(n) * time.Second)
	}
}

func produce_device(ctx context.Context) {

	// intialize the writer with the broker addresses, and the topic
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{brokerAddress},
		Topic:   deviceTopic,
	})

	// initialize a counter
	i := 0

	for {

		// Create the Kafka message (delimited)
		currentTime := time.Now().Format("2006-01-02 15:04:05")
		custID := rand.Intn(50) // generate random customer number between 1 and 100
		msg := fmt.Sprintf("%s,%d,ghi|def", currentTime, custID)

		// Write the message to the queue
		err := writer.WriteMessages(ctx, kafka.Message{
			Key: []byte(strconv.Itoa(i)),

			// create an arbitrary message payload for the value
			Value: []byte(msg),
		})

		// Handle error
		if err != nil {
			panic("could not write message " + err.Error())
		}

		i++

		// sleep for some time
		n := rand.Intn(5) // random integer between 1 and 100
		time.Sleep(time.Duration(n) * time.Second)
	}
}
