package main

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
	"io"
	"kafka-listener/model"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"
)

var (
	groupID     string
	topic       string
	kafkaBroker string
)

func sendToEventProcessor(trackingEvent model.TrackingEvent) (*model.Event, error) {
	url := fmt.Sprintf("http://%s:%s/send-destination", os.Getenv("SERVER_HOST_LOCAL"), os.Getenv("SERVER_PORT_EVENT_PROCESSOR"))
	method := "POST"

	timestamp := time.Unix(trackingEvent.Event.TimeStamp, 0).Unix()

	payload := strings.NewReader(fmt.Sprintf(`{"store_id": "%s","client_id": "%s","bucket_date": %d,"event_type": "%s","count": %d,"event": {"event_id": "%s","timestamp": %d,"status": "%s"}}`,
		trackingEvent.StoreId, trackingEvent.UserId, trackingEvent.BucketDate, trackingEvent.EventType, trackingEvent.Count,
		trackingEvent.Event.ID, timestamp, trackingEvent.Event.Status))

	client := &http.Client{}
	req, err := http.NewRequest(method, url, payload)

	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
		return nil, err

	}

	var response model.Event
	err = json.Unmarshal(body, &response)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	return &response, nil
}

func main() {
	//err := godotenv.Load("/app/.env") deploy staging
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatal("Error loading .env file")
		return
	}

	kafkaBroker = os.Getenv("KAFKA_BROKER_LOCAL")
	topic = os.Getenv("KAFKA_TOPIC")
	groupID = os.Getenv("KAFKA_GROUP_ID")

	kafkaBrokerServer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaBroker})
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		return
	}
	defer kafkaBrokerServer.Close()

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBroker,
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		fmt.Printf("Failed to create consumer: %s\n", err)
		return
	}
	defer func(c *kafka.Consumer) {
		err := c.Close()
		if err != nil {
			fmt.Printf("Failed to close consumer: %s\n", err)
		}
	}(c)

	// Subscribe to the Kafka topic
	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		fmt.Printf("Failed to subscribe to topic: %s\n", err)
		return
	}

	// Setup a channel to handle OS signals for graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)

	// Start consuming messages
	fmt.Printf("Consuming messages from topic: %s\n", topic)
	run := true
	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Received signal %v: terminating\n", sig)
			run = false
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				// Process the consumed message
				var tracking model.TrackingEvent
				err := json.Unmarshal(e.Value, &tracking)
				if err != nil {
					fmt.Printf("Failed to deserialize message: %s\n", err)
					continue
				}
				fmt.Printf("Received booking: %+v\n", tracking)

				resp, err := sendToEventProcessor(model.TrackingEvent{
					StoreId:    tracking.StoreId,
					UserId:     tracking.UserId,
					BucketDate: tracking.BucketDate,
					EventType:  tracking.EventType,
					Count:      tracking.Count,
					Event:      tracking.Event,
				})
				if err != nil {
					fmt.Printf("Failed to send to event processor: %s\n", err)
					continue
				}
				fmt.Printf("Event processed: %+v\n", resp)
			case kafka.Error:
				// Handle Kafka errors
				fmt.Printf("Error: %v\n", e)

			}
		}
	}
}
