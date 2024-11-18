package config

import (
	"context"
	"encoding/json"
	"event-processor-v2/model"
	"fmt"
	kafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/mongo"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"
)

var kafkaBroker string
var topic string
var groupID string
var kafkaBrokerServer *kafka.Producer
var kafkaConsumer *kafka.Consumer

func updateEvent(event model.Event) (*model.Event, error) {
	url := fmt.Sprintf("http://%s:%s/update-event", os.Getenv("SERVER_UPDATE_EVENT"), os.Getenv("SERVER_PORT_UPDATE_EVENT"))
	method := "POST"

	timestamp := time.Unix(event.TimeStamp, 0).Unix()

	payload := strings.NewReader(fmt.Sprintf(`{"event_id": "%s", "timestamp": %d, "status": "%s"}`, event.ID, timestamp, event.Status))

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

func saveInDb(ctx context.Context, client *mongo.Client, tracking model.EventRecord) error {
	db := client.Database(os.Getenv("MONGO_DB"))
	collection := db.Collection(os.Getenv("MONGO_COLLECTION"))
	//insert trackingEvent in db
	resp, err := collection.InsertOne(ctx, tracking)
	if err != nil {
		log.Printf("Error upserting document: %v", err)
		return err
	}
	fmt.Println(fmt.Sprintf("Updated: %d", resp.InsertedID))
	fmt.Println("time: ", time.Now())
	return nil
}

func Listen(ctx context.Context, client *mongo.Client, cb func(msg string)) {
	//err := godotenv.Load()
	//envPath := filepath.Join("..", ".env")
	err := godotenv.Load("/app/.env")
	if err != nil {
		log.Fatal("Error loading .env file")
		return
	}
	kafkaBroker = os.Getenv("KAFKA_BROKER")
	topic = os.Getenv("KAFKA_TOPIC")
	groupID = os.Getenv("KAFKA_GROUP_ID")

	fmt.Println("Kafka Broker: ", kafkaBroker)
	fmt.Println("Kafka Topic: ", topic)
	fmt.Println("Kafka Group ID: ", groupID)

	kafkaBrokerServer, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaBroker})
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		return
	}
	defer kafkaBrokerServer.Close()

	kafkaConsumer, err = kafka.NewConsumer(&kafka.ConfigMap{
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
	}(kafkaConsumer)
	// Subscribe to the Kafka topic
	err = kafkaConsumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		fmt.Printf("Failed to subscribe to topic: %s\n", err)
		return
	}

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
			ev := kafkaConsumer.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				// Process the consumed message
				var tracking model.EventRecord
				err := json.Unmarshal(e.Value, &tracking)
				if err != nil {
					fmt.Printf("Failed to deserialize message: %s\n", err)
					continue
				}
				fmt.Println("Receive from kafka: ", tracking)

				//updatedEvent, err := updateEvent(tracking.Event)
				//if err != nil {
				//	fmt.Println("error call destination: ", err)
				//	continue
				//}
				//tracking.Event.Status = *updatedEvent

				//delay 2s
				//read DELAY_TIME from .env
				delayTime, err := time.ParseDuration(os.Getenv("DELAY_TIME"))
				if err != nil {
					log.Println("Error parsing delay time")
					delayTime = 500
				}

				time.Sleep(delayTime * time.Millisecond)

				serializedTracking, err := json.Marshal(tracking)
				if err != nil {
					fmt.Printf("Failed to marshal message: %s\n", err)
					continue
				}
				fmt.Println("Serialized JSON data:", string(serializedTracking))

				cb(string(serializedTracking))

				err = saveInDb(ctx, client, tracking)
				if err != nil {
					fmt.Println("Error saving to db: ", err)
					continue
				}

			case kafka.Error:
				// Handle Kafka errors
				fmt.Printf("Error: %v\n", e)

			}
		}
	}
}
