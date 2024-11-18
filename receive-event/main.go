package main

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
	"log"
	"math/rand"
	"net/http"
	"os"
	"receive-event/model"
	"strconv"
	"sync"
	"time"
)

var (
	kafkaBroker string
	topic       string
)

func handleMain(w http.ResponseWriter, r *http.Request) {
	// log current time
	fmt.Println("start time:", time.Now())

	// Create a new Kafka producer
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaBroker})
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to create producer: %s", err), http.StatusInternalServerError)
		return
	}
	defer p.Close()

	totalClient := os.Getenv("TOTAL_CLIENT")
	totalClientValue, _ := strconv.Atoi(totalClient)
	totalEventType := os.Getenv("TOTAL_EVENT_TYPE")
	totalEventTypeValue, _ := strconv.Atoi(totalEventType)
	maxEvent, _ := strconv.Atoi(os.Getenv("MAX_AMOUNT_EVENT"))
	userID := os.Getenv("USER_ID")

	err = generateMockData(totalEventTypeValue, maxEvent, totalClientValue, userID, time.Now(), p)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to generate mock data: %s", err), http.StatusInternalServerError)
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Main function executed successfully"))
}

func generateRandomStatus(destinations []string) []model.StatusValue {
	// Randomize the number of destinations (at least 1)
	numDestinations := rand.Intn(len(destinations)) + 1
	status := make([]model.StatusValue, numDestinations)

	// Shuffle the destinations and pick the first few
	rand.Shuffle(len(destinations), func(i, j int) {
		destinations[i], destinations[j] = destinations[j], destinations[i]
	})

	for i := 0; i < numDestinations; i++ {
		status[i] = model.StatusValue{
			DestinationName: destinations[i],
			Status:          rand.Intn(2) == 1, // Randomly true or false
		}
	}

	return status
}

func generateMockEvent() []byte {
	rawData := map[string]interface{}{
		"key1":              "value1",
		"key2":              rand.Float64(),
		"key3":              rand.Intn(100),
		"page_url":          "https://example.com/product/123",
		"referrer":          "https://google.com",
		"timestamp":         "2024-11-14T15:30:00Z",
		"user_agent":        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36",
		"screen_resolution": "1920x1080",
		"browser_language":  "en-US",
		"last_touch":        "last_touch",
		"order_value":       42,
	}

	rawDataBytes, err := json.Marshal(rawData)
	if err != nil {
		log.Fatalf("Error serializing raw data: %v", err)
	}

	return rawDataBytes
}

//func handleMainV2(w http.ResponseWriter, r *http.Request) {
//	// log current time
//	fmt.Println("start time:", time.Now())
//
//	// Create a new Kafka producer
//	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaBroker})
//	if err != nil {
//		http.Error(w, fmt.Sprintf("Failed to create producer: %s", err), http.StatusInternalServerError)
//		return
//	}
//	defer p.Close()
//
//	totalStore := os.Getenv("TOTAL_STORE")
//	totalStoreValue, _ := strconv.Atoi(totalStore)
//	totalClient := os.Getenv("TOTAL_CLIENT")
//	totalClientValue, _ := strconv.Atoi(totalClient)
//	totalEventType := os.Getenv("TOTAL_EVENT_TYPE")
//	totalEventTypeValue, _ := strconv.Atoi(totalEventType)
//	maxEvent, _ := strconv.Atoi(os.Getenv("MAX_AMOUNT_EVENT"))
//
//	err = generateMockData( totalEventTypeValue, maxEvent, totalClientValue, time.Now(), p)
//
//	if err != nil {
//		http.Error(w, fmt.Sprintf("Failed to generate mock data: %s", err), http.StatusInternalServerError)
//	}
//
//	w.WriteHeader(http.StatusOK)
//	w.Write([]byte("Main function executed successfully"))
//}

func generateMockData(mEventTypes, mEvents, nClients int, userId string, bucketDate time.Time, p *kafka.Producer) error {
	destinations := []string{"google_analytic", "facebook", "tiktok", "pinterest", "twitter", "snapchat", "klaviyo", "google_ads"}

	clientPrefix := "client"
	eventTypes := make([]string, mEventTypes)
	for i := 0; i < mEventTypes; i++ {
		eventTypes[i] = fmt.Sprintf("event_type%d", i+1)
	}

	var wg sync.WaitGroup

	errChan := make(chan error, mEvents)

	for i := 0; i < mEvents; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			indexClient := rand.Intn(nClients) + 1
			clientID := fmt.Sprintf("%s%d", clientPrefix, indexClient)
			timestamp := bucketDate.Add(time.Duration(rand.Intn(24)) * time.Hour).Add(time.Duration(rand.Intn(60)) * time.Minute)

			sentEvent := model.EventRecord{
				UserId:      userId,
				ClientID:    clientID,
				Status:      generateRandomStatus(destinations),
				EventName:   []string{"purchase", "init_checkout"}[rand.Intn(2)],
				Timestamp:   timestamp.Unix(),
				RawData:     generateMockEvent(),
				WsEventName: fmt.Sprintf("realtime_dashboard/%s", userId),
			}
			serializedBookingRequest, err := json.Marshal(sentEvent)
			if err != nil {
				//http.Error(w, fmt.Sprintf("Failed to serialize booking request: %s", err), http.StatusInternalServerError)
				errChan <- fmt.Errorf("Failed to serialize booking request: %s", err)
				return
			}

			// Produce the message to the Kafka topic
			err = produceMessage(p, topic, serializedBookingRequest)
			if err != nil {
				//http.Error(w, fmt.Sprintf("Failed to produce message: %s", err), http.StatusInternalServerError)
				errChan <- fmt.Errorf("Failed to produce message: %s", err)
				return
			}

			errChan <- nil
		}(i)
	}
	wg.Wait()
	close(errChan)
	for err := range errChan {
		if err != nil {
			return err
		}
	}
	fmt.Println("===========================Message produced successfully!=============================")
	return nil
}

func main() {
	err := godotenv.Load("/app/.env")
	//err := godotenv.Load(".env")
	if err != nil {
		log.Fatal("Error loading .env file")
		return
	}
	kafkaBroker = os.Getenv("KAFKA_BROKER")
	topic = os.Getenv("KAFKA_TOPIC")

	http.HandleFunc("/receive-event", handleMain)
	fmt.Println(fmt.Sprintf("Server is listening on port %v...", os.Getenv("SERVER_PORT_RECEIVE_EVENT")))
	server := &http.Server{
		Addr:              fmt.Sprintf(":%v", os.Getenv("SERVER_PORT_RECEIVE_EVENT")),
		ReadHeaderTimeout: 3 * time.Second,
	}
	log.Fatal(server.ListenAndServe())
}

func produceMessage(p *kafka.Producer, topic string, message []byte) error {
	// Create a new Kafka message to be produced
	kafkaMessage := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          message,
	}
	// Produce the Kafka message
	deliveryChan := make(chan kafka.Event)
	err := p.Produce(kafkaMessage, deliveryChan)
	if err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}
	// Wait for delivery report or error
	e := <-deliveryChan
	m := e.(*kafka.Message)
	// Check for delivery errors
	if m.TopicPartition.Error != nil {
		return fmt.Errorf("delivery failed: %s", m.TopicPartition.Error)
	}
	// Close the delivery channel
	close(deliveryChan)
	return nil
}

// Helper function to generate random bucket dates
func generateRandomBucketDates(numDates int) []time.Time {
	var dates []time.Time
	baseDate, _ := time.Parse(time.RFC3339, "2024-10-01T00:00:00Z")
	for i := 0; i < numDates; i++ {
		dates = append(dates, baseDate.Add(time.Duration(i)*24*time.Hour))
	}
	return dates
}

// Helper function to generate random status
func randomStatus() string {
	statuses := []string{"success", "failed"}
	return statuses[rand.Intn(len(statuses))]
}
