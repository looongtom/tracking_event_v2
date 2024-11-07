package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

type EventRecord struct {
	ID                string `json:"_id" bson:"id"`
	ClientID          string `json:"client_id" bson:"client_id"`
	StoreID           string `json:"store_id" bson:"store_id"`
	EventType         string `json:"event_type" bson:"event_type"`
	StatusDestination string `json:"status_destination" bson:"status_destination"`
	EventID           string `json:"event_id" bson:"event_id"`
	Timestamp         int64  `json:"timestamp" bson:"timestamp"`
	BucketDate        string `json:"bucket_date" bson:"bucket_date"`
}

func updateEvent(trackingEvent EventRecord) (*EventRecord, error) {
	url := fmt.Sprintf("http://%s:%s/update-event", os.Getenv("SERVER_UPDATE_EVENT"), os.Getenv("SERVER_PORT_UPDATE_EVENT"))
	method := "POST"

	payload := strings.NewReader(
		fmt.Sprintf(`{"event_id": "%s","client_id": "%s","store_id": "%s","bucket_date": "%s","event_type": "%s","status_destination": "%s","timestamp": %d}`,
			trackingEvent.EventID, trackingEvent.ClientID, trackingEvent.StoreID, trackingEvent.BucketDate, trackingEvent.EventType, trackingEvent.StatusDestination, trackingEvent.Timestamp))
	fmt.Println("Payload: ", payload)
	client := &http.Client{}
	req, err := http.NewRequest(method, url, payload)

	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		fmt.Println("error call api: ", err)
		return nil, err

	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
		return nil, err

	}

	var response EventRecord
	err = json.Unmarshal(body, &response)
	if err != nil {
		fmt.Println("error unmarshal", err)
		return nil, err
	}
	return &response, nil
}
func sendToDestination(client *mongo.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var trackingEvent EventRecord

		// Decode the request body into the trackingEvent struct
		if err := json.NewDecoder(r.Body).Decode(&trackingEvent); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		fmt.Println(fmt.Sprintf("Tracking Event: %v", trackingEvent))
		updatedEvent, err := updateEvent(trackingEvent)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Println(fmt.Sprintf("URI: %s", fmt.Sprintf("mongodb://%s", os.Getenv("MONGO_URI"))))

		//insert trackingEvent in db

		trackingEvent.StatusDestination = updatedEvent.StatusDestination

		fmt.Println(fmt.Sprintf("Tracking event: %v", trackingEvent))

		db := client.Database(os.Getenv("MONGO_DB"))
		collection := db.Collection(os.Getenv("MONGO_COLLECTION"))

		resp, err := collection.InsertOne(context.Background(), trackingEvent)
		if err != nil {
			log.Printf("Error upserting document: %v", err)
		}
		fmt.Println(fmt.Sprintf("Updated: %d", resp.InsertedID))
		fmt.Println("time: ", time.Now())
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(trackingEvent); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

	}
}

func main() {
	//err := godotenv.Load()
	err := godotenv.Load("/app/.env") //deploy staging
	if err != nil {
		log.Fatal("Error loading .env file")
		return
	}
	clientOptions := options.Client().ApplyURI(fmt.Sprintf("mongodb://%s", os.Getenv("MONGO_URI")))
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer func(client *mongo.Client, ctx context.Context) {
		err := client.Disconnect(ctx)
		if err != nil {
			log.Fatalln(err.Error())
		}
	}(client, context.Background())

	// Check the connection
	err = client.Ping(context.Background(), nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to MongoDB!")

	http.HandleFunc("/send-destination", sendToDestination(client))
	fmt.Println(fmt.Sprintf("Server is listening on port %v...", os.Getenv("SERVER_PORT_EVENT_PROCESSOR")))
	server := &http.Server{
		Addr:              fmt.Sprintf(":%v", os.Getenv("SERVER_PORT_EVENT_PROCESSOR")),
		ReadHeaderTimeout: 3 * time.Second,
	}
	log.Fatal(server.ListenAndServe())
}
