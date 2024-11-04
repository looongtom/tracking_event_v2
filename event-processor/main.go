package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

type Event struct {
	ID        string `json:"event_id"`
	TimeStamp int64  `json:"timestamp"`
	Status    string `json:"status"`
}

type TrackingEvent struct {
	StoreId    string `json:"store_id"`
	UserId     string `json:"client_id"`
	BucketDate int64  `json:"bucket_date"`
	EventType  string `json:"event_type"`
	Count      int    `json:"count"`
	Event      Event  `json:"event"`
}

func updateEvent(event Event) (*Event, error) {
	url := fmt.Sprintf("http://%s:%s/update-event", os.Getenv("SERVER_HOST_LOCAL"), os.Getenv("SERVER_PORT_UPDATE_EVENT"))
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

	var response Event
	err = json.Unmarshal(body, &response)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	return &response, nil
}

func sendToDestination(w http.ResponseWriter, r *http.Request) {
	var trackingEvent TrackingEvent

	// Decode the request body into the trackingEvent struct
	if err := json.NewDecoder(r.Body).Decode(&trackingEvent); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	updatedEvent, err := updateEvent(trackingEvent.Event)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	clientOptions := options.Client().ApplyURI(fmt.Sprintf("mongodb://%s", os.Getenv("MONGO_URI_LOCAL")))
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

	db := client.Database(os.Getenv("MONGO_DB"))
	collection := db.Collection(os.Getenv("MONGO_COLLECTION"))
	//insert trackingEvent in db
	filter := bson.M{
		"store_id":    trackingEvent.StoreId,
		"client_id":   trackingEvent.UserId,
		"bucket_date": trackingEvent.BucketDate,
		"event_type":  trackingEvent.EventType,
	}
	update := bson.M{
		"$setOnInsert": bson.M{
			"store_id":    trackingEvent.StoreId,
			"client_id":   trackingEvent.UserId,
			"bucket_date": trackingEvent.BucketDate,
			"event_type":  trackingEvent.EventType,
		},
		"$inc": bson.M{
			"count": trackingEvent.Count, // Increment the count field by 1
		},
		"$push": bson.M{
			"list_event": bson.M{
				"event_id":           updatedEvent.ID,
				"timestamp":          updatedEvent.TimeStamp,
				"status_destination": updatedEvent.Status,
			},
		},
	}
	opts := options.Update().SetUpsert(true)
	resp, err := collection.UpdateOne(context.Background(), filter, update, opts)
	if err != nil {
		log.Printf("Error upserting document: %v", err)
	}
	_, err = fmt.Fprintf(w, "Updated %v documents", resp.ModifiedCount)
	if err != nil {
		return
	}

}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
		return
	}
	http.HandleFunc("/send-destination", sendToDestination)
	fmt.Println(fmt.Sprintf("Server is listening on port %v...", os.Getenv("SERVER_PORT_EVENT_PROCESSOR")))
	server := &http.Server{
		Addr:              fmt.Sprintf(":%v", os.Getenv("SERVER_PORT_EVENT_PROCESSOR")),
		ReadHeaderTimeout: 3 * time.Second,
	}
	log.Fatal(server.ListenAndServe())
}
