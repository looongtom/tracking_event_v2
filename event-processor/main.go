package main

import (
	"context"
	"encoding/json"
	"fmt"
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

type EventRecordV3 struct {
	ClientID    string         `json:"client_id"`
	StoreID     string         `json:"store_id"`
	BucketDate  string         `json:"bucket_date"`
	ListSuccess []EventDetails `json:"list_success"`
	ListFailure []EventDetails `json:"list_failure"`
}
type EventRecordRequestV3 struct {
	ClientID    string       `json:"client_id"`
	StoreID     string       `json:"store_id"`
	BucketDate  string       `json:"bucket_date"`
	Status      string       `json:"status"`
	EventDetail EventDetails `json:"event"`
}

type EventDetails struct {
	EventID   string `json:"event_id"`
	Timestamp int64  `json:"timestamp"`
	EventType string `json:"event_type"`
}

func updateEvent(event EventRecordRequestV3) (*EventRecordRequestV3, error) {
	url := fmt.Sprintf("http://%s:%s/update-event", os.Getenv("SERVER_UPDATE_EVENT"), os.Getenv("SERVER_PORT_UPDATE_EVENT"))
	method := "POST"

	payload := strings.NewReader(
		fmt.Sprintf(`{"client_id": "%s","store_id": "%s","bucket_date": "%s","event":{"event_type": "%s","timestamp": %d}}`,
			event.ClientID, event.StoreID, event.BucketDate, event.EventDetail.EventType, event.EventDetail.Timestamp))

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

	var response EventRecordRequestV3
	err = json.Unmarshal(body, &response)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	return &response, nil
}

func sendToDestination(w http.ResponseWriter, r *http.Request) {
	var trackingEvent EventRecordRequestV3

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
		http.Error(w, "Failed to connect to MongoDB", http.StatusInternalServerError)
		return
	}
	fmt.Println("Connected to MongoDB!")

	db := client.Database(os.Getenv("MONGO_DB"))
	collection := db.Collection(os.Getenv("MONGO_COLLECTION"))
	//upsert event in db
	filter := bson.M{
		"store_id":    trackingEvent.StoreID,
		"client_id":   trackingEvent.ClientID,
		"bucket_date": trackingEvent.BucketDate,
	}

	event := EventDetails{
		EventID:   trackingEvent.EventDetail.EventID,
		Timestamp: trackingEvent.EventDetail.Timestamp,
		EventType: trackingEvent.EventDetail.EventType,
	}

	var update bson.M
	if updatedEvent.Status == "success" {
		update = bson.M{
			"$push": bson.M{"list_success": event},
			"$setOnInsert": bson.M{
				"client_id":   trackingEvent.ClientID,
				"store_id":    trackingEvent.StoreID,
				"bucket_date": trackingEvent.BucketDate,
			},
		}
	} else {
		update = bson.M{
			"$push": bson.M{"list_failure": event},
			"$setOnInsert": bson.M{
				"client_id":   trackingEvent.ClientID,
				"store_id":    trackingEvent.StoreID,
				"bucket_date": trackingEvent.BucketDate,
			},
		}
	}

	// Perform the upsert operation
	opts := options.Update().SetUpsert(true)
	resp, err := collection.UpdateOne(context.Background(), filter, update, opts)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Println(fmt.Sprintf("Updated: %d", resp.ModifiedCount))
	fmt.Println("time: ", time.Now())
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(trackingEvent); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

}

func main() {
	//err := godotenv.Load()
	//err := godotenv.Load("/app/.env") //deploy staging
	//if err != nil {
	//	log.Fatal("Error loading .env file")
	//	return
	//}
	http.HandleFunc("/send-destination", sendToDestination)
	fmt.Println(fmt.Sprintf("Server is listening on port %v...", os.Getenv("SERVER_PORT_EVENT_PROCESSOR")))
	server := &http.Server{
		Addr:              fmt.Sprintf(":%v", os.Getenv("SERVER_PORT_EVENT_PROCESSOR")),
		ReadHeaderTimeout: 3 * time.Second,
	}
	log.Fatal(server.ListenAndServe())
}
