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

	var response Event
	err = json.Unmarshal(body, &response)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	return &response, nil
}

func sendToDestination(client *mongo.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var trackingEvent TrackingEvent

		// Decode the request body into the trackingEvent struct
		if err := json.NewDecoder(r.Body).Decode(&trackingEvent); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		fmt.Println(fmt.Sprintf("Tracking Event: %v", trackingEvent))
		updatedEvent, err := updateEvent(trackingEvent.Event)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

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
		fmt.Println(fmt.Sprintf("Filter: %v", filter))
		opts := options.Update().SetUpsert(true)
		resp, err := collection.UpdateOne(context.Background(), filter, update, opts)
		if err != nil {
			log.Printf("Error upserting document: %v", err)
		}
		fmt.Println(fmt.Sprintf("Updated: %d", resp.ModifiedCount))
		fmt.Println("time: ", time.Now())
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(trackingEvent); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

	}
}

func checkAndCreateCollection(client *mongo.Client, dbName, collectionName string) (bool, error) {
	// List collections in the database
	db := client.Database(dbName)
	collections, err := db.ListCollectionNames(context.TODO(), map[string]interface{}{})
	if err != nil {
		return false, fmt.Errorf("failed to list collections: %w", err)
	}

	// Check if the collection already exists
	for _, col := range collections {
		if col == collectionName {
			return true, nil
		}
	}

	// Create the collection if it doesn't exist
	err = db.CreateCollection(context.TODO(), collectionName)
	if err != nil {
		return false, fmt.Errorf("failed to create collection: %w", err)
	}

	return false, nil
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

	dbName := os.Getenv("MONGO_DB")
	collectionName := os.Getenv("MONGO_COLLECTION")

	collectionExists, err := checkAndCreateCollection(client, dbName, collectionName)
	if err != nil {
		log.Fatalf("Failed to check or create collection: %s", err)
	}

	if collectionExists {
		fmt.Printf("Collection %s already exists in database %s\n", collectionName, dbName)
	} else {
		fmt.Printf("Collection %s created in database %s\n", collectionName, dbName)
	}

	http.HandleFunc("/send-destination", sendToDestination(client))
	fmt.Println(fmt.Sprintf("Server is listening on port %v...", os.Getenv("SERVER_PORT_EVENT_PROCESSOR")))
	server := &http.Server{
		Addr:              fmt.Sprintf(":%v", os.Getenv("SERVER_PORT_EVENT_PROCESSOR")),
		ReadHeaderTimeout: 3 * time.Second,
	}
	log.Fatal(server.ListenAndServe())
}
