package main

import (
	"context"
	"event-processor-v2/ws/config"
	"event-processor-v2/ws/connections"
	"event-processor-v2/ws/handlers"
	"fmt"
	"github.com/gorilla/websocket"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"net/http"
	"os"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
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

	fmt.Println(fmt.Sprintf("Setting up server in %v", os.Getenv("SERVER_PORT_EVENT_PROCESSOR")))

	//test
	http.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		conn, _ := upgrader.Upgrade(w, r, nil) // error ignored for sake of simplicity

		for {
			// Read message from browser
			msgType, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}

			// Print the message to the console
			fmt.Printf("%s sent: %s\n", conn.RemoteAddr(), string(msg))

			respMsg := fmt.Sprintf("You sent: %s", string(msg))

			// Write message back to browser
			if err = conn.WriteMessage(msgType, []byte(respMsg)); err != nil {
				return
			}
		}
	})

	http.HandleFunc("/websockets", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "ws/websockets.html")
	})

	//official
	http.HandleFunc("/websockets-display-event", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "/app/ws/websockets2.html")
	})

	ctx := context.Background()

	http.HandleFunc("/socket", handlers.Upgrade)
	go config.Listen(ctx, client, connections.SendMessage)

	http.ListenAndServe(fmt.Sprintf(":%v", os.Getenv("SERVER_PORT_EVENT_PROCESSOR")), nil)
}
