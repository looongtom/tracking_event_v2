package main

import (
	"encoding/json"
	"fmt"
	"github.com/joho/godotenv"
	"log"
	"net/http"
	"os"
	"time"
)

type EventRecord struct {
	ID                string `json:"_id"`
	EventID           string `json:"event_id"`
	ClientID          string `json:"client_id"`
	StoreID           string `json:"store_id"`
	EventType         string `json:"event_type"`
	StatusDestination string `json:"status_destination"`
	Timestamp         int64  `json:"timestamp"`
	BucketDate        string `json:"bucket_date"`
}

func updateEventStatus(w http.ResponseWriter, r *http.Request) {
	var event EventRecord

	// Decode the request body into the event struct
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Update the status of the event
	event.StatusDestination = "updated"

	time.Sleep(500 * time.Millisecond)

	// Encode the updated event back to the response
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(event); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func main() {
	err := godotenv.Load()
	//err := godotenv.Load("/app/.env")
	if err != nil {
		log.Fatal("Error loading .env file")
		return
	}
	http.HandleFunc("/update-event", updateEventStatus)
	fmt.Println(fmt.Sprintf("Server is listening on port %v...", os.Getenv("SERVER_PORT_UPDATE_EVENT")))
	server := &http.Server{
		Addr:              fmt.Sprintf(":%v", os.Getenv("SERVER_PORT_UPDATE_EVENT")),
		ReadHeaderTimeout: 3 * time.Second,
	}
	log.Fatal(server.ListenAndServe())
}
