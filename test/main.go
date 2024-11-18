package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"
)

type Event struct {
	ID        string          `json:"event_id"`
	TimeStamp int64           `json:"timestamp"`
	Status    map[string]bool `json:"status_destination"`
	RawData   json.RawMessage `json:"raw_data"`
}

func main() {
	//destinations := []string{"google_analytic", "facebook", "tiktok", "pinterest", "twitter", "snapchat", "klaviyo", "google_ads"}
	//statusDest := make(map[string]bool)
	//
	//// Randomly set each destination status
	//for _, dest := range destinations {
	//	statusDest[dest] = rand.Intn(2) == 1
	//}
	//
	//rawData := map[string]interface{}{
	//	"key1": "value1",
	//	"key2": rand.Float64(),
	//	"key3": rand.Intn(100),
	//}
	////convert rawData to json.Rawdata
	//rawDataJSON, err := json.Marshal(rawData)
	//if err != nil {
	//	fmt.Println("Error marshalling rawData:", err)
	//	return
	//}
	//var rawMessage json.RawMessage = rawDataJSON
	//
	//fmt.Print(Event{
	//	ID:        "id",
	//	TimeStamp: time.Now().Unix(),
	//	Status:    statusDest,
	//	RawData:   rawMessage,
	//})
	for {
		fmt.Println([]string{"purchase", "init_checkout"}[rand.Intn(2)])
		time.Sleep(1 * time.Second)
	}
}
