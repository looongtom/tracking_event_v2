package model

import "encoding/json"

type StatusValue struct {
	DestinationName string `json:"destination_name"`
	Status          bool   `json:"status"`
}

type Event struct {
	ID        string          `json:"event_id" bson:"id"`
	TimeStamp int64           `json:"timestamp" bson:"time_stamp"`
	Status    []StatusValue   `json:"status_destination" bson:"status"`
	RawData   json.RawMessage `json:"raw_data" bson:"raw_data"`
}

type EventRecord struct {
	ID          string          `json:"_id" bson:"id"`
	UserId      string          `json:"user_id" bson:"user_id"`
	ClientID    string          `json:"client_id" bson:"client_id"`
	Status      []StatusValue   `json:"status_destination" bson:"status"`
	EventName   string          `json:"event_name" bson:"event_name"`
	Timestamp   int64           `json:"timestamp" bson:"timestamp"`
	RawData     json.RawMessage `json:"raw_data" bson:"raw_data"`
	WsEventName string          `json:"ws_name" bson:"ws_name"`
}
