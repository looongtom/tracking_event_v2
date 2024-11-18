package model

import "encoding/json"

type TrackingRecord struct {
	ID         string  `json:"id"`
	StoreId    string  `json:"store_id"`
	UserId     string  `json:"user_id"`
	BucketDate int64   `json:"bucket_date"`
	EventType  string  `json:"event_type"`
	Count      int     `json:"count"`
	ListEvent  []Event `json:"list_events"`
}

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

type TrackingEvent struct {
	WsEventName string `json:"ws_event_name" bson:"ws_event_name"`
	UserId      string `json:"user_id" bson:"user_id"`
	ClientId    string `json:"client_id" bson:"client_id"`
	BucketDate  int64  `json:"bucket_date" bson:"bucket_date"`
	EventName   string `json:"event_name" bson:"event_name"`
	Event       Event  `json:"event" bson:"event"`
}
