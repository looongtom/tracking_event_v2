package model

type TrackingRecord struct {
	ID         string  `json:"id"`
	StoreId    string  `json:"store_id"`
	UserId     string  `json:"user_id"`
	BucketDate int64   `json:"bucket_date"`
	EventType  string  `json:"event_type"`
	Count      int     `json:"count"`
	ListEvent  []Event `json:"list_events"`
}

type Event struct {
	ID        string                 `json:"event_id"`
	TimeStamp int64                  `json:"timestamp"`
	Status    map[string]bool        `json:"status_destination"`
	RawData   map[string]interface{} `json:"raw_data"`
}

type TrackingEvent struct {
	UserId     string `json:"user_id"`
	ClientId   string `json:"client_id"`
	BucketDate int64  `json:"bucket_date"`
	EventName  string `json:"event_name"`
	Event      Event  `json:"event"`
}
