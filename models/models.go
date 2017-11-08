package models

import "time"


////////////////////////////////////////////////////
/* MODELS CORRESPONDING TO EVENTS AND API RESULTS */
////////////////////////////////////////////////////

// UnaddedEvent is the first event that is sent to the server
type UnaddedEvent struct {
	ServiceId             int                    `json:"service_id"`
	Name                  string                 `json:"event_name"`
	Type                  string                 `json:"event_type"`
	Data                  EventData              `json:"event_data"`
	ExtraArgs             map[string]interface{} `json:"extra_args"`
	Timestamp             string                 `json:"timestamp"`
	ConfigurableFilters   map[string][]string    `json:"configurable_filters"`
	ConfigurableGroupings []string               `json:"configurable_groupings"`
}

// Data object, payload of unaddedEvent
type EventData struct {
	Message string      `json:"message"`
	Raw     interface{} `json:"raw_data"`
}

type KeyEventPeriod struct {
	RawDataHash string
	StartTime   time.Time
}

// recent events
type EventRecentResult struct {
	Id            int64       `json:"id"`
	EventType     string      `json:"event_type"`
	EventName     string      `json:"event_name"`
	Count         int         `json:"count"`
	LastUpdated   time.Time   `json:"last_updated"`
	ProcessedData interface{} `json:"processed_data"`
	InstanceIds   []int64     `json:"instance_ids"`
}

// histogram of an event
type EventHistogramResult struct {
	StartTime   time.Time              `json:"start_time"`
	EndTime     time.Time              `json:"end_time"`
	Count       int                    `json:"count"`
	CounterJson map[string]interface{} `json:"count_json"`
}

type EventDetailsResult struct {
	EventType  string      `json:"event_type"`
	EventName  string      `json:"event_name"`
	ServiceId  int         `json:"service_id"`
	RawData    interface{} `json:"raw_data"`
	RawDetails interface{} `json:"raw_details"`
}



/////////////////////////////////////////////
/* MODELS CORRESPONDING TO DATABASE TABLES */
/////////////////////////////////////////////


type EventBase struct {
	Id                int64       `mapstructure:"_id"`
	ServiceId         int         `mapstructure:"service_id"`
	EventType         string      `mapstructure:"event_type"`
	EventName         string      `mapstructure:"event_name"`
	ProcessedData     interface{} `mapstructure:"processed_data"`
	ProcessedDataHash string      `mapstructure:"processed_data_hash"`
}

type EventInstance struct {
	Id            int64       `mapstructure:"_id"`
	EventBaseId   int64       `mapstructure:"event_base_id"`
	EventDetailId int64       `mapstructure:"event_detail_id"`
	RawData       interface{} `mapstructure:"raw_data"`
	RawDataHash   string      `mapstructure:"raw_data_hash"`

	// ignored fields, used internally
	ProcessedDataHash   string
	ProcessedDetailHash string
}

type EventInstancePeriod struct {
	Id              int64                  `mapstructure:"_id"`
	EventInstanceId int64                  `mapstructure:"event_instance_id"`
	StartTime       time.Time              `mapstructure:"start_time"`
	EndTime         time.Time              `mapstructure:"end_time"`
	Updated         time.Time              `mapstructure:"updated"`
	Count           int                    `mapstructure:"count"`
	CounterJson     map[string]interface{} `mapstructure:"counter_json"`
	CAS int `mapstructure:"cas_value"`

	// ignored fields, used internally
	RawDataHash         string
}

type EventDetail struct {
	Id                  int64       `mapstructure:"_id"`
	RawDetail           interface{} `mapstructure:"raw_detail"`
	ProcessedDetail     interface{} `mapstructure:"processed_detail"`
	ProcessedDetailHash string      `mapstructure:"processed_detail_hash"`
}