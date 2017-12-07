package models

import (
	"github.com/mohae/deepcopy"
	"time"
)

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

// Data object, payload of UnaddedEvent
type EventData struct {
	Message string      `json:"message"`
	Raw     interface{} `json:"raw_data"`
}

// Performs deepcopy
func (e *EventData) Copy() EventData {
	return EventData{
		Message: e.Message,
		Raw:     deepcopy.Copy(e.Raw),
	}
}

type KeyEventPeriod struct {
	RawDataHash string
	StartTime   time.Time
}

type EventDetailsResult struct {
	EventType  string      `json:"event_type"`
	EventName  string      `json:"event_name"`
	ServiceId  int       `json:"service_id"`
	RawData    interface{} `json:"raw_data"`
	RawDetails interface{} `json:"raw_details"`
}

/////////////////////////////////////////////
/* MODELS CORRESPONDING TO DATABASE TABLES */
/////////////////////////////////////////////

type EventBase struct {
	Id                int       `mapstructure:"_id"`
	ServiceId         int       `mapstructure:"service_id"`
	EventType         string      `mapstructure:"event_type"`
	EventName         string      `mapstructure:"event_name"`
	EventGroupId      int       `mapstructure:"event_group_id"`
	ProcessedData     EventData   `mapstructure:"processed_data"`
	ProcessedDataHash string      `mapstructure:"processed_data_hash"`
}

type EventInstance struct {
	Id              int       `mapstructure:"_id"`
	EventBaseId     int       `mapstructure:"event_base_id"`
	EventDetailId   int       `mapstructure:"event_detail_id"`
	RawData         EventData   `mapstructure:"raw_data"`
	GenericData     EventData   `mapstructure:"generic_data"`
	GenericDataHash string      `mapstructure:"generic_data_hash"`

	// ignored fields, used internally
	ProcessedDataHash   string
	ProcessedDetailHash string
}

type EventInstancePeriod struct {
	Id              int                  `mapstructure:"_id"`
	EventInstanceId int                  `mapstructure:"event_instance_id"`
	StartTime       time.Time              `mapstructure:"start_time"`
	EndTime         time.Time              `mapstructure:"end_time"`
	Updated         time.Time              `mapstructure:"updated"`
	Count           int                    `mapstructure:"count"`
	CounterJson     map[string]interface{} `mapstructure:"counter_json"`
	CAS             int                    `mapstructure:"cas_value"`

	// ignored fields, used internally
	RawDataHash string
}

type EventDetail struct {
	Id                  int       `mapstructure:"_id"`
	RawDetail           interface{} `mapstructure:"raw_detail"`
	ProcessedDetail     interface{} `mapstructure:"processed_detail"`
	ProcessedDetailHash string      `mapstructure:"processed_detail_hash"`
}

type EventGroup struct {
	Id                  int       `mapstructure:"_id"`
	Name     string `mapstructure:"name"`
	Info string      `mapstructure:"info"`
}

type EventService struct {
	Id int  `mapstructure:"_id"`
	Name string `mapstructure:"name"`
}
