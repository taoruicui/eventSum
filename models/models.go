package models

import (
	"github.com/mohae/deepcopy"
	"time"
	"sort"
	"fmt"
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

// Data object, payload of unaddedEvent
type EventData struct {
	Message string      `json:"message"`
	Raw     interface{} `json:"raw_data"`
}

// Performs deepcopy
func (e *EventData) Copy() EventData {
	return EventData{
		Message: e.Message,
		Raw: deepcopy.Copy(e.Raw),
	}
}

type KeyEventPeriod struct {
	RawDataHash string
	StartTime   time.Time
}

// Represents a bin in a histogram
type Bin struct {
	Count int `json:"count"`
	Start int `json:"start"`
}

// Represents a map of bins, where the key is the start time
type EventBins map[int]*Bin

// return the bins sorted in chronological order
func (e EventBins) ToSlice() []Bin {
	keys := []int{}
	res := []Bin{}
	for i := range e {
		keys = append(keys, i)
	}
	sort.Ints(keys)
	for _, k := range keys {
		res = append(res, *e[k])
	}
	return res
}

// Recent events
type EventResults []EventResult

// used for sort function
func (e EventResults) Len() int {
	return len(e)
}

// used for sort function
func (e EventResults) Swap (i, j int) {
	e[i], e[j] = e[j], e[i]
}

// sort from greatest to smallest
func (e EventResults) Less (i, j int) bool {
	return e[i].TotalCount > e[j].TotalCount
}

// Base event with histogram of occurrences
type EventResult struct {
	Id            int64       `json:"id"`
	EventType     string      `json:"event_type"`
	EventName     string      `json:"event_name"`
	TotalCount    int         `json:"total_count"`
	ProcessedData interface{} `json:"processed_data"`
	InstanceIds   []int64     `json:"instance_ids"`
	Datapoints    EventBins  `json:"datapoints"`
}

// returns formatted name of event
func (e EventResult) FormatName() string {
	return fmt.Sprintf("%v: %v", e.EventType, e.EventName)
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
	GenericData   interface{} `mapstructure:"generic_data"`
	GenericDataHash   string      `mapstructure:"generic_data_hash"`

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