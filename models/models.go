package models

import (
	"time"

	"github.com/mohae/deepcopy"
)

////////////////////////////////////////////////////
/* MODELS CORRESPONDING TO EVENTS AND API RESULTS */
////////////////////////////////////////////////////

type UnaddedEventGroup struct {
	EventId int `json:"event_id"`
	GroupId int `json:"group_id"`
}

// UnaddedEvent is the first event that is sent to the server
type UnaddedEvent struct {
	Service               string                 `json:"service"`
	Environment           string                 `json:"environment"`
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
	Message    string
	RawMessage interface{} `json:"message" mapstructure:"message"`
	Raw        interface{} `json:"raw_data" mapstructure:"raw_data"`
}

// Performs deepcopy
func (e *EventData) Copy() EventData {
	return EventData{
		Message:    e.Message,
		RawMessage: e.RawMessage,
		Raw:        deepcopy.Copy(e.Raw),
	}
}

type KeyEventPeriod struct {
	RawDataHash string
	StartTime   time.Time
}

type EventDetailsResult struct {
	EventType  string      `json:"event_type"`
	EventName  string      `json:"event_name"`
	ServiceId  int         `json:"service_id"`
	RawData    interface{} `json:"raw_data"`
	RawDetails interface{} `json:"raw_details"`
	FirstSeen  string      `json:"first_seen"`
	LastSeen   string      `json:"last_seen"`
}

/////////////////////////////////////////////
/* MODELS CORRESPONDING TO DATABASE TABLES */
/////////////////////////////////////////////

type EventBase struct {
	Id                 int       `mapstructure:"_id"`
	ServiceId          int       `mapstructure:"service_id"`
	EventType          string    `mapstructure:"event_type"`
	EventName          string    `mapstructure:"event_name"`
	EventGroupId       int       `mapstructure:"event_group_id"`
	EventEnvironmentId int       `mapstructure:"event_environment_id"`
	ProcessedData      EventData `mapstructure:"processed_data"`
	ProcessedDataHash  string    `mapstructure:"processed_data_hash"`
}

type EventInstance struct {
	Id                 int       `mapstructure:"_id"`
	EventBaseId        int       `mapstructure:"event_base_id"`
	EventDetailId      int       `mapstructure:"event_detail_id"`
	EventEnvironmentId int       `mapstructure:"event_environment_id"`
	RawData            EventData `mapstructure:"raw_data"`
	GenericData        EventData `mapstructure:"generic_data"`
	GenericDataHash    string    `mapstructure:"generic_data_hash"`
	EventMessage       string    `mapstructure: "event_message"`
	CreatedAt          time.Time `mapstructure: "created_at"`

	// ignored fields, used internally
	ProcessedDataHash   string
	ProcessedDetailHash string
}

type EventInstancePeriod struct {
	Id              int                    `mapstructure:"_id"`
	EventInstanceId int                    `mapstructure:"event_instance_id"`
	StartTime       time.Time              `mapstructure:"start_time"`
	EndTime         time.Time              `mapstructure:"end_time"`
	Updated         time.Time              `mapstructure:"updated"`
	Count           int                    `mapstructure:"count"`
	CounterJson     map[string]interface{} `mapstructure:"counter_json"`
	CAS             int                    `json:"cas_value" mapstructure:"cas_value"`

	// ignored fields, used internally
	RawDataHash string
}

type EventDetail struct {
	Id                  int         `mapstructure:"_id"`
	RawDetail           interface{} `mapstructure:"raw_detail"`
	ProcessedDetail     interface{} `mapstructure:"processed_detail"`
	ProcessedDetailHash string      `mapstructure:"processed_detail_hash"`
}

type EventGroup struct {
	Id   int    `mapstructure:"_id"`
	Name string `mapstructure:"name"`
	Info string `mapstructure:"info"`
}

type EventService struct {
	Id   int    `mapstructure:"_id"`
	Name string `mapstructure:"name"`
}

type EventEnvironment struct {
	Id   int
	Name string
}

type CountStat struct {
	Count       int
	CountPerMin float64
	Increase    float64
}

type OpsdbResult struct {
	EventInstanceId int
	EventBaseId     int
	LastSeen        string
	EvtName         string
	CountSum        int
	Group           string
	EvtMessage      string
	Count           []int
	TimeStamp       []string
	EvtDetails      string
	FirstSeen       string
}

func (o *OpsdbResult) SetCount(count int) {
	o.CountSum = count
}

func (o *OpsdbResult) SetUpdate(update string) {
	o.LastSeen = update
}

func (o *OpsdbResult) Update(c int, t string) {
	o.Count = append(o.Count, c)
	o.TimeStamp = append(o.TimeStamp, t)
}

type DataPointArrays struct {
	Count     string
	TimeStamp string
}

type StackTrace struct {
	Frames []Frame `json:"frames" mapstructure:"frames"`
}

type Frame struct {
	AbsPath     string                 `json:"abs_path" mapstructure:"abs_path"`
	ContextLine string                 `json:"context_line" mapstructure:"context_line"`
	Filename    string                 `json:"filename" mapstructure:"filename"`
	Function    string                 `json:"function" mapstructure:"function"`
	LineNo      int                    `json:"lineno" mapstructure:"lineno"`
	Module      string                 `json:"module" mapstructure:"module"`
	PostContext []string               `json:"post_context" mapstructure:"post_context"`
	PreContext  []string               `json:"pre_context" mapstructure:"pre_context"`
	Vars        map[string]interface{} `json:"vars" mapstructure:"vars"`
}
