package main

import (
	"time"
	"log"
	"fmt"
)

/* EXCEPTION STORE MODELS */

type KeyEventPeriod struct {
	RawStackHash, ProcessedDataHash string
	TimePeriod                      time.Time
}

type UnaddedEvent struct {
	EventId    string                 `json:"event_id"`
	Message    string                 `json:"message"`
	Level      int                    `json:"level"`
	StackTrace StackTrace             `json:"stacktrace"`
	Extra      map[string]interface{} `json:"extra"`
	Modules    map[string]interface{} `json:"modules"`
	Platform   string                 `json:"platform"`
	Sdk        map[string]interface{} `json:"sdk"`
	ServerName string                 `json:"server_name"`
	Timestamp  float64                `json:"timestamp"`
}

type StackTrace struct {
	Module string  `json:"module"`
	Type   string  `json:"type"`
	Value  string  `json:"value"`
	RawStack string `json:"raw_stack"`
	Frames []Frame `json:"frames"`
}

type Frame struct {
	AbsPath     string                 `json:"abs_path"`
	ContextLine string                 `json:"context_line"`
	Filename    string                 `json:"filename"`
	Function    string                 `json:"function"`
	LineNo      int                    `json:"lineno"`
	Module      string                 `json:"module"`
	PostContext []string               `json:"post_context"`
	PreContext  []string               `json:"pre_context"`
	Vars        map[string]interface{} `json:"vars"`
}

// Wrapper struct for Event Channel
type EventChannel struct {
	_queue    chan UnaddedEvent
	BatchSize int
	ticker *time.Ticker
	quit chan int
}

type EventStore struct {
	ds      DataStore         // link to any data store (Postgres, Cassandra, etc.)
	channel *EventChannel // channel, or queue, for the processing of new events
	log *log.Logger
	timeInterval int
}

// create new Event Store. This 'store' stores necessary information
// about the events and how they are processed. The event channel,
// is the queue, and ds contains the link to the data store, or the DB.
func newEventStore(ds DataStore, config EMConfig, log *log.Logger) *EventStore {
	return &EventStore{
		ds,
		&EventChannel{
			make(chan UnaddedEvent, config.BatchSize),
			config.BatchSize,
			time.NewTicker(time.Duration(config.TimeLimit)*time.Second),
			make(chan int),
		},
		log,
		config.TimeInterval,
	}
}

// Starts the periodic processing of channel
func (es *EventStore) Start() {
	for {
		select{
		case <- es.channel.ticker.C:
			fmt.Println("running")
			es.ProcessBatchEvent()
		case <- es.channel.quit:
			es.channel.ticker.Stop()
			return
		}
	}
}

func (es *EventStore) Stop() {
	es.channel.quit <- 0
}

// Add new UnaddedEvent to channel, process if full
func (es *EventStore) Send(exc UnaddedEvent) {
	es.channel._queue <- exc
	if len(es.channel._queue) == es.channel.BatchSize {
		go es.ProcessBatchEvent()
	}
}

func (es *EventStore) FindInstanceById(id int64) {

}

// Process Batch from channel and bulk insert into Db
func (es *EventStore) ProcessBatchEvent() {
	var excsToAdd []UnaddedEvent
	for length := len(es.channel._queue); length > 0; length-- {
		exc := <-es.channel._queue
		excsToAdd = append(excsToAdd, exc)
	}
	if len(excsToAdd) == 0 { return }

	// Match events with each other to find similar ones

	// Rows to add to Tables
	var eventClasses []EventBase
	var eventClassInstances []EventInstance
	var eventClassInstancePeriods []EventInstancePeriod
	var eventData []EventData

	// Maps the hash to the index of the associated array
	var eventClassesMap = make(map[string]int)
	var eventClassInstancesMap = make(map[string]int)
	var eventClassInstancePeriodsMap = make(map[KeyEventPeriod]int)
	var eventDataMap = make(map[string]int)

	for _, event := range excsToAdd {
		rawStack := GenerateRawStack(event.StackTrace)
		processedStack := ProcessStack(event.StackTrace)
		rawData := ExtractDataFromEvent(event)
		processedData := ProcessData(rawData)

		rawStackHash := Hash(rawStack)
		processedStackHash := Hash(processedStack)
		processedDataHash := Hash(processedData)

		// Each hash should be unique in the database, and so we make sure
		// they are not repeated in the array by checking the associated map.
		if _, ok := eventClassesMap[processedStackHash]; !ok {
			eventClasses = append(eventClasses, EventBase{
				ServiceId:          0, // TODO: add proper id
				ServiceVersion:     event.ServerName,
				Name:               event.Message,
				ProcessedStack:     processedStack,
				ProcessedStackHash: processedStackHash,
			})
			eventClassesMap[processedStackHash] = len(eventClasses) - 1
		}

		if _, ok := eventClassInstancesMap[rawStackHash]; !ok {
			eventClassInstances = append(eventClassInstances, EventInstance{
				ProcessedStackHash: processedStackHash, // Used to reference event_class_id later
				ProcessedDataHash:  processedDataHash,  // Used to reference event_data_id later
				RawStack:           rawStack,
				RawStackHash:       rawStackHash,
			})
			eventClassInstancesMap[rawStackHash] = len(eventClassInstances) - 1
		}

		// The unique key should be the raw stack, the processed stack, and the time period,
		// since the count should keep track of an event instance in a certain time frame.
		t := PythonUnixToGoUnix(event.Timestamp).UTC()
		key := KeyEventPeriod{
			rawStackHash,
			processedDataHash,
			FindBoundingTime(t, es.timeInterval),
		}
		if _, ok := eventClassInstancePeriodsMap[key]; !ok {
			eventClassInstancePeriods = append(eventClassInstancePeriods, EventInstancePeriod{
				StartTime:         key.TimePeriod,
				Updated  :         t,
				TimeInterval:      es.timeInterval,
				RawStackHash:      rawStackHash,      // Used to reference event_class_instance_id later
				ProcessedDataHash: processedDataHash, // Used to reference event_data_id later
				Count:             1,
			})
			eventClassInstancePeriodsMap[key] = len(eventClassInstancePeriods) - 1
		} else {
			eventClassInstancePeriods[eventClassInstancePeriodsMap[key]].Count++
		}

		if _, ok := eventDataMap[processedDataHash]; !ok {
			eventData = append(eventData, EventData{
				RawData:           processedData,
				ProcessedData:     processedData,
				ProcessedDataHash: processedDataHash,
			})
			eventDataMap[processedDataHash] = len(eventData) - 1
		}
	}

	if _, err := es.ds.AddEvents(eventClasses); err != nil {
		es.log.Print("Error while inserting events")
	}

	if _, err := es.ds.AddEventData(eventData); err != nil {
		es.log.Print("Error while inserting event data")
	}
	// Query since upsert does not return ids
	if err := es.ds.QueryEvents(eventClasses); err != nil {
		es.log.Print("Error while querying event class")
	}
	// Query since upsert does not return ids
	if _, err := es.ds.QueryEventData(eventData...); err != nil {
		es.log.Print("Error while querying event data")
	}

	// Add the ids generated from above
	for _, idx := range eventClassInstancesMap {
		stackHash := eventClassInstances[idx].ProcessedStackHash
		dataHash := eventClassInstances[idx].ProcessedDataHash
		eventClassInstances[idx].EventBaseId =
			eventClasses[eventClassesMap[stackHash]].Id
		eventClassInstances[idx].EventDataId =
			eventData[eventDataMap[dataHash]].Id
	}

	if _, err := es.ds.AddEventInstances(eventClassInstances); err != nil {
		es.log.Print("Error while inserting event instances")
	}

	if _, err := es.ds.QueryEventInstances(eventClassInstances...); err != nil {
		es.log.Print("Error while querying event instances")
	}
	// Add the ids generated from above
	for _, idx := range eventClassInstancePeriodsMap {
		stackHash := eventClassInstancePeriods[idx].RawStackHash
		dataHash := eventClassInstancePeriods[idx].ProcessedDataHash
		eventClassInstancePeriods[idx].EventInstanceId =
			eventClassInstances[eventClassInstancesMap[stackHash]].Id
		eventClassInstancePeriods[idx].EventDataId =
			eventData[eventDataMap[dataHash]].Id
	}

	if _, err := es.ds.AddEventinstancePeriods(eventClassInstancePeriods); err != nil {
		es.log.Print("Error while inserting event time periods")
	}
}
