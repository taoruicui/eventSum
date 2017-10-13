package main

import (
	"fmt"
	"log"
	"time"
)

/* EXCEPTION STORE MODELS */

type KeyEventPeriod struct {
	RawStackHash, ProcessedDataHash string
	TimePeriod                      time.Time
}

type UnaddedEvent struct {
	ServiceId           int                    `json:"service_id"`
	Name                string                 `json:"event_name"`
	Type                string                 `json:"event_type"`
	Data                EventData              `json:"event_data"`
	ExtraArgs           map[string]interface{} `json:"extra_args"`
	Timestamp           float64                `json:"timestamp"`
	ConfigurableFilters map[string][]string    `json:"configurable_filters"`
}

type EventData struct {
	Message string      `json:"message"`
	Raw     interface{} `json:"raw_data"`
}

type StackTrace struct {
	Module   string  `json:"module"`
	Type     string  `json:"type"`
	Value    string  `json:"value"`
	RawStack string  `json:"raw_stack"`
	Frames   []Frame `json:"frames"`
}

type ExceptionData struct {
	Module   string  `json:"module"`
	Type     string  `json:"type"`
	Value    string  `json:"value"`
	Frames   []Frame `json:"frames"`
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
	ticker    *time.Ticker
	quit      chan int
}

type EventStore struct {
	ds           *DataStore     // link to any data store (Postgres, Cassandra, etc.)
	channel      *EventChannel // channel, or queue, for the processing of new events
	log          *log.Logger
	timeInterval int
}

// create new Event Store. This 'store' stores necessary information
// about the events and how they are processed. The event channel,
// is the queue, and ds contains the link to the data store, or the DB.
func newEventStore(ds *DataStore, config EMConfig, log *log.Logger) *EventStore {
	return &EventStore{
		ds,
		&EventChannel{
			make(chan UnaddedEvent, config.BatchSize),
			config.BatchSize,
			time.NewTicker(time.Duration(config.TimeLimit) * time.Second),
			make(chan int),
		},
		log,
		config.TimeInterval,
	}
}

// Starts the periodic processing of channel
func (es *EventStore) Start() {
	for {
		select {
		case <-es.channel.ticker.C:
			fmt.Println("running")
			es.SummarizeBatchEvents()
		case <-es.channel.quit:
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
		go es.SummarizeBatchEvents()
	}
}

// Process Batch from channel and bulk insert into Db
func (es *EventStore) SummarizeBatchEvents() {
	var excsToAdd []UnaddedEvent
	for length := len(es.channel._queue); length > 0; length-- {
		exc := <-es.channel._queue
		excsToAdd = append(excsToAdd, exc)
	}
	if len(excsToAdd) == 0 {
		return
	}

	f := newFilter()
	// Match events with each other to find similar ones

	// Rows to add to Tables
	var eventClasses []EventBase
	var eventClassInstances []EventInstance
	var eventClassInstancePeriods []EventInstancePeriod
	var eventDetails []EventDetail

	// Maps the hash to the index of the associated array
	var eventClassesMap = make(map[string]int)
	var eventClassInstancesMap = make(map[string]int)
	var eventClassInstancePeriodsMap = make(map[KeyEventPeriod]int)
	var eventDetailsMap = make(map[string]int)

	for _, event := range excsToAdd {
		rawData := ToJson(event.Data.Raw)
		rawDetail := ToJson(event.ExtraArgs)
		// Feed event into filter
		processedData, err := f.Process(event, "data")
		if err != nil {
			es.log.Printf("Error when processing data: %v", err)
			processedData = rawData
		}
		processedDetail, err := f.Process(event, "detail")
		if err != nil {
			es.log.Printf("Error when processing detail: %v", err)
			processedDetail = rawDetail
		}

		rawDataHash := Hash(rawData)
		processedDataHash := Hash(processedData)
		processedDetailHash := Hash(processedDetail)

		// Each hash should be unique in the database, and so we make sure
		// they are not repeated in the array by checking the associated map.
		if _, ok := eventClassesMap[processedDataHash]; !ok {
			eventClasses = append(eventClasses, EventBase{
				ServiceId:         event.ServiceId,
				EventType:         event.Type,
				EventName:         event.Name,
				ProcessedData:     processedData,
				ProcessedDataHash: processedDataHash,
			})
			eventClassesMap[processedDataHash] = len(eventClasses) - 1
		}

		if _, ok := eventClassInstancesMap[rawDataHash]; !ok {
			eventClassInstances = append(eventClassInstances, EventInstance{
				ProcessedDataHash:   processedDataHash,   // Used to reference event_base_id later
				ProcessedDetailHash: processedDetailHash, // Used to reference event_detail_id later
				RawData:             rawData,
				RawDataHash:         rawDataHash,
			})
			eventClassInstancesMap[rawDataHash] = len(eventClassInstances) - 1
		}

		// The unique key should be the raw stack, the processed stack, and the time period,
		// since the count should keep track of an event instance in a certain time frame.
		t := PythonUnixToGoUnix(event.Timestamp).UTC()
		key := KeyEventPeriod{
			rawDataHash,
			processedDataHash,
			FindBoundingTime(t, es.timeInterval),
		}
		if _, ok := eventClassInstancePeriodsMap[key]; !ok {
			eventClassInstancePeriods = append(eventClassInstancePeriods, EventInstancePeriod{
				StartTime:           key.TimePeriod,
				Updated:             t,
				TimeInterval:        es.timeInterval,
				RawDataHash:         rawDataHash,         // Used to reference event_instance_id later
				ProcessedDetailHash: processedDetailHash, // Used to reference event_detail_id later
				Count:               1,
			})
			eventClassInstancePeriodsMap[key] = len(eventClassInstancePeriods) - 1
		} else {
			eventClassInstancePeriods[eventClassInstancePeriodsMap[key]].Count++
		}

		if _, ok := eventDetailsMap[processedDataHash]; !ok {
			eventDetails = append(eventDetails, EventDetail{
				RawDetail:           rawDetail,
				ProcessedDetail:     processedDetail,
				ProcessedDetailHash: processedDetailHash,
			})
			eventDetailsMap[processedDataHash] = len(eventDetails) - 1
		}
	}

	if err := es.ds.AddEvents(eventClasses); err != nil {
		es.log.Print("Error while inserting events")
	}

	if err := es.ds.AddEventDetails(eventDetails); err != nil {
		es.log.Print("Error while inserting event data")
	}
	//// Query since upsert does not return ids
	//if err := es.ds.QueryEvents(eventClasses); err != nil {
	//	es.log.Print("Error while querying event class")
	//}
	//// Query since upsert does not return ids
	//if _, err := es.ds.QueryEventDetails(eventDetails...); err != nil {
	//	es.log.Print("Error while querying event data")
	//}

	// Add the ids generated from above
	for _, idx := range eventClassInstancesMap {
		dataHash := eventClassInstances[idx].ProcessedDataHash
		detailHash := eventClassInstances[idx].ProcessedDetailHash
		eventClassInstances[idx].EventBaseId =
			eventClasses[eventClassesMap[dataHash]].Id
		eventClassInstances[idx].EventDetailId =
			eventDetails[eventDetailsMap[detailHash]].Id
	}

	if err := es.ds.AddEventInstances(eventClassInstances); err != nil {
		es.log.Print("Error while inserting event instances")
	}

	//if _, err := es.ds.QueryEventInstances(eventClassInstances...); err != nil {
	//	es.log.Print("Error while querying event instances")
	//}
	// Add the ids generated from above
	for _, idx := range eventClassInstancePeriodsMap {
		dataHash := eventClassInstancePeriods[idx].RawDataHash
		//detailHash := eventClassInstancePeriods[idx].ProcessedDetailHash
		eventClassInstancePeriods[idx].EventInstanceId =
			eventClassInstances[eventClassInstancesMap[dataHash]].Id
		//eventClassInstancePeriods[idx].EventDetailId =
		//	eventData[eventDataMap[detailHash]].Id
	}

	if err := es.ds.AddEventinstancePeriods(eventClassInstancePeriods); err != nil {
		es.log.Print("Error while inserting event time periods")
	}
}
