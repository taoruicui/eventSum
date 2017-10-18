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

// Wrapper struct for Event Channel
type EventChannel struct {
	_queue    chan UnaddedEvent
	BatchSize int
	ticker    *time.Ticker
	quit      chan int
}

type EventStore struct {
	ds           *DataStore    // link to any data store (Postgres, Cassandra, etc.)
	channel      *EventChannel // channel, or queue, for the processing of new events
	log          *log.Logger
	timeInterval int // interval time for event_instance_period
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
		rawData := event.Data.Raw
		rawDetail := event.ExtraArgs
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
		startTime, endTime := FindBoundingTime(t, es.timeInterval)
		key := KeyEventPeriod{
			rawDataHash,
			processedDataHash,
			startTime,
		}
		if _, ok := eventClassInstancePeriodsMap[key]; !ok {
			eventClassInstancePeriods = append(eventClassInstancePeriods, EventInstancePeriod{
				StartTime:           startTime,
				Updated:             t,
				EndTime:             endTime,
				RawDataHash:         rawDataHash,         // Used to reference event_instance_id later
				ProcessedDetailHash: processedDetailHash, // Used to reference event_detail_id later
				Count:               1,
				CounterJson:         make(map[string]int),
			})
			eventClassInstancePeriodsMap[key] = len(eventClassInstancePeriods) - 1
		} else {
			eventClassInstancePeriods[eventClassInstancePeriodsMap[key]].Count++
			//eventClassInstancePeriods[eventClassInstancePeriodsMap[key]].CounterJson[something]++
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
		es.log.Printf("Error while inserting events: %v", err)
	}

	if err := es.ds.AddEventDetails(eventDetails); err != nil {
		es.log.Printf("Error while inserting event data: %v", err)
	}

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
		es.log.Printf("Error while inserting event instances: %v", err)
	}

	// Add the ids generated from above
	for _, idx := range eventClassInstancePeriodsMap {
		dataHash := eventClassInstancePeriods[idx].RawDataHash
		eventClassInstancePeriods[idx].EventInstanceId =
			eventClassInstances[eventClassInstancesMap[dataHash]].Id
		//detailHash := eventClassInstancePeriods[idx].ProcessedDetailHash
		//eventClassInstancePeriods[idx].EventDetailId =
		//	eventData[eventDataMap[detailHash]].Id
	}

	if err := es.ds.AddEventinstancePeriods(eventClassInstancePeriods); err != nil {
		es.log.Printf("Error while inserting event time periods: %v", err)
	}
}
