package eventsum

import (
	"fmt"
	"log"
	"time"
)

/* EXCEPTION STORE MODELS */

type keyEventPeriod struct {
	RawDataHash, ProcessedDataHash string
	TimePeriod                      time.Time
}

type unaddedEvent struct {
	ServiceId           int                    `json:"service_id"`
	Name                string                 `json:"event_name"`
	Type                string                 `json:"event_type"`
	Data                EventData              `json:"event_data"`
	ExtraArgs           map[string]interface{} `json:"extra_args"`
	Timestamp           float64                `json:"timestamp"`
	ConfigurableFilters map[string][]string    `json:"configurable_filters"`
	ConfigurableGroupings []string    `json:"configurable_groupings"`
}

// Exported struct
type EventData struct {
	Message string      `json:"message"`
	Raw     interface{} `json:"raw_data"`
}

// Wrapper struct for Event Channel
type eventChannel struct {
	_queue    chan unaddedEvent
	BatchSize int
	ticker    *time.Ticker
	quit      chan int
}

type eventStore struct {
	ds           *dataStore    // link to any data store (Postgres, Cassandra, etc.)
	channel      *eventChannel // channel, or queue, for the processing of new events
	log          *log.Logger
	timeInterval int // interval time for event_instance_period
	rule rule
}

// create new Event Store. This 'store' stores necessary information
// about the events and how they are processed. The event channel,
// is the queue, and ds contains the link to the data store, or the DB.
func newEventStore(ds *dataStore, config eventsumConfig, log *log.Logger) *eventStore {
	return &eventStore{
		ds,
		&eventChannel{
			make(chan unaddedEvent, config.BatchSize),
			config.BatchSize,
			time.NewTicker(time.Duration(config.TimeLimit) * time.Second),
			make(chan int),
		},
		log,
		config.TimeInterval,
		newRule(log),
	}
}

// Starts the periodic processing of channel
func (es *eventStore) Start() {
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

func (es *eventStore) Stop() {
	es.channel.quit <- 0
}

// Add new unaddedEvent to channel, process if full
func (es *eventStore) Send(exc unaddedEvent) {
	es.channel._queue <- exc
	if len(es.channel._queue) == es.channel.BatchSize {
		go es.SummarizeBatchEvents()
	}
}

// Process Batch from channel and bulk insert into Db
func (es *eventStore) SummarizeBatchEvents() {
	var excsToAdd []unaddedEvent
	for length := len(es.channel._queue); length > 0; length-- {
		exc := <-es.channel._queue
		excsToAdd = append(excsToAdd, exc)
	}
	if len(excsToAdd) == 0 {
		return
	}

	// Match events with each other to find similar ones

	// Rows to add to Tables
	var eventClasses []eventBase
	var eventClassInstances []eventInstance
	var eventClassInstancePeriods []eventInstancePeriod
	var eventDetails []eventDetail

	// Maps the hash to the index of the associated array
	var eventClassesMap = make(map[string]int)
	var eventClassInstancesMap = make(map[string]int)
	var eventClassInstancePeriodsMap = make(map[keyEventPeriod]int)
	var eventDetailsMap = make(map[string]int)

	for _, event := range excsToAdd {
		rawData := event.Data.Raw
		rawDetail := event.ExtraArgs
		// Feed event into filter
		processedData, err := es.rule.ProcessFilter(event, "data")
		if err != nil {
			es.log.Printf("Error when processing data: %v", err)
			processedData = rawData
		}
		processedDetail, err := es.rule.ProcessFilter(event, "detail")
		if err != nil {
			es.log.Printf("Error when processing detail: %v", err)
			processedDetail = rawDetail
		}

		rawDataHash := hash(rawData)
		processedDataHash := hash(processedData)
		processedDetailHash := hash(processedDetail)

		// Each hash should be unique in the database, and so we make sure
		// they are not repeated in the array by checking the associated map.
		if _, ok := eventClassesMap[processedDataHash]; !ok {
			eventClasses = append(eventClasses, eventBase{
				ServiceId:         event.ServiceId,
				EventType:         event.Type,
				EventName:         event.Name,
				ProcessedData:     processedData,
				ProcessedDataHash: processedDataHash,
			})
			eventClassesMap[processedDataHash] = len(eventClasses) - 1
		}

		if _, ok := eventClassInstancesMap[rawDataHash]; !ok {
			eventClassInstances = append(eventClassInstances, eventInstance{
				ProcessedDataHash:   processedDataHash,   // Used to reference event_base_id later
				ProcessedDetailHash: processedDetailHash, // Used to reference event_detail_id later
				RawData:             rawData,
				RawDataHash:         rawDataHash,
			})
			eventClassInstancesMap[rawDataHash] = len(eventClassInstances) - 1
		}

		// The unique key should be the raw data, the processed data, and the time period,
		// since the count should keep track of an event instance in a certain time frame.
		t := pythonUnixToGoUnix(event.Timestamp).UTC()
		startTime, endTime := findBoundingTime(t, es.timeInterval)
		key := keyEventPeriod{
			rawDataHash,
			processedDataHash,
			startTime,
		}
		if _, ok := eventClassInstancePeriodsMap[key]; !ok {
			eventClassInstancePeriods = append(eventClassInstancePeriods, eventInstancePeriod{
				StartTime:           startTime,
				Updated:             t,
				EndTime:             endTime,
				RawDataHash:         rawDataHash,         // Used to reference event_instance_id later
				ProcessedDetailHash: processedDetailHash, // Used to reference event_detail_id later
				Count:               0,
				CounterJson:         make(map[string]interface{}),
			})
			eventClassInstancePeriodsMap[key] = len(eventClassInstancePeriods) - 1
		}
		e := &eventClassInstancePeriods[eventClassInstancePeriodsMap[key]]
		e.Count++
		e.CounterJson, _ = es.rule.ProcessGrouping(event, e.CounterJson)

		if _, ok := eventDetailsMap[processedDataHash]; !ok {
			eventDetails = append(eventDetails, eventDetail{
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
