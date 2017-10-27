package eventsum

import (
	"fmt"
	"github.com/jacksontj/dataman/src/query"
	"github.com/pkg/errors"
	"log"
	"time"
)

/* EXCEPTION STORE MODELS */

type keyEventPeriod struct {
	RawDataHash, ProcessedDataHash string
	TimePeriod                     time.Time
}

// unaddedEvent is the first event that is sent to the server
type unaddedEvent struct {
	ServiceId             int                    `json:"service_id"`
	Name                  string                 `json:"event_name"`
	Type                  string                 `json:"event_type"`
	Data                  EventData              `json:"event_data"`
	ExtraArgs             map[string]interface{} `json:"extra_args"`
	Timestamp             float64                `json:"timestamp"`
	ConfigurableFilters   map[string][]string    `json:"configurable_filters"`
	ConfigurableGroupings []string               `json:"configurable_groupings"`
}

// Data object, payload of unaddedEvent
type EventData struct {
	Message string      `json:"message"`
	Raw     interface{} `json:"raw_data"`
}

// recent events
type eventRecentResult struct {
	Id            int64       `json:"id"`
	EventType     string      `json:"event_type"`
	EventName     string      `json:"event_name"`
	Count         int         `json:"count"`
	LastUpdated   time.Time   `json:"last_updated"`
	ProcessedData interface{} `json:"processed_data"`
	InstanceIds   []int64     `json:"instance_ids"`
}

// histogram of an event
type eventHistogramResult struct {
	StartTime   time.Time              `json:"start_time"`
	EndTime     time.Time              `json:"end_time"`
	Count       int                    `json:"count"`
	CounterJson map[string]interface{} `json:"count_json"`
}

type eventDetailsResult struct {
	EventType  string      `json:"event_type"`
	EventName  string      `json:"event_name"`
	ServiceId  int         `json:"service_id"`
	RawData    interface{} `json:"raw_data"`
	RawDetails interface{} `json:"raw_details"`
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
	var evtsToAdd []unaddedEvent
	for length := len(es.channel._queue); length > 0; length-- {
		exc := <-es.channel._queue
		evtsToAdd = append(evtsToAdd, exc)
	}
	if len(evtsToAdd) == 0 {
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

	for _, event := range evtsToAdd {
		rawData := event.Data.Raw
		rawDetail := event.ExtraArgs
		// Feed event into filter
		processedData, err := globalRule.ProcessFilter(event, "data")
		if err != nil {
			es.log.Printf("Error when processing data: %v", err)
			processedData = rawData
		}
		processedDetail, err := globalRule.ProcessFilter(event, "extra_args")
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
		e.CounterJson, _ = globalRule.ProcessGrouping(event, e.CounterJson)

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

// Get all events within a time period, including their count
func (es *eventStore) GetRecentEvents(start, end time.Time, serviceId int, limit int) ([]eventRecentResult, error) {
	// TODO: use the limit!!
	var evts []eventRecentResult
	var evtsMap = make(map[int64]int)
	var evtPeriod eventInstancePeriod
	var evtInstance eventInstance
	var evtBase eventBase
	join := []interface{}{"event_instance_id", "event_instance_id.event_base_id"}
	filter := []interface{}{
		map[string]interface{}{"start_time": []interface{}{">", start}}, "AND",
		map[string]interface{}{"end_time": []interface{}{"<", end}},
	}
	res, err := es.ds.Query(query.Filter, "event_instance_period", filter, nil, nil, nil, -1, []string{"start_time"}, join)
	if err != nil {
		return evts, err
	}
	for _, t1 := range res.Return {
		err = mapDecode(t1, &evtPeriod)
		t2, ok := t1["event_instance_id"].(map[string]interface{})
		if !ok {
			continue
		}
		err = mapDecode(t2, &evtInstance)
		t3, ok := t2["event_base_id"].(map[string]interface{})
		if !ok {
			continue
		}
		err = mapDecode(t3, &evtBase)
		if evtBase.ServiceId != serviceId {
			continue
		}
		// TODO: implement grouping
		if _, ok = evtsMap[evtBase.Id]; !ok {
			evts = append(evts, eventRecentResult{
				Id:            evtBase.Id,
				EventType:     evtBase.EventType,
				EventName:     evtBase.EventName,
				ProcessedData: evtBase.ProcessedData,
				Count:         0,
				LastUpdated:   evtPeriod.Updated,
				InstanceIds:   []int64{},
			})
			evtsMap[evtBase.Id] = len(evts) - 1
		}
		evt := &evts[evtsMap[evtBase.Id]]
		evt.Count += evtPeriod.Count
		evt.InstanceIds = append(evt.InstanceIds, evtInstance.Id)
		if evt.LastUpdated.Before(evtPeriod.Updated) {
			evt.LastUpdated = evtPeriod.Updated
		}
	}
	return evts, nil
}

// Get the histogram of a single event instance
func (es *eventStore) GetEventHistogram(start, end time.Time, eventId int) ([]eventHistogramResult, error) {
	var hist []eventHistogramResult
	var bin eventInstancePeriod
	filter := []interface{}{
		[]interface{}{
			map[string]interface{}{"start_time": []interface{}{">", start}}, "AND",
			map[string]interface{}{"end_time": []interface{}{"<", end}},
		}, "AND",
		map[string]interface{}{"event_instance_id": []interface{}{"=", eventId}}}

	res, err := es.ds.Query(query.Filter, "event_instance_period", filter, nil, nil, nil, -1, []string{"start_time"}, nil)
	if err != nil {
		return hist, err
	}
	for _, v := range res.Return {
		mapDecode(v, &bin)
		hist = append(hist, eventHistogramResult{
			StartTime:   bin.StartTime,
			EndTime:     bin.EndTime,
			Count:       bin.Count,
			CounterJson: bin.CounterJson,
		})
	}
	return hist, nil
}

// Get the details of a single event instance
func (es *eventStore) GetEventDetailsbyId(id int) (eventDetailsResult, error) {
	var result eventDetailsResult
	var instance eventInstance
	var detail eventDetail
	var base eventBase
	join := []interface{}{"event_base_id", "event_detail_id"}
	pkey := map[string]interface{}{"_id": id}
	r, err := es.ds.Query(query.Get, "event_instance", nil, nil, nil, pkey, -1, nil, join)
	if r.Error != "" {
		return result, errors.New(r.Error)
	} else if len(r.Return) == 0 {
		return result, err
	}
	mapDecode(r.Return[0], &instance)
	if t1, ok := r.Return[0]["event_base_id"].(map[string]interface{}); ok {
		mapDecode(t1, &base)
		if t2, ok := t1["event_detail_id"].(map[string]interface{}); ok {
			mapDecode(t2, &detail)
		}
	}
	result = eventDetailsResult{
		ServiceId:  base.ServiceId,
		EventType:  base.EventType,
		EventName:  base.EventName,
		RawData:    instance.RawData,
		RawDetails: detail.RawDetail,
	}
	return result, nil
}
