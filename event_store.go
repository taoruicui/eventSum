package eventsum

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/jacksontj/dataman/src/query"
	"github.com/ContextLogic/eventsum/datastore"
	"github.com/ContextLogic/eventsum/log"
	conf "github.com/ContextLogic/eventsum/config"
	. "github.com/ContextLogic/eventsum/models"
	"time"
	"github.com/ContextLogic/eventsum/util"
)

// Wrapper struct for Event Channel
type eventChannel struct {
	_queue    chan UnaddedEvent
	BatchSize int
	ticker    *time.Ticker
	quit      chan int
}

type eventStore struct {
	ds           datastore.DataStore    // link to any data store (Postgres, Cassandra, etc.)
	channel      *eventChannel // channel, or queue, for the processing of new events
	log          *log.Logger
	timeInterval int // interval time for event_instance_period
}

// create new Event Store. This 'store' stores necessary information
// about the events and how they are processed. The event channel,
// is the queue, and ds contains the link to the data store, or the DB.
func newEventStore(ds datastore.DataStore, config conf.EventsumConfig, log *log.Logger) *eventStore {
	return &eventStore{
		ds,
		&eventChannel{
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

// Add new UnaddedEvent to channel, process if full
func (es *eventStore) Send(exc UnaddedEvent) {
	es.channel._queue <- exc
	if len(es.channel._queue) == es.channel.BatchSize {
		go es.SummarizeBatchEvents()
	}
}

// Process Batch from channel and bulk insert into Db
func (es *eventStore) SummarizeBatchEvents() {
	var evtsToAdd []UnaddedEvent
	for length := len(es.channel._queue); length > 0; length-- {
		exc := <-es.channel._queue
		evtsToAdd = append(evtsToAdd, exc)
	}
	if len(evtsToAdd) == 0 {
		return
	}

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

	for _, event := range evtsToAdd {
		rawData := event.Data.Raw
		rawDetail := event.ExtraArgs
		// Feed event into filter
		processedData, err := globalRule.ProcessFilter(event, "data")
		if err != nil {
			es.log.App.Errorf("Error when processing data: %v", err)
			processedData = rawData
		}
		processedDetail, err := globalRule.ProcessFilter(event, "extra_args")
		if err != nil {
			es.log.App.Errorf("Error when processing detail: %v", err)
			processedDetail = rawDetail
		}

		rawDataHash := util.Hash(rawData)
		processedDataHash := util.Hash(processedData)
		processedDetailHash := util.Hash(processedDetail)

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

		// The unique key should be the raw data, and the time period,
		// since the count should keep track of an event instance in a certain time frame.
		t := util.PythonUnixToGoUnix(event.Timestamp).UTC()
		startTime, endTime := util.FindBoundingTime(t, es.timeInterval)
		key := KeyEventPeriod{
			RawDataHash: rawDataHash,
			StartTime: startTime,
		}
		if _, ok := eventClassInstancePeriodsMap[key]; !ok {
			eventClassInstancePeriods = append(eventClassInstancePeriods, EventInstancePeriod{
				StartTime:           startTime,
				Updated:             t,
				EndTime:             endTime,
				RawDataHash:         rawDataHash,         // Used to reference event_instance_id later
				Count:               0,
				CounterJson:         make(map[string]interface{}),
			})
			eventClassInstancePeriodsMap[key] = len(eventClassInstancePeriods) - 1
		}
		e := &eventClassInstancePeriods[eventClassInstancePeriodsMap[key]]
		e.Count++
		e.CounterJson, _ = globalRule.ProcessGrouping(event, e.CounterJson)

		if _, ok := eventDetailsMap[processedDataHash]; !ok {
			eventDetails = append(eventDetails, EventDetail{
				RawDetail:           rawDetail,
				ProcessedDetail:     processedDetail,
				ProcessedDetailHash: processedDetailHash,
			})
			eventDetailsMap[processedDataHash] = len(eventDetails) - 1
		}
	}

	// Returns a map where the keys are the indices that an error occurred
	errBase := es.ds.AddEvents(eventClasses)
	if len(errBase) != 0 {
		for i, v := range errBase {
			es.log.EventLog.LogData(eventClasses[i])

			es.log.App.Errorf("Error while inserting events: %v", v)
		}
	}

	errDetails := es.ds.AddEventDetails(eventDetails)
	if len(errDetails) != 0 {
		for i, v := range errDetails {
			es.log.EventLog.LogData(eventDetails[i])
			es.log.App.Errorf("Error while inserting event data: %v", v)
		}
	}

	// Add the ids generated from above
	for _, idx := range eventClassInstancesMap {

		dataHash := eventClassInstances[idx].ProcessedDataHash
		detailHash := eventClassInstances[idx].ProcessedDetailHash
		eventClassInstances[idx].EventBaseId =
			eventClasses[eventClassesMap[dataHash]].Id
		eventClassInstances[idx].EventDetailId =
			eventDetails[eventDetailsMap[detailHash]].Id
		// log instance if there was an error adding event base or event details
		if _, ok := errBase[eventClassesMap[dataHash]]; ok {
			es.log.EventLog.LogData(eventClassInstances[idx])
		}
		if _, ok := errDetails[eventDetailsMap[detailHash]]; ok {
			es.log.EventLog.LogData(eventClassInstances[idx])
		}
	}

	errInstances := es.ds.AddEventInstances(eventClassInstances)
	if len(errInstances) != 0 {
		for i, v := range errInstances {
			es.log.EventLog.LogData(eventClassInstances[i])
			es.log.App.Errorf("Error while inserting event instances: %v", v)
		}
	}

	// Add the ids generated from above
	for _, idx := range eventClassInstancePeriodsMap {
		dataHash := eventClassInstancePeriods[idx].RawDataHash
		eventClassInstancePeriods[idx].EventInstanceId =
			eventClassInstances[eventClassInstancesMap[dataHash]].Id

		if _, ok := errInstances[eventClassInstancesMap[dataHash]]; ok {
			es.log.EventLog.LogData(eventClassInstancePeriods[idx])
		}
	}

	if err := es.ds.AddEventinstancePeriods(eventClassInstancePeriods); len(err) != 0 {
		for i, v := range err {
			es.log.EventLog.LogData(eventClassInstancePeriods[i])
			es.log.App.Errorf("Error while inserting event time periods: %v", v)
		}
	}
}

// Get all events within a time period, including their count
func (es *eventStore) GetRecentEvents(start, end time.Time, serviceId int, limit int) ([]EventRecentResult, error) {
	// TODO: use the limit!!
	var evts []EventRecentResult
	var evtsMap = make(map[int64]int)
	var evtPeriod EventInstancePeriod
	var evtInstance EventInstance
	var evtBase EventBase
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
		err = util.MapDecode(t1, &evtPeriod)
		t2, ok := t1["event_instance_id"].(map[string]interface{})
		if !ok {
			continue
		}
		err = util.MapDecode(t2, &evtInstance)
		t3, ok := t2["event_base_id"].(map[string]interface{})
		if !ok {
			continue
		}
		err = util.MapDecode(t3, &evtBase)
		if evtBase.ServiceId != serviceId {
			continue
		}
		// TODO: implement grouping
		if _, ok = evtsMap[evtBase.Id]; !ok {
			evts = append(evts, EventRecentResult{
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
func (es *eventStore) GetEventHistogram(start, end time.Time, eventId int) ([]EventHistogramResult, error) {
	var hist []EventHistogramResult
	var bin EventInstancePeriod
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
		util.MapDecode(v, &bin)
		hist = append(hist, EventHistogramResult{
			StartTime:   bin.StartTime,
			EndTime:     bin.EndTime,
			Count:       bin.Count,
			CounterJson: bin.CounterJson,
		})
	}
	return hist, nil
}

// Get the details of a single event instance
func (es *eventStore) GetEventDetailsbyId(id int) (EventDetailsResult, error) {
	var result EventDetailsResult
	var instance EventInstance
	var detail EventDetail
	var base EventBase
	join := []interface{}{"event_base_id", "event_detail_id"}
	pkey := map[string]interface{}{"_id": id}
	r, err := es.ds.Query(query.Get, "event_instance", nil, nil, nil, pkey, -1, nil, join)
	if r.Error != "" {
		return result, errors.New(r.Error)
	} else if len(r.Return) == 0 {
		return result, err
	}
	util.MapDecode(r.Return[0], &instance)
	if t1, ok := r.Return[0]["event_base_id"].(map[string]interface{}); ok {
		util.MapDecode(t1, &base)
		if t2, ok := t1["event_detail_id"].(map[string]interface{}); ok {
			util.MapDecode(t2, &detail)
		}
	}
	result = EventDetailsResult{
		ServiceId:  base.ServiceId,
		EventType:  base.EventType,
		EventName:  base.EventName,
		RawData:    instance.RawData,
		RawDetails: detail.RawDetail,
	}
	return result, nil
}
