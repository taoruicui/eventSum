package eventsum

import (
	conf "github.com/ContextLogic/eventsum/config"
	"github.com/ContextLogic/eventsum/datastore"
	"github.com/ContextLogic/eventsum/log"
	"github.com/ContextLogic/eventsum/metrics"
	. "github.com/ContextLogic/eventsum/models"
	"github.com/ContextLogic/eventsum/util"
	"github.com/jacksontj/dataman/src/query"
	"github.com/pkg/errors"
	"time"
)

// Wrapper struct for Event Channel
type eventChannel struct {
	_queue    chan UnaddedEvent
	BatchSize int
	ticker    *time.Ticker
	quit      chan int
}

type eventStore struct {
	ds           datastore.DataStore // link to any data store (Postgres, Cassandra, etc.)
	channel      *eventChannel       // channel, or queue, for the processing of new events
	log          *log.Logger
	timeInterval int // interval time for event_instance_period
	timeFormat   string
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
		config.TimeFormat,
	}
}

// Starts the periodic processing of channel
func (es *eventStore) Start() {
	for {
		select {
		case <-es.channel.ticker.C:
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
	start := time.Now()
	defer func() {
		metrics.EventStoreLatency("SummarizeBatchEvents", start)
	}()

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
	var serviceNameMap = es.ds.GetServicesMap()
	var envNameMap = es.ds.GetEnvironmentsMap()

	for _, event := range evtsToAdd {
		rawEvent := event // Used for grouping
		rawDetail := event.ExtraArgs
		event, err := globalRule.ProcessFilter(event, "instance")
		// TODO: save to logs if filters fail
		if err != nil {
			es.log.App().Errorf("Error when processing instance: %v", err)
			continue
		}
		genericData := event.Data
		// Feed event into filter
		event, err = globalRule.ProcessFilter(event, "base")
		if err != nil {
			es.log.App().Errorf("Error when processing base: %v", err)
			continue
		}
		processedData := event.Data
		event, err = globalRule.ProcessFilter(event, "extra_args")
		if err != nil {
			es.log.App().Errorf("Error when processing extra args: %v", err)
			continue
		}
		processedDetail := event.ExtraArgs

		genericDataHash := util.Hash(genericData)
		processedDataHash := util.Hash(processedData)
		processedDetailHash := util.Hash(processedDetail)

		// Each hash should be unique in the database, and so we make sure
		// they are not repeated in the array by checking the associated map.
		if _, ok := eventClassesMap[processedDataHash]; !ok {
			eventClasses = append(eventClasses, EventBase{
				ServiceId:          serviceNameMap[event.Service].Id,
				EventType:          event.Type,
				EventName:          event.Name,
				EventEnvironmentId: envNameMap[event.Environment].Id,
				ProcessedData:      processedData,
				ProcessedDataHash:  processedDataHash,
			})
			eventClassesMap[processedDataHash] = len(eventClasses) - 1
		}

		if _, ok := eventClassInstancesMap[genericDataHash]; !ok {
			eventClassInstances = append(eventClassInstances, EventInstance{
				ProcessedDataHash:   processedDataHash,   // Used to reference event_base_id later
				ProcessedDetailHash: processedDetailHash, // Used to reference event_detail_id later
				RawData:             rawEvent.Data,
				GenericData:         genericData,
				GenericDataHash:     genericDataHash,
				EventEnvironmentId:  envNameMap[event.Environment].Id,
			})
			eventClassInstancesMap[genericDataHash] = len(eventClassInstances) - 1
		}

		// The unique key should be the raw data, and the time period,
		// since the count should keep track of an event instance in a certain time frame.
		t, err := time.Parse(es.timeFormat, event.Timestamp)
		startTime, endTime := util.FindBoundingTime(t, es.timeInterval)
		key := KeyEventPeriod{
			RawDataHash: genericDataHash,
			StartTime:   startTime,
		}
		if _, ok := eventClassInstancePeriodsMap[key]; !ok {
			eventClassInstancePeriods = append(eventClassInstancePeriods, EventInstancePeriod{
				StartTime:   startTime,
				Updated:     t,
				EndTime:     endTime,
				RawDataHash: genericDataHash, // Used to reference event_instance_id later
				Count:       0,
				CounterJson: make(map[string]interface{}),
			})
			eventClassInstancePeriodsMap[key] = len(eventClassInstancePeriods) - 1
		}
		e := &eventClassInstancePeriods[eventClassInstancePeriodsMap[key]]
		e.Count++
		e.CounterJson, _ = globalRule.ProcessGrouping(rawEvent, e.CounterJson)

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
			es.log.EventLog().LogData(eventClasses[i])
			es.log.App().Errorf("Error while inserting events: %v", v)
		}
	}

	errDetails := es.ds.AddEventDetails(eventDetails)
	if len(errDetails) != 0 {
		for i, v := range errDetails {
			es.log.EventLog().LogData(eventDetails[i])
			es.log.App().Errorf("Error while inserting event data: %v", v)
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
			es.log.EventLog().LogData(eventClassInstances[idx])
		}
		if _, ok := errDetails[eventDetailsMap[detailHash]]; ok {
			es.log.EventLog().LogData(eventClassInstances[idx])
		}
	}

	errInstances := es.ds.AddEventInstances(eventClassInstances)
	if len(errInstances) != 0 {
		for i, v := range errInstances {
			es.log.EventLog().LogData(eventClassInstances[i])
			es.log.App().Errorf("Error while inserting event instances: %v", v)
		}
	}

	// Add the ids generated from above
	for _, idx := range eventClassInstancePeriodsMap {
		dataHash := eventClassInstancePeriods[idx].RawDataHash
		eventClassInstancePeriods[idx].EventInstanceId =
			eventClassInstances[eventClassInstancesMap[dataHash]].Id

		if _, ok := errInstances[eventClassInstancesMap[dataHash]]; ok {
			es.log.EventLog().LogData(eventClassInstancePeriods[idx])
		}
	}

	if err := es.ds.AddEventinstancePeriods(eventClassInstancePeriods); len(err) != 0 {
		for i, v := range err {
			es.log.EventLog().LogData(eventClassInstancePeriods[i])
			es.log.App().Errorf("Error while inserting event time periods: %v", v)
		}
	}
}

// GeneralQuery is used by various handlers (grafana queries,
// web queries, etc). It takes in a time range, as well as query
// params in the form of maps, which will filter out the events.
// Empty maps indicate that there should be no filtering for that
// parameter.
func (es *eventStore) GeneralQuery(
	start, end time.Time,
	eventGroupMap, eventBaseMap, serviceIdMap, envIdMap map[int]bool) (EventResults, error) {

	now := time.Now()
	defer func() {
		metrics.EventStoreLatency("GetRecentEvents", now)
	}()

	var evts EventResults
	var evtsMap = make(map[int]int)
	join := []interface{}{"event_instance_id", "event_instance_id.event_base_id"}
	filter := []interface{}{
		map[string]interface{}{"updated": []interface{}{">=", start}}, "AND",
		map[string]interface{}{"updated": []interface{}{"<=", end}},
	}
	res, err := es.ds.Query(query.Filter, "event_instance_period", filter, nil, nil, nil, -1, nil, join)
	if err != nil {
		return evts, err
	}

	// loop through results
	for _, t1 := range res.Return {
		evtPeriod := EventInstancePeriod{}
		evtInstance := EventInstance{}
		evtBase := EventBase{}
		err = util.MapDecode(t1, &evtPeriod, true)
		t2, ok := t1["event_instance_id"].(map[string]interface{})
		if !ok {
			continue
		}
		err = util.MapDecode(t2, &evtInstance, true)
		t3, ok := t2["event_base_id"].(map[string]interface{})
		if !ok {
			continue
		}
		err = util.MapDecode(t3, &evtBase, true)

		// check if event matches params. If map is empty then every event matches
		if _, ok := serviceIdMap[evtBase.ServiceId]; !ok && len(serviceIdMap) != 0 {
			continue
		}
		if _, ok := envIdMap[evtBase.EventEnvironmentId]; !ok && len(envIdMap) != 0 {
			continue
		}
		if _, ok := eventBaseMap[evtBase.Id]; !ok && len(eventBaseMap) != 0 {
			continue
		}
		if _, ok := eventGroupMap[evtBase.EventGroupId]; !ok && len(eventGroupMap) != 0 {
			continue
		}

		// Aggregate similar events
		if _, ok = evtsMap[evtBase.Id]; !ok {
			evts = append(evts, EventResult{
				Id:            evtBase.Id,
				EventType:     evtBase.EventType,
				EventName:     evtBase.EventName,
				TotalCount:    0,
				ProcessedData: evtBase.ProcessedData,
				InstanceIds:   []int{},
				Datapoints:    EventBins{},
			})
			evtsMap[evtBase.Id] = len(evts) - 1
		}

		start := int(evtPeriod.Updated.Unix() * 1000)
		evt := &evts[evtsMap[evtBase.Id]]
		evt.TotalCount += evtPeriod.Count
		evt.InstanceIds = append(evt.InstanceIds, evtInstance.Id)

		if _, ok := evt.Datapoints[start]; !ok {
			evt.Datapoints[start] = &Bin{Start: start, Count: 0}
		}

		evt.Datapoints[start].Count += evtPeriod.Count
	}

	return evts, nil
}

// Get the details of a single event instance
func (es *eventStore) GetEventDetailsbyId(id int) (EventDetailsResult, error) {
	now := time.Now()
	defer func() {
		metrics.EventStoreLatency("GetEventDetailsbyId", now)
	}()

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
	util.MapDecode(r.Return[0], &instance, false)
	if t1, ok := r.Return[0]["event_base_id"].(map[string]interface{}); ok {
		util.MapDecode(t1, &base, false)
		if t2, ok := r.Return[0]["event_detail_id"].(map[string]interface{}); ok {
			util.MapDecode(t2, &detail, false)
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
