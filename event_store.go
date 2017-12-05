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
	"sort"
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
				ServiceId:         event.ServiceId,
				EventType:         event.Type,
				EventName:         event.Name,
				ProcessedData:     processedData,
				ProcessedDataHash: processedDataHash,
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

// Get all events within a time period, including their count
func (es *eventStore) GetRecentEvents(start, end time.Time, serviceId int, limit int) (EventResults, error) {
	now := time.Now()
	defer func() {
		metrics.EventStoreLatency("GetRecentEvents", now)
	}()

	var evts EventResults
	var evtsMap = make(map[int64]int)
	var evtPeriod EventInstancePeriod
	var evtInstance EventInstance
	var evtBase EventBase
	join := []interface{}{"event_instance_id", "event_instance_id.event_base_id"}
	filter := []interface{}{
		map[string]interface{}{"start_time": []interface{}{">", start}}, "AND",
		map[string]interface{}{"end_time": []interface{}{"<", end}},
	}
	res, err := es.ds.Query(query.Filter, "event_instance_period", filter, nil, nil, nil, -1, nil, join)
	if err != nil {
		return evts, err
	}

	// loop through results
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

		// Aggregate similar events
		if _, ok = evtsMap[evtBase.Id]; !ok {
			evts = append(evts, EventResult{
				Id:            evtBase.Id,
				EventType:     evtBase.EventType,
				EventName:     evtBase.EventName,
				TotalCount:    0,
				ProcessedData: evtBase.ProcessedData,
				InstanceIds:   []int64{},
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

	// Sort and filter top <limit> events
	sort.Sort(evts)
	if len(evts) < limit {
		return evts, nil
	} else {
		return evts[:limit], nil
	}
}

// Get all increasing events by comparing beginning and end of time period
func (es *eventStore) GetIncreasingEvents(start, end time.Time, serviceId int, limit int) (EventResults, error) {
	var evts EventResults
	middle := util.AvgTime(start, end)
	result1, err := es.GetRecentEvents(start, middle, serviceId, limit)
	if err != nil {
		return evts, err
	}
	result2, err := es.GetRecentEvents(middle, end, serviceId, limit)
	if err != nil {
		return evts, err
	}

	// compare average exception count of result1 and result2
	mapResult1 := map[int64]EventResult{}
	for _, v := range result1 {
		mapResult1[v.Id] = v
	}
	for _, v := range result2 {
		if r, ok := mapResult1[v.Id]; !ok {
			// if not in map, then its a new/increasing event
			evts = append(evts, v)
		} else if float64(r.TotalCount) < 1.5*float64(v.TotalCount) {
			// only append if result2 count is 1.5 times greater than result1
			evts = append(evts, v)
		}
	}

	// Sort and filter top <limit> events
	sort.Sort(evts)
	if len(evts) < limit {
		return evts, nil
	} else {
		return evts[:limit], nil
	}
}

// Get the histogram of a single event instance
func (es *eventStore) GetEventHistogram(start, end time.Time, baseId int) (EventResult, error) {
	now := time.Now()
	defer func() {
		metrics.EventStoreLatency("GetEventHistogram", now)
	}()

	var period EventInstancePeriod
	var instance EventInstance
	join := []interface{}{"event_instance_id"}
	filter := []interface{}{
		map[string]interface{}{"start_time": []interface{}{">", start}}, "AND",
		map[string]interface{}{"end_time": []interface{}{"<", end}},
	}

	base, err := es.GetByBaseId(baseId)
	if err != nil {
		return EventResult{}, err
	}
	result := EventResult{
		Id:            base.Id,
		EventType:     base.EventType,
		EventName:     base.EventName,
		TotalCount:    0,
		ProcessedData: base.ProcessedData,
		InstanceIds:   []int64{},
		Datapoints:    EventBins{},
	}

	res, err := es.ds.Query(query.Filter, "event_instance_period", filter, nil, nil, nil, -1, nil, join)
	if err != nil {
		return result, err
	}
	for _, t1 := range res.Return {
		err = util.MapDecode(t1, &period)
		t2, ok := t1["event_instance_id"].(map[string]interface{})
		if !ok {
			continue
		}
		err = util.MapDecode(t2, &instance)
		if int(instance.EventBaseId) != baseId {
			continue
		}

		// group bins
		st := int(period.StartTime.Unix() * 1000)
		if _, ok := result.Datapoints[st]; !ok {
			result.Datapoints[st] = &Bin{Count: 0, Start: st}
		}
		result.Datapoints[st].Count += period.Count
		result.TotalCount += period.Count
		result.InstanceIds = append(result.InstanceIds, instance.Id)
	}

	return result, nil
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

// Return all service ids that appear in event_base
func (es *eventStore) GetServiceIds() ([]int, error) {
	now := time.Now()
	defer func() {
		metrics.EventStoreLatency("GetEventDetailsbyId", now)
	}()

	var result []int
	var base EventBase

	res, err := es.ds.Query(query.Filter, "event_base", nil, nil, nil, nil, -1, nil, nil)

	if err != nil {
		return result, err
	}

	keys := make(map[int]bool)
	for _, v := range res.Return {
		util.MapDecode(v, &base)
		if _, ok := keys[base.ServiceId]; !ok {
			keys[base.ServiceId] = true
			result = append(result, base.ServiceId)
		}
	}
	return result, nil
}

// Return all event group ids that appear in event_base
func (es *eventStore) GetAllGroups() ([]EventGroup, error) {
	now := time.Now()
	defer func() {
		metrics.EventStoreLatency("GetEventDetailsbyId", now)
	}()

	var result []EventGroup
	var group EventGroup

	res, err := es.ds.Query(query.Filter, "event_group", nil, nil, nil, nil, -1, nil, nil)

	if err != nil {
		return result, err
	}

	for _, v := range res.Return {
		util.MapDecode(v, &group)
		result = append(result, group)
	}
	return result, nil
}

// get id of base events which match service_id
func (es *eventStore) GetBaseIds(service_id int) ([]int, error) {
	now := time.Now()
	defer func() {
		metrics.EventStoreLatency("GetServiceIds", now)
	}()

	var result []int
	var base EventBase

	res, err := es.ds.Query(query.Filter, "event_base", nil, nil, nil, nil, -1, nil, nil)

	if err != nil {
		return result, err
	}

	keys := make(map[int64]bool)
	for _, v := range res.Return {
		util.MapDecode(v, &base)
		if _, ok := keys[base.Id]; base.ServiceId == service_id && !ok {
			keys[base.Id] = true
			result = append(result, int(base.Id))
		}
	}
	return result, nil
}

func (es *eventStore) GetByBaseId(base_id int) (EventBase, error) {
	now := time.Now()
	defer func() {
		metrics.EventStoreLatency("GetByBaseId", now)
	}()

	var result EventBase
	pkey := map[string]interface{}{"_id": base_id}
	res, err := es.ds.Query(query.Get, "event_base", nil, nil, nil, pkey, 1, nil, nil)

	if err != nil {
		return result, err
	}

	for _, v := range res.Return {
		util.MapDecode(v, &result)
	}
	return result, nil
}
