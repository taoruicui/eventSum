package eventsum

import (
	"time"

	conf "github.com/ContextLogic/eventsum/config"
	"github.com/ContextLogic/eventsum/datastore"
	"github.com/ContextLogic/eventsum/log"
	"github.com/ContextLogic/eventsum/metrics"
	. "github.com/ContextLogic/eventsum/models"
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
	now := time.Now()
	defer func() {
		metrics.EventStoreLatency("SummarizeBatchEvents", now)
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

	// Event classes are maps for quick access
	var eventBases = make(map[string]EventBase)
	var eventInstances = make(map[string]EventInstance)
	var eventInstancePeriods = make(map[KeyEventPeriod]EventInstancePeriod)
	var eventDetails = make(map[string]EventDetail)
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

		// Each hash should be unique in the database
		if _, ok := eventBases[processedDataHash]; !ok {
			eventBases[processedDataHash] = EventBase{
				ServiceId:          serviceNameMap[event.Service].Id,
				EventType:          event.Type,
				EventName:          event.Name,
				EventEnvironmentId: envNameMap[event.Environment].Id,
				ProcessedData:      processedData,
				ProcessedDataHash:  processedDataHash,
			}
		}

		if _, ok := eventInstances[genericDataHash]; !ok {
			eventInstances[genericDataHash] = EventInstance{
				ProcessedDataHash:   processedDataHash,   // Used to reference event_base_id later
				ProcessedDetailHash: processedDetailHash, // Used to reference event_detail_id later
				RawData:             rawEvent.Data,
				GenericData:         genericData,
				GenericDataHash:     genericDataHash,
				EventEnvironmentId:  envNameMap[event.Environment].Id,
			}
		}

		// The unique key should be the raw data, and the time period,
		// since the count should keep track of an event instance in a certain time frame.
		t, err := time.Parse(es.timeFormat, event.Timestamp)
		startTime, endTime := util.FindBoundingTime(t, es.timeInterval)
		key := KeyEventPeriod{
			RawDataHash: genericDataHash,
			StartTime:   startTime,
		}
		if _, ok := eventInstancePeriods[key]; !ok {
			eventInstancePeriods[key] = EventInstancePeriod{
				StartTime:   startTime,
				Updated:     t,
				EndTime:     endTime,
				RawDataHash: genericDataHash, // Used to reference event_instance_id later
				Count:       0,
				CounterJson: make(map[string]interface{}),
			}
		}
		e := eventInstancePeriods[key]
		e.Count++
		e.CounterJson, _ = globalRule.ProcessGrouping(rawEvent, e.CounterJson)
		eventInstancePeriods[key] = e

		if _, ok := eventDetails[processedDetailHash]; !ok {
			eventDetails[processedDetailHash] = EventDetail{
				RawDetail:           rawDetail,
				ProcessedDetail:     processedDetail,
				ProcessedDetailHash: processedDetailHash,
			}
		}
	}

	// Add the event bases into the db
	for k, base := range eventBases {
		if err := es.ds.AddEventBase(&base); err != nil {
			es.log.EventLog().LogData(base)
			es.log.App().Errorf("Error while inserting events: %v", err)
		}
		eventBases[k] = base
	}

	for k, detail := range eventDetails {
		if err := es.ds.AddEventDetail(&detail); err != nil {
			es.log.EventLog().LogData(detail)
			es.log.App().Errorf("Error while inserting event data: %v", err)
		}
		eventDetails[k] = detail
	}

	// Add the ids generated from above
	for k, instance := range eventInstances {
		instance.EventBaseId = eventBases[instance.ProcessedDataHash].Id
		instance.EventDetailId = eventDetails[instance.ProcessedDetailHash].Id
		// log instance if there was an error adding event base or event details
		if instance.EventBaseId == 0 {
			es.log.EventLog().LogData(instance)
		}
		if instance.EventDetailId == 0 {
			es.log.EventLog().LogData(instance)
		}
		eventInstances[k] = instance
	}

	for k, instance := range eventInstances {
		if err := es.ds.AddEventInstance(&instance); err != nil {
			es.log.EventLog().LogData(instance)
			es.log.App().Errorf("Error while inserting event instances: %v", err)
		}
		eventInstances[k] = instance
	}

	// Add the ids generated from above
	for k, period := range eventInstancePeriods {
		period.EventInstanceId = eventInstances[period.RawDataHash].Id

		// log period if there was an error adding event instance
		if period.EventInstanceId == 0 {
			es.log.EventLog().LogData(period)
		}
		eventInstancePeriods[k] = period
	}

	for k, period := range eventInstancePeriods {
		if err := es.ds.AddEventInstancePeriod(&period); err != nil {
			es.log.EventLog().LogData(period)
			es.log.App().Errorf("Error while inserting event time periods: %v", err)
		}
		eventInstancePeriods[k] = period
	}
}

func (es *eventStore) GeneralQuery(
	start, end time.Time,
	eventGroupMap, eventBaseMap, serviceIdMap, envIdMap map[int]bool) (EventResults, error) {

	now := time.Now()
	defer func() {
		metrics.EventStoreLatency("GetRecentEvents", now)
	}()

	return es.ds.GeneralQuery(start, end, eventGroupMap, eventBaseMap, serviceIdMap, envIdMap)
}

func (es *eventStore) GetEventByHash(hash string) (EventBase, error) {
	now := time.Now()
	defer func() {
		metrics.EventStoreLatency("GetEventByHash", now)
	}()

	return es.ds.GetEventByHash(hash)
}

func (es *eventStore) GetEventDetailsbyId(id int) (EventDetailsResult, error) {
	now := time.Now()
	defer func() {
		metrics.EventStoreLatency("GetEventDetailsbyId", now)
	}()

	return es.ds.GetEventDetailsbyId(id)
}

func (es *eventStore) SetGroupId(eventBaseId int, groupId int) (EventBase, error) {
	now := time.Now()
	defer func() {
		metrics.EventStoreLatency("SetGroupId", now)
	}()

	return es.ds.SetGroupId(eventBaseId, groupId)
}

func (es *eventStore) AddEventGroup(group EventGroup) (EventGroup, error) {
	now := time.Now()
	defer func() {
		metrics.EventStoreLatency("AddEventGroup", now)
	}()
	return es.ds.AddEventGroup(group)
}

func (es *eventStore) GetEventsByGroup(group_id int, group_name string) ([]EventBase, error) {
	now := time.Now()
	defer func() {
		metrics.EventStoreLatency("GetEventByGroup", now)
	}()
	return es.ds.GetEventsByGroup(group_id, group_name)
}

func (es *eventStore) ModifyEventGroup(name string, info string, newName string) error {
	now := time.Now()
	defer func() {
		metrics.EventStoreLatency("ModifyEventGroup", now)
	}()
	return es.ds.ModifyEventGroup(name, info, newName)
}

func (es *eventStore) DeleteEventGroup(name string) error {
	now := time.Now()
	defer func() {
		metrics.EventStoreLatency("DeleteEventGroup", now)
	}()
	return es.ds.DeleteEventGroup(name)
}

func (es *eventStore) CountEvents(filter map[string]string) (CountStat, error) {
	now := time.Now()
	defer func() {
		metrics.EventStoreLatency("CountEvents", now)
	}()
	return es.ds.CountEvents(filter)
}
