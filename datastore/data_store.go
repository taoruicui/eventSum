package datastore

import (
	"context"
	"encoding/json"
	"github.com/ContextLogic/eventsum/config"
	"github.com/ContextLogic/eventsum/metrics"
	. "github.com/ContextLogic/eventsum/models"
	"github.com/ContextLogic/eventsum/rules"
	"github.com/ContextLogic/eventsum/util"
	"github.com/jacksontj/dataman/src/client"
	"github.com/jacksontj/dataman/src/client/direct"
	"github.com/jacksontj/dataman/src/query"
	"github.com/jacksontj/dataman/src/storage_node"
	"github.com/jacksontj/dataman/src/storage_node/metadata"
	"github.com/pkg/errors"
	"io/ioutil"
	"fmt"
	"time"
)

var GlobalRule *rules.Rule

type DataStore interface {
	Query(typ query.QueryType,
		collection string,
		filter interface{},
		record map[string]interface{},
		recordOp map[string]interface{},
		pkey map[string]interface{},
		limit int,
		sort []string,
		join []interface{}) (*query.Result, error)
	AddEventBase(evt *EventBase) error
	AddEventInstance(evt *EventInstance) error
	AddEventInstancePeriod(evt *EventInstancePeriod) error
	AddEventDetail(evt *EventDetail) error
	GetServices() []EventService
	GetServicesMap() map[string]EventService
	GetEnvironments() []EventEnvironment
	GetEnvironmentsMap() map[string]EventEnvironment
	GetGroups() ([]EventGroup, error)
	GetEventsByServiceId(id int) ([]EventBase, error)
	GetEventByHash(hash string) (EventBase, error)
	GetEventDetailsbyId(id int) (EventDetailsResult, error)
	SetGroupId(eventBaseId, eventGroupId int) (EventBase, error)
	GeneralQuery(
		start, end time.Time,
		eventGroupMap, eventBaseMap, serviceIdMap, envIdMap map[int]bool,
	) (EventResults, error)
	AddEventGroup(group EventGroup) (EventGroup, error)
	GetEventTypes(match string) ([]string, error)
}

type postgresStore struct {
	Name   string
	Client *datamanclient.Client

	// Variables stored in memory (for faster access)
	Services            []EventService
	ServicesNameMap     map[string]EventService
	Environments        []EventEnvironment
	EnvironmentsNameMap map[string]EventEnvironment
}

// Create a new dataStore
func NewDataStore(config config.EventsumConfig) (DataStore, error) {
	// Create a connection to Postgres Database through Dataman

	storagenodeConfig, err := storagenode.DatasourceInstanceConfigFromFile(config.DataSourceInstance)
	if err != nil {
		return nil, err
	}

	// Load meta
	meta := &metadata.Meta{}
	metaBytes, err := ioutil.ReadFile(config.DataSourceSchema)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal([]byte(metaBytes), meta)
	if err != nil {
		return nil, err
	}

	// TODO: remove
	storagenodeConfig.SkipProvisionTrim = true

	transport, err := datamandirect.NewStaticDatasourceInstanceTransport(storagenodeConfig, meta)
	if err != nil {
		metrics.DBError("transport")
		return nil, err
	}

	// build services and environment variables from config file
	services := []EventService{}
	servicesNameMap := make(map[string]EventService)
	environments := []EventEnvironment{}
	environmentsNameMap := make(map[string]EventEnvironment)

	for k, v := range config.Services {
		service := EventService{Id: v["service_id"], Name: k}
		services = append(services, service)
		servicesNameMap[k] = service
	}
	for k, v := range config.Environments {
		environment := EventEnvironment{Id: v["environment_id"], Name: k}
		environments = append(environments, environment)
		environmentsNameMap[k] = environment
	}

	client := &datamanclient.Client{Transport: transport}
	return &postgresStore{
		Name:                config.DatabaseName,
		Client:              client,
		Services:            services,
		ServicesNameMap:     servicesNameMap,
		Environments:        environments,
		EnvironmentsNameMap: environmentsNameMap,
	}, nil
}

func (p *postgresStore) Query(typ query.QueryType,
	collection string,
	filter interface{},
	record map[string]interface{},
	recordOp map[string]interface{},
	pkey map[string]interface{},
	limit int,
	sort []string,
	join []interface{}) (*query.Result, error) {
	var res *query.Result
	q := &query.Query{
		Type: typ,
		Args: map[string]interface{}{
			"db":             p.Name,
			"collection":     collection,
			"shard_instance": "public",
		},
	}
	if filter != nil {
		q.Args["filter"] = filter
	}
	if record != nil {
		q.Args["record"] = record
	}
	if recordOp != nil {
		q.Args["record_op"] = recordOp
	}
	if pkey != nil {
		q.Args["pkey"] = pkey
	}
	if limit != -1 {
		q.Args["limit"] = limit
	}
	if sort != nil {
		q.Args["sort"] = sort
	}
	if join != nil {
		q.Args["join"] = join
	}
	res, err := p.Client.DoQuery(context.Background(), q)
	//res, err := &query.Result{}, errors.New("asdf")
	if err != nil {
		metrics.DBError("transport")
		return res, err
	} else if res.Error != "" {
		return res, errors.New(res.Error)
	}
	return res, err
}

func (p *postgresStore) AddEventBase(evt *EventBase) error {
	filter := map[string]interface{}{
		"service_id":          []interface{}{"=", evt.ServiceId},
		"event_type":          []interface{}{"=", evt.EventType},
		"processed_data_hash": []interface{}{"=", evt.ProcessedDataHash},
	}
	res, err := p.Query(query.Filter, "event_base", filter, nil, nil, nil, 1, nil, nil)
	if err != nil {
		metrics.DBError("read")
		return err
	} else if len(res.Return) == 0 {
		//TODO: fix uniqueness constraint
		record := map[string]interface{}{
			"service_id":           evt.ServiceId,
			"event_type":           evt.EventType,
			"event_name":           evt.EventName,
			"event_environment_id": evt.EventEnvironmentId,
			"event_group_id":       evt.EventGroupId,
			"processed_data":       evt.ProcessedData,
			"processed_data_hash":  evt.ProcessedDataHash,
		}

		res, err = p.Query(query.Set, "event_base", nil, record, nil, nil, -1, nil, nil)
		if err != nil {
			metrics.DBError("write")
			return err
		}
	}
	util.MapDecode(res.Return[0], &evt, false)
	return nil
}

func (p *postgresStore) AddEventInstance(evt *EventInstance) error {
	filter := map[string]interface{}{
		"generic_data_hash": []interface{}{"=", evt.GenericDataHash},
	}
	res, err := p.Query(query.Filter, "event_instance", filter, nil, nil, nil, 1, nil, nil)
	if err != nil {
		metrics.DBError("read")
		return err
	} else if len(res.Return) == 0 {
		//TODO: fix uniqueness constraint
		record := map[string]interface{}{
			"event_base_id":        evt.EventBaseId,
			"event_detail_id":      evt.EventDetailId,
			"raw_data":             evt.RawData,
			"generic_data":         evt.GenericData,
			"generic_data_hash":    evt.GenericDataHash,
			"event_environment_id": evt.EventEnvironmentId,
		}
		res, err = p.Query(query.Set, "event_instance", nil, record, nil, nil, -1, nil, nil)
		if err != nil {
			metrics.DBError("write")
			return err
		}
	}
	util.MapDecode(res.Return[0], &evt, false)
	return nil
}

func (p *postgresStore) AddEventInstancePeriod(evt *EventInstancePeriod) error {
	filter := map[string]interface{}{
		"event_instance_id": []interface{}{"=", evt.EventInstanceId},
		"start_time":        []interface{}{"=", evt.StartTime},
		"end_time":          []interface{}{"=", evt.EndTime},
	}
	record := map[string]interface{}{
		"event_instance_id": evt.EventInstanceId,
		"start_time":        evt.StartTime,
		"updated":           evt.Updated,
		"end_time":          evt.EndTime,
		"count":             evt.Count,
		"counter_json":      evt.CounterJson,
	}
	for {
		res, err := p.Query(query.Filter, "event_instance_period", filter, nil, nil, nil, -1, nil, nil)
		if err != nil {
			metrics.DBError("read")
			return err
		} else if len(res.Return) == 0 {
			//TODO: fix uniqueness constraint
			res, err = p.Query(query.Set, "event_instance_period", nil, record, nil, nil, -1, nil, nil)
			if err != nil {
				metrics.DBError("write")
				return err
			}
		} else {
			var tmp EventInstancePeriod
			util.MapDecode(res.Return[0], &tmp, true)
			filter["cas_value"] = []interface{}{"=", tmp.CAS}
			record["count"] = tmp.Count + evt.Count
			// TODO: handle error
			record["counter_json"], _ = GlobalRule.Consolidate(tmp.CounterJson, evt.CounterJson)
			record["cas_value"] = tmp.CAS + 1
			res, err = p.Query(query.Update, "event_instance_period", filter, record, nil, nil, -1, nil, nil)
			// if update failed then CAS failed, must retry
			if err != nil || len(res.Return) == 0 {
				continue
			}
		}
		util.MapDecode(res.Return[0], &evt, false)
		return err
	}
}

func (p *postgresStore) AddEventDetail(evt *EventDetail) error {
	filter := map[string]interface{}{
		"processed_detail_hash": []interface{}{"=", evt.ProcessedDetailHash},
	}
	res, err := p.Query(query.Filter, "event_detail", filter, nil, nil, nil, 1, nil, nil)
	if err != nil {
		metrics.DBError("read")
		return err
	} else if len(res.Return) == 0 {
		//TODO: fix uniqueness constraint
		record := map[string]interface{}{
			"raw_detail":            evt.RawDetail,
			"processed_detail":      evt.ProcessedDetail,
			"processed_detail_hash": evt.ProcessedDetailHash,
		}
		res, err = p.Query(query.Set, "event_detail", nil, record, nil, nil, -1, nil, nil)
		if err != nil {
			metrics.DBError("write")
			return err
		}
	}
	util.MapDecode(res.Return[0], &evt, false)
	return err
}

func (p *postgresStore) GetServices() []EventService {
	return p.Services
}

func (p *postgresStore) GetEnvironments() []EventEnvironment {
	return p.Environments
}

func (p *postgresStore) GetServicesMap() map[string]EventService {
	return p.ServicesNameMap
}

func (p *postgresStore) GetEnvironmentsMap() map[string]EventEnvironment {
	return p.EnvironmentsNameMap
}

// Return all event group ids that appear in event_base
func (p *postgresStore) GetGroups() ([]EventGroup, error) {

	var result []EventGroup
	var group EventGroup

	res, err := p.Query(query.Filter, "event_group", nil, nil, nil, nil, -1, nil, nil)

	if err != nil {
		metrics.DBError("read")
		return result, err
	}

	for _, v := range res.Return {
		util.MapDecode(v, &group, true)
		result = append(result, group)
	}
	return result, nil
}

func (p *postgresStore) GetEventsByServiceId(id int) ([]EventBase, error) {
	var result []EventBase
	var base EventBase

	filter := map[string]interface{}{"service_id": []interface{}{"=", id}}

	res, err := p.Query(query.Filter, "event_base", filter, nil, nil, nil, -1, nil, nil)

	if err != nil {
		metrics.DBError("read")
		return result, err
	}

	for _, v := range res.Return {
		util.MapDecode(v, &base, true)
		result = append(result, base)
	}

	return result, nil
}

// GeneralQuery is used by various handlers (grafana queries,
// web queries, etc). It takes in a time range, as well as query
// params in the form of maps, which will filter out the events.
// Empty maps indicate that there should be no filtering for that
// parameter.
func (p *postgresStore) GeneralQuery(
	start, end time.Time,
	eventGroupMap, eventBaseMap, serviceIdMap, envIdMap map[int]bool) (EventResults, error) {

	now := time.Now()
	defer func() {
		metrics.EventStoreLatency("GetRecentEvents", now)
	}()

	var evts EventResults
	var evtsMap = make(map[int]int)
	var evtsDatapointMap = make(map[int]EventBins) // for storing datapoints map
	join := []interface{}{"event_instance_id", "event_instance_id.event_base_id"}
	filter := []interface{}{
		map[string]interface{}{"updated": []interface{}{">=", start}}, "AND",
		map[string]interface{}{"updated": []interface{}{"<=", end}},
	}
	res, err := p.Query(query.Filter, "event_instance_period", filter, nil, nil, nil, -1, nil, join)
	if err != nil {
		metrics.DBError("read")
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
				Id:                 evtBase.Id,
				EventType:          evtBase.EventType,
				EventName:          evtBase.EventName,
				EventGroupId:       evtBase.EventGroupId,
				EventEnvironmentId: evtBase.EventEnvironmentId,
				TotalCount:         0,
				ProcessedData:      evtBase.ProcessedData,
				InstanceIds:        []int{},
				Datapoints:         []Bin{},
			})
			evtsMap[evtBase.Id] = len(evts) - 1
			evtsDatapointMap[evtBase.Id] = EventBins{}
		}

		start := int(evtPeriod.Updated.Unix() * 1000)
		evt := &evts[evtsMap[evtBase.Id]]
		evt.TotalCount += evtPeriod.Count
		evt.InstanceIds = append(evt.InstanceIds, evtInstance.Id)

		// update datapoints map with new count
		if _, ok := evtsDatapointMap[evtBase.Id][start]; !ok {
			evtsDatapointMap[evtBase.Id][start] = &Bin{Start: start, Count: 0}
		}
		evtsDatapointMap[evtBase.Id][start].Count += evtPeriod.Count
	}

	// turning map into sorted array
	for id, datapoints := range evtsDatapointMap {
		evts[evtsMap[id]].Datapoints = datapoints.ToSlice(1000)
	}

	return evts, nil
}

// Get EventBase by processed_hash
func (p *postgresStore) GetEventByHash(hash string) (EventBase, error) {
	var base EventBase
	filter := map[string]interface{}{"processed_data_hash": []interface{}{"=", hash}}
	res, err := p.Query(query.Get, "event_base", filter, nil, nil, nil, -1, nil, nil)

	if err != nil {
		metrics.DBError("read")
		return base, err
	} else if len(res.Return) == 0 {
		return base, errors.New("No base matches hash")
	} else {
		err = util.MapDecode(res.Return[0], &base, true)
		return base, err
	}
}

// Get the details of a single event instance
func (p *postgresStore) GetEventDetailsbyId(id int) (EventDetailsResult, error) {
	var result EventDetailsResult
	var instance EventInstance
	var detail EventDetail
	var base EventBase
	join := []interface{}{"event_base_id", "event_detail_id"}
	pkey := map[string]interface{}{"_id": id}

	res, err := p.Query(query.Get, "event_instance", nil, nil, nil, pkey, -1, nil, join)

	if err != nil {
		metrics.DBError("read")
		return result, err
	} else if len(res.Return) == 0 {
		return result, errors.New(fmt.Sprintf("no event instance with id %v", id))
	}

	util.MapDecode(res.Return[0], &instance, false)
	if t1, ok := res.Return[0]["event_base_id"].(map[string]interface{}); ok {
		util.MapDecode(t1, &base, false)
		if t2, ok := res.Return[0]["event_detail_id"].(map[string]interface{}); ok {
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

// Sets the event_group_id of a event base, returning error if event base
// does not exist or event group does not exist
func (p *postgresStore) SetGroupId(eventBaseId, eventGroupId int) (EventBase, error) {
	var base EventBase

	filter := map[string]interface{}{
		"_id": []interface{}{"=", eventBaseId},
	}
	record := map[string]interface{}{
		"event_group_id": eventGroupId,
	}

	res, err := p.Query(query.Update, "event_base", filter, record, nil, nil, -1, nil, nil)

	if err != nil {
		metrics.DBError("write")
		return base, err
	} else if len(res.Return) == 0 {
		return base, errors.New(fmt.Sprintf("no event base with id %v", eventBaseId))
	}

	util.MapDecode(res.Return[0], &base, true)
	return base, nil
}

func (p *postgresStore) AddEventGroup(group EventGroup)(EventGroup, error){
	record := map[string]interface{}{
		"name": group.Name,
		"info": group.Info,
	}
	_, err := p.Query(query.Set, "event_group", nil, record, nil,nil, -1, nil, nil)
	if err != nil {
		metrics.DBError("write")
		return group, err
	}
	return group, nil
}

func (p *postgresStore) GetEventTypes(match string) ([]string, error){

	//filter := map[string]interface{}{
	//	"event_type":          []interface{}{"=", match},
	//}
	res, err := p.Query(query.Get, "event_base", nil, nil, nil,nil, -1, nil, nil)

	fmt.Println(res.Return)


	return nil, err
}
