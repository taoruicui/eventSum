package datastore

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"time"

	"sort"

	"bytes"

	"github.com/ContextLogic/eventsum/config"
	"github.com/ContextLogic/eventsum/metrics"
	. "github.com/ContextLogic/eventsum/models"
	"github.com/ContextLogic/eventsum/rules"
	"github.com/ContextLogic/eventsum/util"
	"github.com/jacksontj/dataman/src/client"
	"github.com/jacksontj/dataman/src/query"
	"github.com/jacksontj/dataman/src/storage_node"
	"github.com/jacksontj/dataman/src/storage_node/metadata"
	"github.com/pkg/errors"

	"database/sql"

	_ "github.com/lib/pq"
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
	GetRegionsMap() map[string]int
	GetGroups() ([]EventGroup, error)
	GetGroupIdByGroupName(name string) (string, error)
	GetEventsByServiceId(id int) ([]EventBase, error)
	GetEventsByCriteria(serviceId string, eventType string, eventName string, environmentId string) ([]EventBase, error)
	GetEventByHash(hash string) (EventBase, error)
	GetEventDetailsbyId(id int) (EventDetailsResult, error)
	SetGroupId(eventBaseId, eventGroupId int) (EventBase, error)
	GeneralQuery(
		start, end time.Time,
		eventGroupMap, eventBaseMap, serviceIdMap, envIdMap map[int]bool,
	) (EventResults, error)
	GrafanaQuery(start, end time.Time, eventGroupId, eventBaseId, serviceId, envId []int, eventName,
		eventType []string) (EventResults, error)
	AddEventGroup(group EventGroup) (EventGroup, error)
	ModifyEventGroup(name string, info string, newName string) error
	DeleteEventGroup(group_id int, name string) error
	GetEventTypes(statement string) ([]string, error)
	GetEventNames(statement string) ([]string, error)
	GetEventsByGroup(group_id int, group_name string) ([]EventBase, error)
	GetDBConfig() *storagenode.DatasourceInstanceConfig
	CountEvents(map[string]string) (CountStat, error)
	OpsdbQuery(from string, to string, envId string, serviceId string, groupId string, regionID int) ([]OpsdbResult, error)
	FindEventBaseId(evt EventBase) (int64, error)
	AddBaseEvent(evt EventBase) (int64, error)
	FindEventInstanceId(evt EventInstance) (int64, error)
	AddInstanceEvent(evt EventInstance) (int64, error)
	FindEventDetailId(evt EventDetail) (int64, error)
	AddEventDetails(evtDetail EventDetail) (int64, error)
	UpdateEventInstancePeriod(evt EventInstancePeriod) error
	AddEventInstancePeriods(evt EventInstancePeriod) error
	Test(from string, to string, evtId int) (DataPointArrays, error)
}

type postgresStore struct {
	Name     string
	Client   *datamanclient.Client
	DBConfig *storagenode.DatasourceInstanceConfig
	DB       *sql.DB

	// Variables stored in memory (for faster access)
	Services            []EventService
	ServicesNameMap     map[string]EventService
	Environments        []EventEnvironment
	EnvironmentsNameMap map[string]EventEnvironment
	RegionsMap          map[string]int
	Region              int
}

// Create a new dataStore
func NewDataStore(c config.EventsumConfig) (DataStore, error) {
	// Create a connection to Postgres Database through Dataman

	storagenodeConfig, err := storagenode.DatasourceInstanceConfigFromFile(c.DataSourceInstance)
	if err != nil {
		return nil, err
	}

	// Load meta
	meta := &metadata.Meta{}
	metaBytes, err := ioutil.ReadFile(c.DataSourceSchema)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal([]byte(metaBytes), meta)
	if err != nil {
		return nil, err
	}

	// TODO: remove
	storagenodeConfig.SkipProvisionTrim = true

	transport, err := storagenode.NewStaticDatasourceInstanceTransport(storagenodeConfig, meta)
	if err != nil {
		metrics.DBError("transport")
		return nil, err
	}

	// build services and environment variables from config file
	services := []EventService{}
	servicesNameMap := make(map[string]EventService)
	environments := []EventEnvironment{}
	environmentsNameMap := make(map[string]EventEnvironment)

	for k, v := range c.Services {
		service := EventService{Id: v["service_id"], Name: k}
		services = append(services, service)
		servicesNameMap[k] = service
	}
	for k, v := range c.Environments {
		environment := EventEnvironment{Id: v["environment_id"], Name: k}
		environments = append(environments, environment)
		environmentsNameMap[k] = environment
	}

	connStr, err := config.ParseDataSourceInstanceConfig(c.DataSourceInstance)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		metrics.DBError("transport")
		return nil, err
	}

	regionID := c.RegionsMap[c.Region]

	client := &datamanclient.Client{Transport: transport}
	return &postgresStore{
		Name:                c.DatabaseName,
		Client:              client,
		DBConfig:            storagenodeConfig,
		Services:            services,
		ServicesNameMap:     servicesNameMap,
		Environments:        environments,
		EnvironmentsNameMap: environmentsNameMap,
		DB:                  db,
		Region:              regionID,
		RegionsMap:          c.RegionsMap,
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
			"event_message":        evt.EventMessage,
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

func (p *postgresStore) GetRegionsMap() map[string]int {
	return p.RegionsMap
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

func (p *postgresStore) GetGroupIdByGroupName(name string) (string, error) {
	filter := map[string]interface{}{"name": []interface{}{"=", name}}
	var result string
	res, err := p.Query(query.Filter, "event_group", filter, nil, nil, nil, -1, nil, nil)
	if err != nil {
		metrics.DBError("read")
		return result, err
	} else if len(res.Return) == 0 {
		return result, err
	}

	result = strconv.FormatInt(res.Return[0]["_id"].(int64), 10)

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
	join := []interface{}{".event_instance_id", ".event_instance_id.event_base_id"}
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
		t2, ok := t1["event_instance_id."].([]map[string]interface{})
		if !ok {
			continue
		}
		for _, t := range t2 {
			err = util.MapDecode(t, &evtInstance, true)
			t3, ok := t["event_base_id."].([]map[string]interface{})
			if !ok {
				continue
			}

			for _, t = range t3 {
				err = util.MapDecode(t, &evtBase, true)

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

		}

	}

	// turning map into sorted array
	for id, datapoints := range evtsDatapointMap {
		evts[evtsMap[id]].Datapoints = datapoints.ToSlice(1000)
	}

	return evts, nil
}

func (p *postgresStore) GrafanaQuery(start, end time.Time, eventGroupId, eventBaseId, serviceId, envId []int, eventName, eventType []string) (EventResults, error) {

	now := time.Now()
	defer func() {
		metrics.EventStoreLatency("GetRecentEvents", now)
	}()

	join := []interface{}{".event_instance_id", ".event_instance_id.event_base_id"}
	filter := []interface{}{
		map[string]interface{}{"updated": []interface{}{">=", start}}, "AND",
		map[string]interface{}{"updated": []interface{}{"<=", end}},
	}

	res, err := p.Query(query.Filter, "event_instance_period", filter, nil, nil, nil, -1, nil, join)
	if err != nil {
		metrics.DBError("read")
		return nil, err
	}

	var evts EventResults
	var evtsMap = make(map[int]int)
	var evtsDatapointMap = make(map[int]EventBins) // for storing datapoints map

	for _, r := range res.Return {
		evtPeriod := EventInstancePeriod{}
		evtInstance := EventInstance{}
		evtBase := EventBase{}
		err = util.MapDecode(r, &evtPeriod, true)
		for _, r2 := range r["event_instance_id."].([]map[string]interface{}) {
			err = util.MapDecode(r2, &evtInstance, true)
			for _, r3 := range r2["event_base_id."].([]map[string]interface{}) {

				err = util.MapDecode(r3, &evtBase, true)
				if len(eventGroupId) != 0 && !util.IsInList(eventGroupId, evtBase.EventGroupId, nil, "") {
					continue
				}
				if len(eventBaseId) != 0 && !util.IsInList(eventBaseId, evtBase.Id, nil, "") {
					continue
				}
				if len(serviceId) != 0 && !util.IsInList(serviceId, evtBase.ServiceId, nil, "") {
					continue
				}
				if len(envId) != 0 && !util.IsInList(envId, evtBase.EventEnvironmentId, nil, "") {
					continue
				}
				if len(eventName) != 0 && !util.IsInList(nil, 0, eventName, evtBase.EventName) {
					continue
				}
				if len(eventType) != 0 && !util.IsInList(nil, 0, eventType, evtBase.EventType) {
					continue
				}

				// Aggregate similar events
				if _, ok := evtsMap[evtBase.Id]; !ok {
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
		}

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
	join := []interface{}{".event_base_id", ".event_detail_id"}
	pkey := map[string]interface{}{"_id": id}

	res, err := p.Query(query.Get, "event_instance", nil, nil, nil, pkey, -1, nil, join)

	if err != nil {
		metrics.DBError("read")
		return result, err
	} else if len(res.Return) == 0 {
		return result, errors.New(fmt.Sprintf("no event instance with id %v", id))
	}
	util.MapDecode(res.Return[0], &instance, false)
	if t1, ok := res.Return[0]["event_base_id."]; ok {
		util.MapDecode(t1.([]map[string]interface{})[0], &base, false)
		if t2, ok := res.Return[0]["event_detail_id."]; ok {
			util.MapDecode(t2.([]map[string]interface{})[0], &detail, false)
		}
	}

	filter := map[string]interface{}{"event_instance_id": []interface{}{"=", id}}
	res, err = p.Query(query.Filter, "event_instance_period", filter, nil, nil, nil, -1, []string{"updated"}, nil)
	if err != nil {
		metrics.DBError("read")
		return result, err
	} else if len(res.Return) == 0 {
		return result, errors.New(fmt.Sprintf("no event instance with id %v", id))
	}
	var firstEvtInsPeriod, lastEvtInsPeriod EventInstancePeriod
	util.MapDecode(res.Return[0], &firstEvtInsPeriod, false)
	util.MapDecode(res.Return[len(res.Return)-1], &lastEvtInsPeriod, false)
	firstSeen := firstEvtInsPeriod.Updated.Add(-7 * time.Duration(time.Hour)).Format("2006-01-02 15:04:05")
	lastSeen := lastEvtInsPeriod.Updated.Add(-7 * time.Duration(time.Hour)).Format("2006-01-02 15:04:05")

	instance.RawData.RawMessage = instance.RawData.Message
	//fmt.Println(instance.RawData.Message)

	result = EventDetailsResult{
		ServiceId:  base.ServiceId,
		EventType:  base.EventType,
		EventName:  base.EventName,
		RawData:    instance.RawData,
		RawDetails: detail.RawDetail,
		LastSeen:   lastSeen,
		FirstSeen:  firstSeen,
	}

	//for _, frame := range result.RawData.(EventData).Raw.(map[string]interface{})["frames"].([]interface{}) {
	//	var buffer bytes.Buffer
	//
	//	if preContext, ok := frame.(map[string]interface{})["pre_context"].([]interface{}); ok {
	//		for _, s := range preContext {
	//			buffer.WriteString(s.(string))
	//			buffer.WriteString("\n")
	//		}
	//	} else {
	//		continue
	//	}
	//
	//	if contextLine, ok := frame.(map[string]interface{})["context_line"].(string); ok {
	//		buffer.WriteString(contextLine)
	//		buffer.WriteString("\n")
	//	} else {
	//		continue
	//	}
	//
	//	if postContext, ok := frame.(map[string]interface{})["post_context"].([]interface{}); ok {
	//		for _, s := range postContext {
	//			buffer.WriteString(s.(string))
	//			buffer.WriteString("\n")
	//		}
	//	} else {
	//		continue
	//	}
	//
	//	frame.(map[string]interface{})["code_snippet"] = buffer.String()
	//	frame.(map[string]interface{})["start_lineno"] = int(frame.(map[string]interface{})["lineno"].(float64)) - len(frame.(map[string]interface{})["pre_context"].([]interface{}))
	//	frame.(map[string]interface{})["highlight_lineno"] = len(frame.(map[string]interface{})["pre_context"].([]interface{})) + 1
	//}

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

//given a group, add it into DB
func (p *postgresStore) AddEventGroup(group EventGroup) (EventGroup, error) {
	record := map[string]interface{}{
		"name": group.Name,
		"info": group.Info,
	}
	_, err := p.Query(query.Set, "event_group", nil, record, nil, nil, -1, nil, nil)
	if err != nil {
		metrics.DBError("write")
		return group, err
	}
	return group, nil
}

//modify an existing group name and info given an old group name
func (p *postgresStore) ModifyEventGroup(name string, info string, newName string) error {

	filter := map[string]interface{}{
		"name": []interface{}{"=", name},
	}
	record := map[string]interface{}{
		"name": newName,
	}
	if info != "" {
		record["info"] = info
	}
	_, err := p.Query(query.Update, "event_group", filter, record, nil, nil, -1, nil, nil)
	if err != nil {
		metrics.DBError("write")
		return err
	}
	return nil
}

//given a group name, delete the corresponding group in DB
func (p *postgresStore) DeleteEventGroup(group_id int, name string) error {

	if group_id == 0 || name == "default" {
		return errors.New("cannot delete default group")
	}

	//find group id if group id is not given while name is given
	if group_id == -1 {
		filter := map[string]interface{}{
			"name": []interface{}{"=", name},
		}
		res, err := p.Query(query.Filter, "event_group", filter, nil, nil, nil, -1, nil, nil)
		if err != nil {
			metrics.DBError("read")
			return err
		} else if len(res.Return) == 0 {
			return errors.New(fmt.Sprintf("no group with name %s", name))
		}
		group_id64 := res.Return[0]["_id"].(int64)
		group_id = int(group_id64)
	}

	filter := map[string]interface{}{
		"event_group_id": []interface{}{"=", group_id},
	}
	record := map[string]interface{}{
		"event_group_id": 0,
	}

	_, err := p.Query(query.Update, "event_base", filter, record, nil, nil, -1, nil, nil)
	if err != nil {
		metrics.DBError("write")
		return err
	}

	pkey := map[string]interface{}{"_id": group_id}

	_, err = p.Query(query.Delete, "event_group", nil, nil, nil, pkey, -1, nil, nil)
	if err != nil {
		metrics.DBError("write")
		return err
	}
	return nil

}

//given event types, filter out the events
func (p *postgresStore) GetEventTypes(statement string) ([]string, error) {
	var result []string

	if strings.Contains(statement, "=") {
		statement = strings.Split(statement, "=")[1]
		filter := map[string]interface{}{
			"event_type": []interface{}{"=", statement},
		}
		res, err := p.Query(query.Filter, "event_base", filter, nil, nil, nil, -1, nil, nil)
		if err != nil {
			metrics.DBError("read")
			return result, err
		} else if len(res.Return) == 0 {
			return result, errors.New(fmt.Sprintf("no event type with %s", statement))
		}
		for _, r := range res.Return {
			result = append(result, fmt.Sprintf("%s", r["event_type"]))
		}
		return result, nil
	} else {
		statement = strings.Split(statement, ".contains.")[1]
		res, err := p.Query(query.Filter, "event_base", nil, nil, nil, nil, -1, nil, nil)
		if err != nil {
			metrics.DBError("read")
			return result, err
		}
		for _, r := range res.Return {
			tmp := fmt.Sprintf("%s", r["event_type"])
			if strings.Contains(tmp, statement) {
				result = append(result, tmp)
			}
		}
		return result, nil
	}
}

//given event names, filter out the events
func (p *postgresStore) GetEventNames(statement string) ([]string, error) {
	var result []string

	if strings.Contains(statement, "=") {
		statement = strings.Split(statement, "=")[1]
		filter := map[string]interface{}{
			"event_name": []interface{}{"=", statement},
		}
		res, err := p.Query(query.Filter, "event_base", filter, nil, nil, nil, -1, nil, nil)
		if err != nil {
			metrics.DBError("read")
			return result, err
		} else if len(res.Return) == 0 {
			return result, errors.New(fmt.Sprintf("no event name with %s", statement))
		}
		for _, r := range res.Return {
			result = append(result, fmt.Sprintf("%s", r["event_name"]))
		}
		return result, nil
	} else {
		statement = strings.Split(statement, ".contains.")[1]
		res, err := p.Query(query.Filter, "event_base", nil, nil, nil, nil, -1, nil, nil)
		if err != nil {
			metrics.DBError("read")
			return result, err
		}
		for _, r := range res.Return {
			tmp := fmt.Sprintf("%s", r["event_name"])
			if strings.Contains(tmp, statement) {
				result = append(result, tmp)
			}
		}
		return result, nil
	}
}

//given group names, filter out events
func (p *postgresStore) GetEventsByGroup(group_id int, group_name string) ([]EventBase, error) {
	var evts []EventBase

	if group_name != "" {
		nameFilter := map[string]interface{}{
			"name": []interface{}{"=", "test"},
		}
		res, err := p.Query(query.Filter, "event_group", nameFilter, nil, nil, nil, -1, nil, nil)
		if err != nil {
			metrics.DBError("read")
			return evts, err
		} else if len(res.Return) == 0 {
			return evts, nil
		}
		group_id, _ = strconv.Atoi(fmt.Sprintf("%v", res.Return[0]["_id"]))
	}

	idFilter := map[string]interface{}{
		"event_group_id": []interface{}{"=", group_id},
	}

	res, err := p.Query(query.Filter, "event_base", idFilter, nil, nil, nil, -1, nil, nil)
	if err != nil {
		metrics.DBError("read")
		return evts, err
	}
	var evt EventBase
	for _, r := range res.Return {
		if err := util.MapDecode(r, &evt, true); err != nil {
			return evts, err
		}
		evts = append(evts, evt)
	}
	return evts, nil
}

//given a collection of filters, filter out the events and return their base IDs.
func (p *postgresStore) GetEventsByCriteria(serviceId string, eventType string, eventName string, environmentId string) ([]EventBase, error) {
	var evts []EventBase
	var filter = make(map[string]interface{})
	if serviceId != "" {
		sid, _ := strconv.Atoi(serviceId)
		filter["service_id"] = []interface{}{"=", sid}
	}
	if eventType != "" {
		filter["event_type"] = []interface{}{"=", eventType}
	}
	if eventName != "" {
		filter["event_name"] = []interface{}{"=", eventName}
	}
	if environmentId != "" {
		eid, _ := strconv.Atoi(environmentId)
		filter["event_environment_id"] = []interface{}{"=", eid}
	}
	res, err := p.Query(query.Filter, "event_base", filter, nil, nil, nil, -1, nil, nil)
	if err != nil {
		metrics.DBError("read")
		return evts, err
	}
	var evt EventBase
	for _, r := range res.Return {
		if err := util.MapDecode(r, &evt, true); err != nil {
			return evts, err
		}
		evts = append(evts, evt)
	}

	return evts, nil
}

func (p *postgresStore) GetDBConfig() *storagenode.DatasourceInstanceConfig {
	return p.DBConfig
}

//given a collection of filters, filter out the result exception and aggregate the count
//and calculate the increase, count/min.
func (p *postgresStore) CountEvents(filterMap map[string]string) (CountStat, error) {

	var result CountStat
	filter := []interface{}{
		map[string]interface{}{
			"event_instance_id": []interface{}{"=", filterMap["id"]},
		},
	}

	layout := "2006-01-02 15:04:05"
	var start, end time.Time
	var err error
	if filterMap["start_time"] != "" {
		start, err = time.Parse(layout, filterMap["start_time"])
		if err != nil {
			return result, err
		}
	} else {
		start, _ = time.Parse(layout, "2006-01-02 00:00:00")
	}
	filter[0].(map[string]interface{})["updated"] = []interface{}{">=", start}

	if filterMap["end_time"] != "" {
		end, err = time.Parse(layout, filterMap["end_time"])
		if err != nil {
			return result, err
		}
	} else {
		end = time.Now()
	}
	filter = append(filter, "AND")
	filter = append(filter, map[string]interface{}{
		"updated": []interface{}{"<=", end},
	})

	res, err := p.Query(query.Filter, "event_instance_period", filter, nil, nil, nil, -1, nil, nil)

	if err != nil {
		metrics.DBError("read")
		return result, err
	} else if len(res.Return) == 0 {
		return result, nil
	}

	var evt EventInstancePeriod
	sort.Sort(util.ByTime(res.Return))
	for _, e := range res.Return {
		if err := util.MapDecode(e, &evt, true); err != nil {
			return result, err
		}
		result.Count += evt.Count
	}
	mostRecent := res.Return[0]
	var secondRecent map[string]interface{}
	if len(res.Return) > 1 {
		secondRecent = res.Return[1]
	}
	result.Increase = util.GetExptPerMinIncrease(mostRecent, secondRecent)

	diff := end.Sub(start).Minutes()
	result.CountPerMin = float64(result.Count) / diff
	return result, nil
}

func (p *postgresStore) OpsdbQuery(start string, end string, envId string, serviceId string, groupId string, regionID int) ([]OpsdbResult, error) {
	////var tmp = make(map[int]OpsdbResult)
	////var dataPointTmp = make(map[int]DataPoint)
	////var result []OpsdbResult
	////
	////var start, end time.Time
	////var err error
	////if filterMap["start_time"] != "" {
	////	start, err = util.EpochToTime(filterMap["start_time"])
	////	if err != nil {
	////		return result, err
	////	}
	////} else {
	////	start = time.Now()
	////}
	////
	////if filterMap["end_time"] != "" {
	////	end, err = util.EpochToTime(filterMap["end_time"])
	////	if err != nil {
	////		return result, err
	////	}
	////} else {
	////	end = time.Now()
	////}
	////fmt.Println(start, end)
	////filter := []interface{}{
	////	map[string]interface{}{"updated": []interface{}{">=", start}}, "AND",
	////	map[string]interface{}{"updated": []interface{}{"<=", end}},
	////}
	////
	////join := []interface{}{".event_instance_id", ".event_instance_id.event_base_id"}
	////res, err := p.Query(query.Filter, "event_instance_period", filter, nil, nil, nil, -1, nil, join)
	////if err != nil {
	////	metrics.DBError("read")
	////} else if len(res.Return) == 0 {
	////	return result, nil
	////}
	////
	////fmt.Println(len(res.Return))
	////c := 0
	////
	////for _, t1 := range res.Return {
	////	evtPeriod := EventInstancePeriod{}
	////	evtInstance := EventInstance{}
	////	evtBase := EventBase{}
	////	evtGroup := EventGroup{}
	////	opsdbResult := OpsdbResult{}
	////	err = util.MapDecode(t1, &evtPeriod, true)
	////	t2, ok := t1["event_instance_id."].([]map[string]interface{})
	////	if !ok {
	////		fmt.Println(t1)
	////		continue
	////	}
	////	for _, t := range t2 {
	////		err = util.MapDecode(t, &evtInstance, true)
	////		t3, ok := t["event_base_id."].([]map[string]interface{})
	////		if !ok {
	////			continue
	////		}
	////		for _, t = range t3 {
	////			err = util.MapDecode(t, &evtBase, true)
	////			if val, ok := filterMap["service"]; ok && strconv.Itoa(evtBase.ServiceId) != val {
	////				continue
	////			}
	////			if val, ok := filterMap["environment"]; ok && strconv.Itoa(evtBase.EventEnvironmentId) != val {
	////				continue
	////			}
	////			if val, ok := filterMap["group"]; ok && strconv.Itoa(evtBase.EventGroupId) != val {
	////				continue
	////			}
	////			c = c + 1
	////
	////			if val, ok := tmp[evtBase.Id]; !ok {
	////				opsdbResult.EventInstanceId = evtInstance.Id
	////				opsdbResult.EventBaseId = evtBase.Id
	////				opsdbResult.EvtName = evtBase.EventName
	////				opsdbResult.EvtMessage = evtBase.ProcessedData.Message
	////				opsdbResult.LastSeen = evtPeriod.Updated
	////				opsdbResult.Count = evtPeriod.Count
	////				opsdbResult.Group = evtGroup.Name
	////				tmp[evtBase.Id] = opsdbResult
	////
	////				dataPoint := DataPoint{}
	////				dataPoint.Count = []int{evtPeriod.Count}
	////				dataPoint.TimeStamp = []time.Time{evtPeriod.Updated}
	////				dataPointTmp[evtBase.Id] = dataPoint
	////
	////			} else {
	////				v := dataPointTmp[evtBase.Id]
	////				v.Update(evtPeriod.Count, evtPeriod.Updated)
	////				dataPointTmp[evtBase.Id] = v
	////				val.Count += evtPeriod.Count
	////				if evtPeriod.Updated.Sub(val.LastSeen) > 0 {
	////					val.SetUpdate(evtPeriod.Updated)
	////				}
	////				val.SetCount(val.Count)
	////
	////			}
	////
	////		}
	////	}
	//}
	//
	//fmt.Println(c)
	//for _, val := range tmp {
	//	result = append(result, val)
	//}
	//
	//return result, nil
	var tmp = make(map[int]OpsdbResult)
	var opsdbResult []OpsdbResult
	var sqlString string
	if regionID == 1 {
		sqlString = fmt.Sprintf(
			"select event_instance_id, event_base_id, event_name, updated, event_instance_period.count, event_message, event_group.name, event_detail.raw_detail, created_at "+
				"from event_instance_period "+
				"join event_instance on event_instance_id = event_instance._id "+
				"join event_base on event_base_id = event_base._id "+
				"join event_group on event_base.event_group_id = event_group._id "+
				"join event_detail on event_instance.event_detail_id = event_detail._id "+
				"where updated >= '%s' "+
				"and updated <= '%s' "+
				"and event_base.event_environment_id = %s "+
				"and service_id = %s "+
				"and event_group_id = %s "+
				"and (region_id = %d or region_id is NULL);", start, end, envId, serviceId, groupId, regionID)
	} else {
		sqlString = fmt.Sprintf(
			"select event_instance_id, event_base_id, event_name, updated, event_instance_period.count, event_message, event_group.name, event_detail.raw_detail, created_at "+
				"from event_instance_period "+
				"join event_instance on event_instance_id = event_instance._id "+
				"join event_base on event_base_id = event_base._id "+
				"join event_group on event_base.event_group_id = event_group._id "+
				"join event_detail on event_instance.event_detail_id = event_detail._id "+
				"where updated >= '%s' "+
				"and updated <= '%s' "+
				"and event_base.event_environment_id = %s "+
				"and service_id = %s "+
				"and event_group_id = %s "+
				"and region_id = %d;", start, end, envId, serviceId, groupId, regionID)
	}

	rows, err := p.DB.Query(sqlString)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		opsdbResult := OpsdbResult{}

		var eventBaseId, eventInstanceId int
		var updated time.Time
		var count int
		var eventName, eventMessage, groupName, lastSeen, eventDetail, firstSeen string
		var firstSeenString sql.NullString

		if err := rows.Scan(&eventInstanceId, &eventBaseId, &eventName, &updated, &count, &eventMessage, &groupName, &eventDetail, &firstSeenString); err != nil {
			return nil, err
		}

		lastSeen = updated.Add(-7 * time.Duration(time.Hour)).Format("2006-01-02 15:04:05")

		if firstSeenString.Valid {
			firstSeenTime, _ := time.Parse("2006-01-02T15:04:05Z", firstSeenString.String)
			firstSeen = firstSeenTime.Add(-7 * time.Duration(time.Hour)).Format("2006-01-02 15:04:05")
		}

		if val, ok := tmp[eventBaseId]; !ok {
			opsdbResult.EventInstanceId = eventInstanceId
			opsdbResult.EventBaseId = eventBaseId
			opsdbResult.EvtName = eventName
			opsdbResult.EvtMessage = eventMessage
			opsdbResult.LastSeen = lastSeen
			opsdbResult.CountSum = count
			opsdbResult.Group = groupName
			opsdbResult.Count = []int{count}
			opsdbResult.TimeStamp = []string{lastSeen}
			opsdbResult.EvtDetails = eventDetail
			opsdbResult.FirstSeen = firstSeen
			tmp[eventBaseId] = opsdbResult
		} else {
			val.Update(count, lastSeen)
			val.CountSum += count
			if lastSeen > val.LastSeen {
				val.SetUpdate(lastSeen)
			}
			val.SetCount(val.CountSum)
			tmp[eventBaseId] = val
		}

	}
	for _, val := range tmp {
		opsdbResult = append(opsdbResult, val)
	}

	return opsdbResult, nil

}

func (p *postgresStore) Test(from string, to string, evtId int) (DataPointArrays, error) {
	res := DataPointArrays{}

	var sqlString = fmt.Sprintf(
		"select updated, event_instance_period.count "+
			"from event_instance_period "+
			"join event_instance on event_instance_id = event_instance._id "+
			"where updated >= '%s' "+
			"and updated <= '%s' "+
			"and event_instance_id = %d", from, to, evtId)

	rows, err := p.DB.Query(sqlString)
	if err != nil {
		return res, err
	}

	defer rows.Close()

	var counts, timeStamps bytes.Buffer
	counts.WriteString("['data1',")
	timeStamps.WriteString("['x1',")

	for rows.Next() {
		var updated time.Time
		var count int

		if err := rows.Scan(&updated, &count); err != nil {
			return res, err
		}

		lastSeen := updated.Add(-7 * time.Duration(time.Hour)).Format("2006-01-02 15:04:05")

		counts.WriteString(fmt.Sprintf("%d,", count))
		timeStamps.WriteString(fmt.Sprintf("'%s',", lastSeen))

	}
	counts.WriteString("]")
	timeStamps.WriteString("]")

	res.TimeStamp = timeStamps.String()
	res.Count = counts.String()
	return res, nil
}

func (p *postgresStore) FindEventBaseId(evt EventBase) (int64, error) {
	var id int64
	row := p.DB.QueryRow("SELECT _id FROM event_base WHERE service_id = $1 AND event_type = $2 AND event_environment_id = $3 AND processed_data_hash = $4",
		evt.ServiceId, evt.EventType, evt.EventEnvironmentId, evt.ProcessedDataHash)
	err := row.Scan(&id)
	if err != nil {
		if err == sql.ErrNoRows {
			return p.AddBaseEvent(evt)
		} else {
			return -1, err
		}
	}
	return id, nil
}

func (p *postgresStore) AddBaseEvent(evt EventBase) (int64, error) {
	var id int64
	row := p.DB.QueryRow("INSERT INTO event_base (service_id, event_type, event_name, event_group_id, event_environment_id, processed_data, processed_data_hash) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING _id",
		evt.ServiceId, evt.EventType, evt.EventName, evt.EventGroupId, evt.EventEnvironmentId, util.EncodeToJsonRawMsg(evt.ProcessedData), evt.ProcessedDataHash)
	err := row.Scan(&id)
	if err != nil {
		return -1, err
	}
	return id, nil
}

func (p *postgresStore) FindEventDetailId(evtDetail EventDetail) (int64, error) {
	var id int64
	row := p.DB.QueryRow("SELECT _id FROM event_detail WHERE processed_detail_hash = $1",
		evtDetail.ProcessedDetailHash)
	err := row.Scan(&id)
	if err != nil {
		if err == sql.ErrNoRows {
			return p.AddEventDetails(evtDetail)
		} else {
			return -1, err
		}
	}
	return id, nil
}

func (p *postgresStore) AddEventDetails(evtDetail EventDetail) (int64, error) {
	var id int64
	row := p.DB.QueryRow("INSERT INTO event_detail (raw_detail, processed_detail, processed_detail_hash) VALUES ($1, $2, $3) RETURNING _id",
		util.EncodeToJsonRawMsg(evtDetail.RawDetail), util.EncodeToJsonRawMsg(evtDetail.ProcessedDetail), evtDetail.ProcessedDetailHash)
	err := row.Scan(&id)
	if err != nil {
		return -1, err
	}
	return id, nil
}

func (p *postgresStore) FindEventInstanceId(evt EventInstance) (int64, error) {
	var id int64
	row := p.DB.QueryRow("SELECT _id FROM event_instance WHERE generic_data_hash = $1 AND event_environment_id = $2",
		evt.GenericDataHash, evt.EventEnvironmentId)
	err := row.Scan(&id)
	if err != nil {
		if err == sql.ErrNoRows {
			return p.AddInstanceEvent(evt)
		} else {
			return -1, err
		}
	} else {
		//TODO figure out a better way of updating an existing event instsance
		//return id, p.UpdateEventInstance(evt, id)

		return id, nil
	}
	return id, nil
}

func (p *postgresStore) UpdateEventInstance(evt EventInstance, targetId int64) error {
	var id int64
	row := p.DB.QueryRow("UPDATE event_instance SET raw_data = $1, event_message = $2 WHERE _id = $3 RETURNING _id", util.EncodeToJsonRawMsg(evt.RawData), evt.EventMessage, targetId)
	err := row.Scan(&id)
	if err != nil {
		return err
	}
	return nil
}

func (p *postgresStore) AddInstanceEvent(evt EventInstance) (int64, error) {
	var id int64
	row := p.DB.QueryRow("INSERT INTO event_instance "+
		"(event_base_id, event_detail_id, event_environment_id, raw_data, generic_data, generic_data_hash, event_message, created_at) "+
		"VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING _id",
		evt.EventBaseId, evt.EventDetailId, evt.EventEnvironmentId, util.EncodeToJsonRawMsg(evt.RawData), util.EncodeToJsonRawMsg(evt.GenericData), evt.GenericDataHash, evt.EventMessage, evt.CreatedAt)
	err := row.Scan(&id)
	if err != nil {
		return -1, err
	}
	return id, nil
}

func (p *postgresStore) UpdateEventInstancePeriod(evt EventInstancePeriod) error {
	var id int64
	row := p.DB.QueryRow("UPDATE event_instance_period SET count = count + $1 WHERE event_instance_id = $2 AND start_time = $3 AND end_time = $4 AND region_id = $5 RETURNING _id",
		evt.Count, evt.EventInstanceId, evt.StartTime, evt.EndTime, p.Region)
	err := row.Scan(&id)
	if err != nil {
		if err == sql.ErrNoRows {
			return p.AddEventInstancePeriods(evt)
		} else {
			return err
		}
	}
	return nil
}

func (p *postgresStore) AddEventInstancePeriods(evt EventInstancePeriod) error {
	var id int64
	row := p.DB.QueryRow("INSERT INTO event_instance_period (event_instance_id, start_time, end_time, updated, count, region_id) VALUES ($1, $2, $3, $4, $5, $6) RETURNING _id",
		evt.EventInstanceId, evt.StartTime, evt.EndTime, evt.Updated, evt.Count, p.Region)
	err := row.Scan(&id)
	if err != nil {
		return err
	}
	return nil
}
