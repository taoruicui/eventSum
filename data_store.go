package eventsum

import (
	"log"
	"time"

	"context"
	"encoding/json"
	"github.com/jacksontj/dataman/src/client"
	"github.com/jacksontj/dataman/src/client/direct"
	"github.com/jacksontj/dataman/src/query"
	"github.com/jacksontj/dataman/src/storage_node"
	"github.com/jacksontj/dataman/src/storage_node/metadata"
	"github.com/pkg/errors"
	"io/ioutil"
)

type dataStore struct {
	client       *datamanclient.Client
	log          *log.Logger
	timeInterval int
}

/* MODELS CORRESPONDING TO DATABASE TABLES */

type eventBase struct {
	Id                int64  `mapstructure:"_id"`
	ServiceId         int    `mapstructure:"service_id"`
	EventType         string `mapstructure:"event_type"`
	EventName         string `mapstructure:"event_name"`
	ProcessedData     interface{} `mapstructure:"processed_data"`
	ProcessedDataHash string `mapstructure:"processed_data_hash"`
}

type eventInstance struct {
	Id            int64  `mapstructure:"_id"`
	EventBaseId   int64  `mapstructure:"event_base_id"`
	EventDetailId int64  `mapstructure:"event_detail_id"`
	RawData       interface{} `mapstructure:"raw_data"`
	RawDataHash   string `mapstructure:"raw_data_hash"`

	// ignored fields, used internally
	ProcessedDataHash   string
	ProcessedDetailHash string
}

type eventInstancePeriod struct {
	Id              int64     `mapstructure:"_id"`
	EventInstanceId int64     `mapstructure:"event_instance_id"`
	StartTime       time.Time `mapstructure:"start_time"`
	EndTime         time.Time `mapstructure:"end_time"`
	Updated         time.Time `mapstructure:"updated"`
	Count           int       `mapstructure:"count"`
	CounterJson     map[string]interface{}    `mapstructure:"counter_json"`

	// ignored fields, used internally
	RawDataHash         string
	ProcessedDetailHash string
}

type eventDetail struct {
	Id                  int64  `mapstructure:"_id"`
	RawDetail           interface{} `mapstructure:"raw_detail"`
	ProcessedDetail     interface{} `mapstructure:"processed_detail"`
	ProcessedDetailHash string `mapstructure:"processed_detail_hash"`
}

// Create a new dataStore
func newDataStore(conf eventsumConfig, log *log.Logger) *dataStore {
	// Create a connection to Postgres Database through Dataman

	storagenodeConfig, err := storagenode.DatasourceInstanceConfigFromFile(conf.DataSourceInstance)
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}

	// Load meta
	meta := &metadata.Meta{}
	metaBytes, err := ioutil.ReadFile(conf.DataSourceSchema)
	if err != nil {
		log.Fatalf("Error loading schema: %v", err)
	}
	err = json.Unmarshal([]byte(metaBytes), meta)
	if err != nil {
		log.Fatalf("Error loading meta: %v", err)
	}

	// TODO: remove
	storagenodeConfig.SkipProvisionTrim = true

	transport, err := datamandirect.NewStaticDatasourceInstanceTransport(storagenodeConfig, meta)
	if err != nil {
		log.Fatalf("Error NewStaticDatasourceInstanceClient: %v", err)
	}

	client := &datamanclient.Client{Transport: transport}
	return &dataStore{
		client:       client,
		log:          log,
		timeInterval: conf.TimeInterval,
	}
}

func (d *dataStore) Query(  typ query.QueryType,
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
			"db":             "event_sum",
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
	res, err := d.client.DoQuery(context.Background(), q)
	if err != nil {
		d.log.Panic(err)
	} else if res.Error != "" {
		return res, errors.New(res.Error)
	}
	return res, err
}

func (d *dataStore) GetRecentEvents(start, end time.Time, serviceId int, limit int) ([]eventRecentResult, error) {
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
	res, err := d.Query(query.Filter, "event_instance_period", filter,nil, nil,nil, -1, []string{"start_time"}, join)
	d.log.Print(res, err)
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
				Id: evtBase.Id,
				EventType: evtBase.EventType,
				EventName: evtBase.EventName,
				ProcessedData: evtBase.ProcessedData,
				Count: 0,
				LastUpdated: evtPeriod.Updated,
				InstanceIds: []int64{},
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

func (d *dataStore) GetEventPeriods(start, end time.Time, eventId int) ([]eventInstancePeriod, error) {
	// TODO: Change to use filter function
	var hist []eventInstancePeriod
	var bin eventInstancePeriod
	filter := []interface{}{
		[]interface{}{
			map[string]interface{}{"start_time": []interface{}{">", start}}, "AND",
			map[string]interface{}{"end_time": []interface{}{"<", end}},
		}, "AND",
		map[string]interface{}{"event_instance_id": []interface{}{"=", eventId}}}

	res, err := d.Query(query.Filter, "event_instance_period", filter,nil, nil,nil, -1, []string{"start_time"}, nil)
	d.log.Print(res)
	if err != nil {
		d.log.Print(res.Error)
		return hist, err
	}
	for _, v := range res.Return {
		mapDecode(v, &bin)
		hist = append(hist, bin)
	}
	return hist, nil
}

func (d *dataStore) GetEventDetailsbyId(id int) (eventDetailsResult, error) {
	var result eventDetailsResult
	var instance eventInstance
	var detail eventDetail
	var base eventBase
	join := []interface{}{"event_base_id", "event_detail_id"}
	pkey := map[string]interface{}{"_id": id}
	r, err := d.Query(query.Get, "event_instance", nil, nil, nil, pkey, -1, nil, join)
	if r.Error != "" {
		return result, errors.New(r.Error)
	} else if len(r.Return) == 0 {
		return result, err
	}
	mapDecode(r.Return[0], &instance)
	if t1, ok := r.Return[0]["event_base_id"].(map[string]interface{}); ok {
		mapDecode(t1, &base)
		if t2, ok := t1["event_detail_id"].(map[string]interface{}); ok{
			mapDecode(t2, &detail)
		}
	}
	result = eventDetailsResult{
		EventType:   base.EventType,
		EventName:     base.EventName,
		RawData:  instance.RawData,
		RawDetails:        detail.RawDetail,
	}
	return result, nil
}

func (d *dataStore) AddEvent(evt *eventBase) error {
	filter := map[string]interface{}{
		"service_id": []interface{}{"=", evt.ServiceId},
		"event_type": []interface{}{"=", evt.EventType},
		"processed_data_hash": []interface{}{"=", evt.ProcessedDataHash},
	}
	res, err := d.Query(query.Filter, "event_base", filter, nil, nil,nil,1, nil, nil)
	if err != nil {
		return err
	} else if len(res.Return) == 0 {
		//TODO: fix uniqueness constraint
		record := map[string]interface{}{
			"service_id":          evt.ServiceId,
			"event_type":          evt.EventType,
			"event_name":          evt.EventName,
			"processed_data":      evt.ProcessedData,
			"processed_data_hash": evt.ProcessedDataHash,
		}

		res, err = d.Query(query.Set, "event_base", nil, record, nil,nil,-1, nil, nil)
		if err != nil {
			return err
		}
	}
	mapDecode(res.Return[0], &evt)
	return nil
}

func (d *dataStore) AddEvents(evts []eventBase) error {
	for i := range evts {
		err := d.AddEvent(&evts[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *dataStore) AddEventInstance(evt *eventInstance) error {
	filter := map[string]interface{}{
		"raw_data_hash": []interface{}{"=", evt.RawDataHash},
	}
	res, err := d.Query(query.Filter, "event_instance", filter, nil, nil,nil,1, nil, nil)
	if err != nil {
		return err
	} else if len(res.Return) == 0 {
		//TODO: fix uniqueness constraint
		record := map[string]interface{}{
			"event_base_id":   evt.EventBaseId,
			"event_detail_id": evt.EventDetailId,
			"raw_data":        evt.RawData,
			"raw_data_hash":   evt.RawDataHash,
		}
		res, err = d.Query(query.Set, "event_instance", nil, record, nil,nil,-1, nil, nil)
		if err != nil {
			return err
		}
	}
	mapDecode(res.Return[0], &evt)
	return nil
}

func (d *dataStore) AddEventInstances(evts []eventInstance) error {
	for i := range evts {
		err := d.AddEventInstance(&evts[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *dataStore) AddEventInstancePeriod(evt *eventInstancePeriod) error {
	filter := map[string]interface{}{
		"event_instance_id": []interface{}{"=", evt.EventInstanceId},
		"start_time": []interface{}{"=", evt.StartTime},
		"end_time": []interface{}{"=", evt.EndTime},
	}
	record := map[string]interface{}{
		"event_instance_id": evt.EventInstanceId,
		"start_time":        evt.StartTime,
		"updated":           evt.Updated,
		"end_time":          evt.EndTime,
		"count":             evt.Count,
		"counter_json":      evt.CounterJson,
	}
	recordOp := map[string]interface{} {
		"count": []interface{}{"+", evt.Count},
	}
	// Add all the counter_json keys
	for k, v := range evt.CounterJson {
		recordOp["counter_json."+k] = []interface{}{"+", v}
	}
	res, err := d.Query(query.Filter, "event_instance_period", filter,nil, nil,nil,-1, nil, nil)
	if err != nil {
		return err
	} else if len(res.Return) == 0 {
		//TODO: fix uniqueness constraint
		res, err = d.Query(query.Set, "event_instance_period", nil, record, nil,nil,-1, nil, nil)
		if err != nil {
			return err
		}
	} else {
		var tmp eventInstancePeriod
		mapDecode(res.Return[0], &tmp)
		record["count"] = tmp.Count + evt.Count
		record["counter_json"], _ = globalRule.Consolidate(tmp.CounterJson, evt.CounterJson)
		res, err = d.Query(query.Update, "event_instance_period", filter, record,nil,nil,-1,nil,nil)
	}
	mapDecode(res.Return[0], &evt)
	return err
}

func (d *dataStore) AddEventinstancePeriods(evts []eventInstancePeriod) error {
	for i := range evts {
		err := d.AddEventInstancePeriod(&evts[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *dataStore) AddEventDetail(evt *eventDetail) error {
	filter := map[string]interface{}{
		"processed_detail_hash": []interface{}{"=", evt.ProcessedDetailHash},
	}
	res, err := d.Query(query.Filter, "event_detail", filter, nil, nil,nil,1, nil, nil)
	if err != nil {
		return err
	} else if len(res.Return) == 0 {
		//TODO: fix uniqueness constraint
		record := map[string]interface{}{
			"raw_detail":            evt.RawDetail,
			"processed_detail":      evt.ProcessedDetail,
			"processed_detail_hash": evt.ProcessedDetailHash,
		}
		res, err = d.Query(query.Set, "event_detail", nil, record, nil,nil,-1, nil, nil)
		if err != nil {
			return err
		}
	}
	mapDecode(res.Return[0], &evt)
	return err
}

func (d *dataStore) AddEventDetails(evts []eventDetail) error {
	for i := range evts {
		err := d.AddEventDetail(&evts[i])
		if err != nil {
			return err
		}
	}
	return nil
}
