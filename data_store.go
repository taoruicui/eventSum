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
