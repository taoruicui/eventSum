package main

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
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"io/ioutil"
	"fmt"
)

type DataStore struct {
	client       *datamanclient.Client
	log          *log.Logger
	timeInterval int
}

/* MODELS CORRESPONDING TO DATABASE TABLES */

type EventBase struct {
	Id                int64  `mapstructure:"_id"`
	ServiceId         int    `mapstructure:"service_id"`
	EventType         string `mapstructure:"event_type"`
	EventName         string `mapstructure:"event_name"`
	ProcessedData     interface{} `mapstructure:"processed_data"`
	ProcessedDataHash string `mapstructure:"processed_data_hash"`
}

type EventInstance struct {
	Id            int64  `mapstructure:"_id"`
	EventBaseId   int64  `mapstructure:"event_base_id"`
	EventDetailId int64  `mapstructure:"event_detail_id"`
	RawData       interface{} `mapstructure:"raw_data"`
	RawDataHash   string `mapstructure:"raw_data_hash"`

	// ignored fields, used internally
	ProcessedDataHash   string
	ProcessedDetailHash string
}

type EventInstancePeriod struct {
	Id              int64     `mapstructure:"_id"`
	EventInstanceId int64     `mapstructure:"event_instance_id"`
	StartTime       time.Time `mapstructure:"start_time"`
	EndTime         time.Time `mapstructure:"end_time"`
	Updated         time.Time `mapstructure:"updated"`
	Count           int       `mapstructure:"count"`
	CounterJson     map[string]int    `mapstructure:"counter_json"`

	// ignored fields, used internally
	RawDataHash         string
	ProcessedDetailHash string
}

type EventDetail struct {
	Id                  int64  `mapstructure:"_id"`
	RawDetail           interface{} `mapstructure:"raw_detail"`
	ProcessedDetail     interface{} `mapstructure:"processed_detail"`
	ProcessedDetailHash string `mapstructure:"processed_detail_hash"`
}

// Create a new DataStore
func newDataStore(conf EMConfig, log *log.Logger) *DataStore {
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
	return &DataStore{
		client:       client,
		log:          log,
		timeInterval: conf.TimeInterval,
	}
}

func (d *DataStore) GetById(table string, id int) (*query.Result, error) {
	q := &query.Query{
		Type: query.Filter,
		Args: map[string]interface{}{
			"db":             "event_sum",
			"collection":     table,
			"shard_instance": "public",
			"filter":         map[string]interface{}{"_id": []interface{}{"=",id}},
		},
	}
	res, err := d.client.DoQuery(context.Background(), q)
	if err != nil {
		d.log.Panic(err)
	}
	return res, err
}

func (d *DataStore) GetEventPeriods(start, end time.Time, eventId int) ([]EventInstancePeriod, error) {
	//filter := fmt.Sprintf(`
	//	[ {“event_instance_id”: [“=”, %v]}, “AND”, [{“start_time”: [“<”, "%v"]}, “AND”, {“start_time”: [“>”, "%v"]}]`,
	//	eventId, end, start)
	var hist []EventInstancePeriod
	var bin EventInstancePeriod
	q := &query.Query{
		Type: query.Filter,
		Args: map[string]interface{}{
			"db":             "event_sum",
			"collection":     "event_instance_period",
			"shard_instance": "public",
			"sort": []string{"start_time"},
			"filter":         []interface{}{
				[]interface{}{
					map[string]interface{}{
						"start_time": []interface{}{">", start},
					},
					"AND",
					map[string]interface{}{
						"end_time": []interface{}{"<", end},
					},
				},
				"AND",
				map[string]interface{}{
					"event_instance_id": eventId,
				}},
		},
	}
	res, err := d.client.DoQuery(context.Background(), q)
	if err != nil {
		d.log.Panic(err)
	} else if res.Error != "" {
		return hist, errors.New(res.Error)
	}
	for _, v := range res.Return {
		mapstructure.Decode(v, &bin)
		hist = append(hist, bin)
	}
	return hist, nil
}

func (d *DataStore) GetEventBaseById(id int) (EventBase, error) {
	var result EventBase
	res, err := d.GetById("event_base", id)
	if res.Error != "" {
		return result, errors.New(res.Error)
	} else if len(res.Return) == 0 {
		return result, err
	}
	mapstructure.Decode(res.Return[0], &result)
	return result, err
}

func (d *DataStore) GetInstanceById(id int) (EventInstance, error) {
	var result EventInstance
	res, err := d.GetById("event_instance", id)
	if res.Error != "" {
		return result, errors.New(res.Error)
	} else if len(res.Return) == 0 {
		return result, err
	}
	mapstructure.Decode(res.Return[0], &result)
	return result, err
}

func (d *DataStore) GetDetailById(id int) (EventDetail, error) {
	var result EventDetail
	res, err := d.GetById("event_detail", id)
	if res.Error != "" {
		return result, errors.New(res.Error)
	} else if len(res.Return) == 0 {
		return result, err
	}
	mapstructure.Decode(res.Return[0], &result)
	return result, err
}

func (d *DataStore) set(table string,
						record map[string]interface{},
						recordOp map[string]interface{},
						join []string) (*query.Result, error) {
	q := &query.Query{
		Type: query.Set,
		Args: map[string]interface{}{
			"db":             "event_sum",
			"collection":     table,
			"shard_instance": "public",
			"record": record,
		},
	}
	if recordOp != nil {
		q.Args["record_op"] = recordOp
	}
	if join != nil {
		q.Args["join"] = join
	}
	res, err := d.client.DoQuery(context.Background(), q)
	if err != nil {
		d.log.Panic(err)
	}
	return res, err
}

func (d *DataStore) AddEvent(evt *EventBase) error {
	record := map[string]interface{}{
		"service_id":          evt.ServiceId,
		"event_type":          evt.EventType,
		"event_name":          evt.EventName,
		"processed_data":      evt.ProcessedData,
		"processed_data_hash": evt.ProcessedDataHash,
	}

	res, err := d.set("event_base", record, nil, nil)
	if res.Error != "" {
		return errors.New(res.Error)
	}
	mapstructure.Decode(res.Return[0], &evt)
	return err
}

func (d *DataStore) AddEvents(evts []EventBase) error {
	for i := range evts {
		err := d.AddEvent(&evts[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *DataStore) AddEventInstance(evt *EventInstance) error {
	record := map[string]interface{}{
		"event_base_id":   evt.EventBaseId,
		"event_detail_id": evt.EventDetailId,
		"raw_data":        evt.RawData,
		"raw_data_hash":   evt.RawDataHash,
	}
	res, err := d.set("event_instance", record, nil, nil)
	if res.Error != "" {
		return errors.New(res.Error)
	}
	mapstructure.Decode(res.Return[0], &evt)
	return err
}

func (d *DataStore) AddEventInstances(evts []EventInstance) error {
	for i := range evts {
		err := d.AddEventInstance(&evts[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *DataStore) AddEventInstancePeriod(evt *EventInstancePeriod) error {
	record := map[string]interface{}{
		"event_instance_id": evt.EventInstanceId,
		"start_time":        evt.StartTime,
		"updated":           evt.Updated,
		"end_time":          evt.EndTime,
		"counter_json":      evt.CounterJson,
	}
	recordOp := map[string]interface{} {
		"count": []interface{}{"+", evt.Count},
	}
	res, err := d.set("event_instance_period", record, recordOp, nil)
	if res.Error != "" {
		return errors.New(res.Error)
	}
	mapstructure.Decode(res.Return[0], &evt)
	return err
}

func (d *DataStore) AddEventinstancePeriods(evts []EventInstancePeriod) error {
	for i := range evts {
		err := d.AddEventInstancePeriod(&evts[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *DataStore) AddEventDetail(evt *EventDetail) error {
	record := map[string]interface{}{
		"raw_detail":            evt.RawDetail,
		"processed_detail":      evt.ProcessedDetail,
		"processed_detail_hash": evt.ProcessedDetailHash,
	}
	res, err := d.set("event_detail", record, nil, nil)
	if res.Error != "" {
		return errors.New(res.Error)
	}
	mapstructure.Decode(res.Return[0], &evt)
	return err
}

func (d *DataStore) AddEventDetails(evts []EventDetail) error {
	for i := range evts {
		err := d.AddEventDetail(&evts[i])
		if err != nil {
			return err
		}
	}
	return nil
}
