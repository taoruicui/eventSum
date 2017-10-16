package main

import (
	"log"
	"time"

	"context"
	"encoding/json"
	"fmt"
	"github.com/jacksontj/dataman/src/client"
	"github.com/jacksontj/dataman/src/client/direct"
	"github.com/jacksontj/dataman/src/query"
	"github.com/jacksontj/dataman/src/storage_node"
	"github.com/jacksontj/dataman/src/storage_node/metadata"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"io/ioutil"
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
	ProcessedData     string `mapstructure:"processed_data"`
	ProcessedDataHash string `mapstructure:"processed_data_hash"`
}

type EventInstance struct {
	Id            int64  `mapstructure:"_id"`
	EventBaseId   int64  `mapstructure:"event_base_id"`
	EventDetailId int64  `mapstructure:"event_detail_id"`
	RawData       string `mapstructure:"raw_data"`
	RawDataHash   string `mapstructure:"raw_data_hash"`

	// ignored fields, used internally
	ProcessedDataHash   string
	ProcessedDetailHash string
}

type EventInstancePeriod struct {
	Id              int64     `mapstructure:"_id"`
	EventInstanceId int64     `mapstructure:"event_instance_id"`
	StartTime       time.Time `mapstructure:"start_time"`
	Updated         time.Time `mapstructure:"updated"`
	TimeInterval    int       `mapstructure:"time_interval"`
	Count           int       `mapstructure:"count"`
	CounterJson     string    `mapstructure:"counter_json"`

	// ignored fields, used internally
	RawDataHash         string
	ProcessedDetailHash string
}

type EventDetail struct {
	Id                  int64  `mapstructure:"_id"`
	RawDetail           string `mapstructure:"raw_detail"`
	ProcessedDetail     string `mapstructure:"processed_detail"`
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

func (d *DataStore) getById(table string, id int) (*query.Result, error) {
	q := &query.Query{
		Type: query.Get,
		Args: map[string]interface{}{
			"db":             "event_sum",
			"collection":     table,
			"shard_instance": "public",
			"_id":            id,
		},
	}
	res, err := d.client.DoQuery(context.Background(), q)
	if err != nil {
		d.log.Panic(err)
	}
	return res, err
}

func (d *DataStore) FindPeriods(excId, dataId int64) ([]EventInstancePeriod, error) {
	return []EventInstancePeriod{}, nil
}

func (d *DataStore) QueryEvents(evts []EventBase) error {
	return nil
}

func (d *DataStore) GetInstanceById(id int) (EventInstance, error) {
	var result EventInstance
	res, err := d.getById("event_instance", id)
	if res.Error != "" {
		return result, errors.New(res.Error)
	}
	mapstructure.Decode(res.Return[0], &result)
	return result, err
}

func (d *DataStore) GetDetailById(id int) (EventDetail, error) {
	var result EventDetail
	res, err := d.getById("event_detail", id)
	if res.Error != "" {
		return result, errors.New(res.Error)
	}
	mapstructure.Decode(res.Return[0], &result)
	return result, err
}

func (d *DataStore) set(table string, record map[string]interface{}) (*query.Result, error) {
	q := &query.Query{
		Type: query.Set,
		Args: map[string]interface{}{
			"db":             "event_sum",
			"collection":     table,
			"shard_instance": "public",
			"record": record,
		},
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

	res, err := d.set("event_base", record)
	if res.Error != "" {
		return errors.New(res.Error)
	}
	mapstructure.Decode(res.Return[0], &evt)
	return err
}

func (d *DataStore) AddEvents(evts []EventBase) error {
	for i := range evts {
		err := d.AddEvent(&evts[i])
		fmt.Println(evts[i], err)
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
	res, err := d.set("event_instance", record)
	if res.Error != "" {
		return errors.New(res.Error)
	}
	mapstructure.Decode(res.Return[0], &evt)
	return err
}

func (d *DataStore) AddEventInstances(evts []EventInstance) error {
	for i := range evts {
		err := d.AddEventInstance(&evts[i])
		fmt.Println(evts[i], err)
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
		"time_interval":     evt.TimeInterval,
		"count":             evt.Count,
		"counter_json":      evt.CounterJson,
	}
	res, err := d.set("event_instance_period", record)
	if res.Error != "" {
		return errors.New(res.Error)
	}
	mapstructure.Decode(res.Return[0], &evt)
	return err
}

func (d *DataStore) AddEventinstancePeriods(evts []EventInstancePeriod) error {
	for i := range evts {
		err := d.AddEventInstancePeriod(&evts[i])
		fmt.Println(evts[i], err)
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
	res, err := d.set("event_detail", record)
	if res.Error != "" {
		return errors.New(res.Error)
	}
	mapstructure.Decode(res.Return[0], &evt)
	return err
}

func (d *DataStore) AddEventDetails(evts []EventDetail) error {
	for i := range evts {
		err := d.AddEventDetail(&evts[i])
		fmt.Println(evts[i], err)
		if err != nil {
			return err
		}
	}
	return nil
}
