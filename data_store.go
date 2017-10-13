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
	ProcessedDataHash   string `sql:"-"`
	ProcessedDetailHash string `sql:"-"`
}

type EventInstancePeriod struct {
	tableName       struct{} `sql:"event_instance_period,alias:event_instance_period"`
	Id              int64    `sql:"_id,pk"`
	EventInstanceId int64
	StartTime       time.Time
	Updated         time.Time
	TimeInterval    int
	Count           int
	CounterJson     string

	// ignored fields, used internally
	RawDataHash         string `sql:"-"`
	ProcessedDetailHash string `sql:"-"`
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

func (d *DataStore) Query(e interface{}) {

}

func (d *DataStore) FindPeriods(excId, dataId int64) ([]EventInstancePeriod, error) {
	return []EventInstancePeriod{}, nil
}

func (d *DataStore) QueryEvents(excs []EventBase) error {
	//q := &query.Query{
	//	query.Filter,
	//	map[string]interface{}{
	//		"db":             "event_sum",
	//		"collection":     "event_base",
	//		"shard_instance": "public",
	//		"filter":         map[string]interface{}{},
	//	},
	//}
	//res, err := d.client.DoQuery(context.Background(), q)
	//return res, err
	return nil
}

func (d *DataStore) QueryEventDetails(excs ...EventDetail) (EventDetail, error) {
	//if len(excs) == 0 {
	//	return EventDetail{}, errors.New("Array length is 0!")
	//}
	//err := p.db.Model(&excs).Select()
	//return excs[0], err
	return EventDetail{}, nil
}

func (d *DataStore) QueryEventInstances(excs ...EventInstance) (EventInstance, error) {
	//if len(excs) == 0 {
	//	return EventInstance{}, errors.New("Array length is 0!")
	//}
	//err := p.db.Model(&excs).Select()
	//return excs[0], err
	return EventInstance{}, nil
}

func (d *DataStore) QueryEventInstancePeriods(excs []EventInstance) error {
	//err := p.db.Model(&excs).Select()
	return nil
}

func (d *DataStore) AddEvent(evt *EventBase) error {
	q := &query.Query{
		Type: query.Set,
		Args: map[string]interface{}{
			"db":             "event_sum",
			"collection":     "event_base",
			"shard_instance": "public",
			"record": map[string]interface{}{
				"service_id":          evt.ServiceId,
				"event_type":          evt.EventType,
				"event_name":          evt.EventName,
				"processed_data":      evt.ProcessedData,
				"processed_data_hash": evt.ProcessedDataHash,
			},
		},
	}
	res, err := d.client.DoQuery(context.Background(), q)
	if err != nil {
		d.log.Panic(err)
	} else if res.Error != "" {
		return errors.New(res.Error)
	}
	mapstructure.Decode(res.Return[0], &evt)
	return err
}

// Adds new events as long as the stack hash is unique
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
	q := &query.Query{
		Type: query.Set,
		Args: map[string]interface{}{
			"db":             "event_sum",
			"collection":     "event_instance",
			"shard_instance": "public",
			"record": map[string]interface{}{
				"event_base_id":   evt.EventBaseId,
				"event_detail_id": evt.EventDetailId,
				"raw_data":        evt.RawData,
				"raw_data_hash":   evt.RawDataHash,
			},
		},
	}
	res, err := d.client.DoQuery(context.Background(), q)
	if err != nil {
		d.log.Panic(err)
	} else if res.Error != "" {
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
	q := &query.Query{
		Type: query.Set,
		Args: map[string]interface{}{
			"db":             "event_sum",
			"collection":     "event_instance_period",
			"shard_instance": "public",
			"record": map[string]interface{}{
				"event_instance_id": evt.EventInstanceId,
				"start_time":        evt.StartTime,
				"updated":           evt.Updated,
				"time_interval":     evt.TimeInterval,
				"count":             evt.Count,
				"counter_json":      evt.CounterJson,
			},
		},
	}
	res, err := d.client.DoQuery(context.Background(), q)
	if err != nil {
		d.log.Panic(err)
	} else if res.Error != "" {
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
	q := &query.Query{
		Type: query.Set,
		Args: map[string]interface{}{
			"db":             "event_sum",
			"collection":     "event_detail",
			"shard_instance": "public",
			"record": map[string]interface{}{
				"raw_detail":            evt.RawDetail,
				"processed_detail":      evt.ProcessedDetail,
				"processed_detail_hash": evt.ProcessedDetailHash,
			},
		},
	}
	res, err := d.client.DoQuery(context.Background(), q)
	if err != nil {
		d.log.Panic(err)
	} else if res.Error != "" {
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
