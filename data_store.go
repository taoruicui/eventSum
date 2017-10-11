package main

import (
	"github.com/go-pg/pg"
	"github.com/go-pg/pg/orm"
	"github.com/pkg/errors"
	"log"
	"time"
)

type DataStore interface {
	FindPeriods(int64, int64) ([]EventInstancePeriod, error)
	Query(interface{})
	QueryEvents([]EventBase) error
	QueryEventDetails(...EventDetail) (EventDetail, error)
	QueryEventInstances(...EventInstance) (EventInstance, error)
	QueryEventInstancePeriods([]EventInstance) error
	AddEvents([]EventBase) (orm.Result, error)
	AddEventInstances([]EventInstance) (orm.Result, error)
	AddEventinstancePeriods([]EventInstancePeriod) (orm.Result, error)
	AddEventDetails([]EventDetail) (orm.Result, error)
}

type PostgresStore struct {
	db           *pg.DB
	log          *log.Logger
	timeInterval int
}

/* MODELS CORRESPONDING TO DATABASE TABLES */

type EventBase struct {
	tableName         struct{} `sql:"event_base,alias:event_base"`
	Id                int64    `sql:"_id,pk"`
	ServiceId         int
	EventType         string
	EventName         string
	ProcessedData     string
	ProcessedDataHash string
}

type EventInstance struct {
	tableName     struct{} `sql:"event_instance,alias:event_instance"`
	Id            int64    `sql:"_id,pk"`
	EventBaseId   int64
	EventDetailId int64
	RawData       string
	RawDataHash   string

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
	tableName           struct{} `sql:"event_detail,alias:event_detail"`
	Id                  int64    `sql:"_id,pk"`
	RawDetail           string
	ProcessedDetail     string
	ProcessedDetailHash string
}

// Create a new DataStore
func newDataStore(conf EMConfig, log *log.Logger) DataStore {
	// Create a connection to Postgres Database
	db := pg.Connect(&pg.Options{
		Addr:     conf.PgAddress,
		User:     conf.PgUsername,
		Password: conf.PgPassword,
		Database: conf.PgDatabase,
	})
	dataStore := PostgresStore{db, log, conf.TimeInterval}
	return &dataStore
}

func (p *PostgresStore) Query(e interface{}) {
	// Figure out what model it is
	//t := reflect.TypeOf(e).Name()
	//v := reflect.ValueOf(e)
	//var m *orm.Query
	//switch  t {
	//case "Event":
	//	m = p.db.Model(v.Interface().(Event))
	//case "EventInstance":
	//
	//case "EventInstancePeriod":
	//
	//case "EventData":
	//
	//default:
	//
	//}
	//fmt.Println(t,v)
}

func (p *PostgresStore) FindPeriods(excId, dataId int64) ([]EventInstancePeriod, error) {
	var res []EventInstancePeriod
	m := p.db.Model(&res)
	if excId != 0 {
		m = m.Where("event_instance_id = ?", excId)
	}
	if dataId != 0 {
		m = m.Where("event_data_id = ?", dataId)
	}
	err := m.Select()
	return res, err
}

func (p *PostgresStore) QueryEvents(excs []EventBase) error {
	err := p.db.Model(&excs).Select()
	return err
}

func (p *PostgresStore) QueryEventDetails(excs ...EventDetail) (EventDetail, error) {
	if len(excs) == 0 {
		return EventDetail{}, errors.New("Array length is 0!")
	}
	err := p.db.Model(&excs).Select()
	return excs[0], err
}

func (p *PostgresStore) QueryEventInstances(excs ...EventInstance) (EventInstance, error) {
	if len(excs) == 0 {
		return EventInstance{}, errors.New("Array length is 0!")
	}
	err := p.db.Model(&excs).Select()
	return excs[0], err
}

func (p *PostgresStore) QueryEventInstancePeriods(excs []EventInstance) error {
	err := p.db.Model(&excs).Select()
	return err
}

// Adds new events as long as the stack hash is unique
func (p *PostgresStore) AddEvents(excs []EventBase) (orm.Result, error) {
	res, err := p.db.Model(&excs).
		OnConflict("(service_id, event_type, processed_data_hash) DO NOTHING").
		Returning("_id").
		Insert()
	if err != nil {
		p.log.Print(err)
	}
	return res, err
}

func (p *PostgresStore) AddEventInstances(excs []EventInstance) (orm.Result, error) {
	res, err := p.db.Model(&excs).
		OnConflict("(raw_data_hash) DO NOTHING").
		Returning("_id").
		Insert()
	if err != nil {
		p.log.Print(err)
	}
	return res, err
}

func (p *PostgresStore) AddEventinstancePeriods(excs []EventInstancePeriod) (orm.Result, error) {
	res, err := p.db.Model(&excs).
		OnConflict("(event_instance_id, start_time, time_interval) DO UPDATE").
		Set("count = event_instance_period.count + EXCLUDED.count").
		Returning("_id").
		Insert()
	if err != nil {
		p.log.Print(err)
	}
	return res, err
}

func (p *PostgresStore) AddEventDetails(excs []EventDetail) (orm.Result, error) {
	res, err := p.db.Model(&excs).
		OnConflict("(processed_detail_hash) DO NOTHING").
		Returning("_id").
		Insert()
	if err != nil {
		p.log.Print(err)
	}
	return res, err
}
