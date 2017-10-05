package main

import (
	"github.com/go-pg/pg"
	"github.com/go-pg/pg/orm"
	"time"
	"log"
	"reflect"
	"fmt"
)

type DataStore interface {
	FindPeriods(int64, int64) ([]ExceptionInstancePeriod, error)
	Query(interface{})
	QueryExceptions([]Exception) error
	QueryExceptionData([]ExceptionData) error
	QueryExceptionInstances([]ExceptionInstance) error
	QueryExceptionInstancePeriods([]ExceptionInstance) error
	AddExceptions([]Exception) (orm.Result, error)
	AddExceptionInstances([]ExceptionInstance) (orm.Result, error)
	AddExceptioninstancePeriods([]ExceptionInstancePeriod) (orm.Result, error)
	AddExceptionData([]ExceptionData) (orm.Result, error)
}

type PostgresStore struct {
	db *pg.DB
	log *log.Logger
	timeInterval int
}

/* MODELS CORRESPONDING TO DATABASE TABLES */

type Exception struct {
	tableName          struct{} `sql:"exception,alias:exception"`
	Id                 int64    `sql:"_id,pk"`
	ServiceId          int
	ServiceVersion     string
	Name               string
	ProcessedStack     string
	ProcessedStackHash string
}

type ExceptionInstance struct {
	tableName       struct{} `sql:"exception_instance,alias:exception_instance"`
	Id              int64    `sql:"_id,pk"`
	ExceptionId     int64
	ExceptionDataId int64
	RawStack        string
	RawStackHash    string

	// ignored fields, used internally
	ProcessedStackHash string `sql:"-"`
	ProcessedDataHash  string `sql:"-"`
}

type ExceptionInstancePeriod struct {
	tableName           struct{} `sql:"exception_instance_period,alias:exception_instance_period"`
	Id                  int64    `sql:"_id,pk"`
	ExceptionInstanceId int64
	ExceptionDataId     int64
	StartTime           time.Time
	Updated             time.Time
	TimeInterval        int
	Count               int

	// ignored fields, used internally
	RawStackHash      string `sql:"-"`
	ProcessedDataHash string `sql:"-"`
}

type ExceptionData struct {
	tableName         struct{} `sql:"exception_data,alias:exception_data"`
	Id                int64    `sql:"_id,pk"`
	RawData           string
	ProcessedData     string
	ProcessedDataHash string
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
	t := reflect.TypeOf(e).Name()
	v := reflect.ValueOf(e)
	var m *orm.Query
	switch  t {
	case "Exception":
		m = p.db.Model(v.Interface().(Exception))
	case "ExceptionInstance":

	case "ExceptionInstancePeriod":

	case "ExceptionData":

	default:

	}
	fmt.Println(t,v)
}

func (p *PostgresStore) FindPeriods(excId, dataId int64) ([]ExceptionInstancePeriod, error) {
	var res []ExceptionInstancePeriod
	m := p.db.Model(&res)
	if excId != 0 {
		m = m.Where("exception_instance_id = ?", excId)
	}
	if dataId != 0 {
		m = m.Where("exception_data_id = ?", dataId)
	}
	err := m.Select()
	return res, err
}

func (p *PostgresStore) QueryExceptions(excs []Exception) error {
	err := p.db.Model(&excs).Select()
	return err
}

func (p *PostgresStore) QueryExceptionData(excs []ExceptionData) error {
	err := p.db.Model(&excs).Select()
	return err
}

func (p *PostgresStore) QueryExceptionInstances(excs []ExceptionInstance) error {
	err := p.db.Model(&excs).Select()
	return err
}

func (p *PostgresStore) QueryExceptionInstancePeriods(excs []ExceptionInstance) error {
	err := p.db.Model(&excs).Select()
	return err
}

// Adds new exceptions as long as the stack hash is unique
func (p *PostgresStore) AddExceptions(excs []Exception) (orm.Result, error) {
	res, err := p.db.Model(&excs).
		OnConflict("(processed_stack_hash) DO NOTHING").
		Returning("_id").
		Insert()
	if err != nil {
		p.log.Print("Cannot insert rows into table")
	}
	return res, err
}

func (p *PostgresStore) AddExceptionInstances(excs []ExceptionInstance) (orm.Result, error) {
	res, err := p.db.Model(&excs).
		OnConflict("(raw_stack_hash) DO NOTHING").
		Returning("_id").
		Insert()
	if err != nil {
		p.log.Print("Cannot insert rows into table")
	}
	return res, err
}

func (p *PostgresStore) AddExceptioninstancePeriods(excs []ExceptionInstancePeriod) (orm.Result, error) {
	res, err := p.db.Model(&excs).
		OnConflict("(exception_instance_id, exception_data_id, start_time, time_interval) DO UPDATE").
		Set("count = exception_instance_period.count + EXCLUDED.count").
		Returning("_id").
		Insert()
	if err != nil {
		p.log.Print("Cannot insert rows into table")
	}
	return res, err
}

func (p *PostgresStore) AddExceptionData(excs []ExceptionData) (orm.Result, error) {
	res, err := p.db.Model(&excs).
		OnConflict("(processed_data_hash) DO NOTHING").
		Returning("_id").
		Insert()
	if err != nil {
		p.log.Print("Cannot insert rows into table")
	}
	return res, err
}
