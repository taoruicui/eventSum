package main

import (
	"time"
	"github.com/go-pg/pg"
	"github.com/go-pg/pg/orm"
)

type DataStore interface {
	AddExceptions([]Exception) (orm.Result, error)
	AddExceptionInstances([]ExceptionInstance) (orm.Result, error)
	AddExceptioninstancePeriods([]ExceptionInstancePeriod) (orm.Result, error)
	AddExceptionData([]ExceptionData) (orm.Result, error)
}

type PostgresStore struct {
	db *pg.DB
}

/* MODELS CORRESPONDING TO DATABASE TABLES */

type Exception struct {
	tableName struct{} `sql:"exception,alias:exception"`
	Id                 int64 `sql:"_id,pk"`
	ServiceId          int
	ServiceVersion     string
	Name               string
	ProcessedStack     string
	ProcessedStackHash string
}

type ExceptionInstance struct {
	tableName struct{} `sql:"exception_instance,alias:exception_instance"`
	Id               int64 `sql:"_id,pk"`
	ExceptionId int64
	ExceptionDataId  int64
	RawStack         string
	RawStackHash string

	// ignored fields, used internally
	ProcessedStackHash string `sql:"-"`
	ProcessedDataHash string `sql:"-"`
}

type ExceptionInstancePeriod struct {
	tableName struct{} `sql:"exception_instance_period,alias:exception_instance_period"`
	Id                       int64 `sql:"_id,pk"`
	ExceptionInstanceId int64
	ExceptionDataId          int64
	CreatedAt                time.Time
	UpdatedAt                time.Time
	Count                    int

	// ignored fields, used internally
	RawStackHash string `sql:"-"`
	ProcessedDataHash string `sql:"-"`
}

type ExceptionData struct {
	tableName struct{} `sql:"exception_data,alias:exception_data"`
	Id            int64 `sql:"_id,pk"`
	RawData       string
	ProcessedData string
	ProcessedDataHash          string
}

// Create a new DataStore
func newDataStore(conf EMConfig) DataStore {
	// Create a connection to Postgres Database
	db := pg.Connect(&pg.Options{
		Addr:     conf.PgAddress,
		User:     conf.PgUsername,
		Password: conf.PgPassword,
		Database: conf.PgDatabase,
	})
	dataStore := PostgresStore{db}
	return &dataStore
}

// Adds new exceptions as long as the stack hash is unique, returning all ids that have
// been created or have already existed before
func (p *PostgresStore) AddExceptions(excs []Exception) (orm.Result, error) {
	res, err := p.db.Model(&excs).
		OnConflict("(processed_stack_hash) DO UPDATE").
		Set("processed_stack_hash = EXCLUDED.processed_stack_hash").
		Returning("_id").
		Insert()
	return res, err
}

func (p *PostgresStore) AddExceptionInstances(excs []ExceptionInstance) (orm.Result, error) {
	res, err := p.db.Model(&excs).
		OnConflict("(raw_stack_hash) DO UPDATE").
		Set("raw_stack_hash = EXCLUDED.raw_stack_hash").
		Returning("_id").
		Insert()
	return res, err
}

func (p *PostgresStore) AddExceptioninstancePeriods(excs []ExceptionInstancePeriod) (orm.Result, error) {
	res, err := p.db.Model(&excs).
		OnConflict("(exception_instance_id, exception_data_id) DO UPDATE").
		Set("count = exception_instance_period.count + EXCLUDED.count").
		//Where("exception_instance_period.exception_instance_id = EXCLUDED.exception_instance_id and exception_instance_period.exception_data_id = EXCLUDED.exception_data_id").
		Returning("_id").
		Insert()
	return res, err
}

func (p *PostgresStore) AddExceptionData(excs []ExceptionData) (orm.Result, error) {
	res, err := p.db.Model(&excs).
		OnConflict("(processed_data_hash) DO UPDATE").
		Set("processed_data_hash = EXCLUDED.processed_data_hash").
		Returning("_id").
		Insert()
	return res, err
}