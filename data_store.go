package main

import (
	"time"
	"github.com/go-pg/pg"
	"errors"
)

type DataStore interface {
	AddExceptions([]Exception) error
	AddExceptionInstances([]ExceptionInstance) error
	AddExceptioninstancePeriods([]ExceptionInstancePeriod) error
	AddExceptionData([]ExceptionData) error
}

type PostgresStore struct {
	db *pg.DB
}

/* MODELS CORRESPONDING TO DATABASE TABLES */

type Exception struct {
	tableName struct{} `sql:"exception,alias:exception"`
	_Id                int
	ServiceId          int
	ServiceVersion     string
	Name               string
	ProcessedStack     string
	ProcessedStackHash string
}

type ExceptionInstance struct {
	tableName struct{} `sql:"exception_instance,alias:exception_instance"`
	_Id              int
	ExceptionClassId string
	ExceptionDataId  string
	RawStack         string
	RawStackHash string
}

type ExceptionInstancePeriod struct {
	tableName struct{} `sql:"exception_instance_period,alias:exception_period"`
	_Id                      int
	ExceptionClassInstanceId int
	CreatedAt                time.Time
	UpdatedAt                time.Time
	ExceptionDataId          int
	Count                    int
}

type ExceptionData struct {
	tableName struct{} `sql:"exception_data,alias:exception_data"`
	_Id           int
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

func (p *PostgresStore) AddExceptions([]Exception) error {
	return errors.New("")
}

func (p *PostgresStore) AddExceptionInstances([]ExceptionInstance) error {
	return errors.New("")
}

func (p *PostgresStore) AddExceptioninstancePeriods([]ExceptionInstancePeriod) error {
	return errors.New("")
}

func (p *PostgresStore) AddExceptionData([]ExceptionData) error {
	return errors.New("")
}