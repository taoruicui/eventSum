package main

import "time"

/* MODELS CORRESPONDING TO DATABASE TABLES */

type Exception struct {
	tableName struct{} `sql:"exception,alias:exception"`

	_Id                int8
	ServiceId          int8
	ServiceVersion     string
	Name               string
	ProcessedStack     string
	ProcessedStackHash string
}

type ExceptionInstance struct {
	tableName struct{} `sql:"exception_instance,alias:exception_instance"`

	_Id              int8
	ExceptionClassId int8
	ExceptionDataId  string
	RawStack         string
}

type ExceptionInstancePeriod struct {
	tableName struct{} `sql:"exception_instance_period,alias:exception_period"`

	_Id                      int8
	ExceptionClassInstanceId int8
	CreatedAt                time.Time
	UpdatedAt                time.Time
	ExceptionDataId          int8
	Count                    int8
}

type ExceptionData struct {
	tableName struct{} `sql:"exception_data,alias:exception_data"`

	_Id           int8
	RawData       string
	ProcessedData string
	Hash          string
}

/* OTHER MODELS */

type UnaddedException struct {
	Level string
	Modules string

}

/* FUNCTIONS */

// Process Batch from channel and bulk insert into Db
func ProcessBatchException(c chan UnaddedException, size int) {

}