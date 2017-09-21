package main

import (
	"time"
	"fmt"
)

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
	RawStackHash string
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
	EventId string `json:"event_id"`
	Message string `json:"message"`
	Level int `json:"level"`
	StackTrace StackTrace `json:"stacktrace"`
	Extra map[string]interface{} `json:"extra"`
	Modules map[string]interface{} `json:"modules"`
	Platform string `json:"platform"`
	Sdk map[string]interface{} `json:"sdk"`
	ServerName string `json:"server_name"`
	Timestamp float32 `json:"timestamp"`
}

type StackTrace struct {
	Module string `json:"module"`
	Type string `json:"type"`
	Value string `json:"value"`
	Frames []Frame `json:"frames"`
}

type Frame struct {
	AbsPath string `json:"abs_path"`
	ContextLine string `json:"context_line"`
	Filename string `json:"filename"`
	Function string `json:"function"`
	LineNo int `json:"lineno"`
	Module string `json:"module"`
	PostContext []string `json:"post_context"`
	PreContext []string `json:"pre_context"`
	Vars map[string]interface{} `json:"vars"`
}

// Wrapper struct for Exception Channel
type ExceptionChannel struct {
	_queue chan UnaddedException
	BatchSize int
	TimeLimit time.Duration
	TimeStart time.Time
}

func (c *ExceptionChannel) Send(exc UnaddedException) {
	c._queue <- exc
}

// Checks if either the channel has reached the max batch size or passed the time duration
func (c *ExceptionChannel) HasReachedLimit(t time.Time) bool {
	fmt.Println(len(c._queue), c.TimeLimit.Seconds())
	if c.TimeStart.Add(c.TimeLimit).Before(t) || len(c._queue) == c.BatchSize {
		c.TimeStart = t
		return true
	} else {
		c.TimeStart = t
		return false
	}
}

// Process Batch from channel and bulk insert into Db
func (channel *ExceptionChannel) ProcessBatchException() {
	var excsToAdd []UnaddedException
	for length:=len(channel._queue); length>0; length-- {
		exc := <- channel._queue
		fmt.Println(exc)
		excsToAdd = append(excsToAdd, exc)
	}

	// Match exceptions with each other to find similar ones

}