package main

import (
	"time"
	"fmt"
)

/* EXCEPTION STORE MODELS */

type KeyExceptionPeriod struct {
	RawStackHash, ProcessedStackHash string
}

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

type ExceptionStore struct {
	ds DataStore // link to any datastore (Postgres, Cassandra, etc.)
	channel *ExceptionChannel // channel, or queue, for the processing of new exceptions
}

// create new Exception Store. This 'store' stores necessary information
// about the exceptions and how they are processed, the exception channel,
// as well as contains the link to the datastore, or the DB.
func newExceptionStore(ds DataStore, config EMConfig) *ExceptionStore {
	return &ExceptionStore{
		ds,
		&ExceptionChannel{
			make(chan UnaddedException, config.BatchSize),
			config.BatchSize,
			config.TimeLimit,
			time.Now(),
		},
	}
}

func (s *StackTrace) GenerateFullStack() string {
	return ""
}

func (es *ExceptionStore) Send(exc UnaddedException) {
	es.channel._queue <- exc
}

// Checks if either the channel has reached the max batch size or passed the time duration
func (es *ExceptionStore) HasReachedLimit(t time.Time) bool {
	fmt.Println(len(es.channel._queue), es.channel.TimeLimit.Seconds())
	if es.channel.TimeStart.Add(es.channel.TimeLimit).Before(t) || len(es.channel._queue) == es.channel.BatchSize {
		es.channel.TimeStart = t
		return true
	} else {
		es.channel.TimeStart = t
		return false
	}
}

// Process Batch from channel and bulk insert into Db
func (es *ExceptionStore) ProcessBatchException() {
	var excsToAdd []UnaddedException
	var excs []Exception
	for length:=len(es.channel._queue); length>0; length-- {
		exc := <- es.channel._queue
		fmt.Println(exc)
		excsToAdd = append(excsToAdd, exc)
		excs = append(excs, Exception{
			ServiceId: length,
			ProcessedStack: exc.Message,
		})
	}
	fmt.Println(excs)
	//res, err := db.insert(&excs)
	//fmt.Println(res, err)


}