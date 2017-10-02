package main

import (
	"time"
	"log"
)

/* EXCEPTION STORE MODELS */

type KeyExceptionPeriod struct {
	RawStackHash, ProcessedDataHash string
	TimePeriod                      time.Time
}

type UnaddedException struct {
	EventId    string                 `json:"event_id"`
	Message    string                 `json:"message"`
	Level      int                    `json:"level"`
	StackTrace StackTrace             `json:"stacktrace"`
	Extra      map[string]interface{} `json:"extra"`
	Modules    map[string]interface{} `json:"modules"`
	Platform   string                 `json:"platform"`
	Sdk        map[string]interface{} `json:"sdk"`
	ServerName string                 `json:"server_name"`
	Timestamp  float64                `json:"timestamp"`
}

type StackTrace struct {
	Module string  `json:"module"`
	Type   string  `json:"type"`
	Value  string  `json:"value"`
	RawStack string `json:"raw_stack"`
	Frames []Frame `json:"frames"`
}

type Frame struct {
	AbsPath     string                 `json:"abs_path"`
	ContextLine string                 `json:"context_line"`
	Filename    string                 `json:"filename"`
	Function    string                 `json:"function"`
	LineNo      int                    `json:"lineno"`
	Module      string                 `json:"module"`
	PostContext []string               `json:"post_context"`
	PreContext  []string               `json:"pre_context"`
	Vars        map[string]interface{} `json:"vars"`
}

// Wrapper struct for Exception Channel
type ExceptionChannel struct {
	_queue    chan UnaddedException
	BatchSize int
	ticker *time.Ticker
	quit chan int
}

type ExceptionStore struct {
	ds      DataStore         // link to any data store (Postgres, Cassandra, etc.)
	channel *ExceptionChannel // channel, or queue, for the processing of new exceptions
	log *log.Logger
}

// create new Exception Store. This 'store' stores necessary information
// about the exceptions and how they are processed, the exception channel,
// as well as contains the link to the data store, or the DB.
func newExceptionStore(ds DataStore, config EMConfig, log *log.Logger) *ExceptionStore {
	return &ExceptionStore{
		ds,
		&ExceptionChannel{
			make(chan UnaddedException, config.BatchSize),
			config.BatchSize,
			time.NewTicker(config.TimeLimit),
			make(chan int),
		},
		log,
	}
}

// Starts the periodic processing of channel
func (es *ExceptionStore) Start() {
	quit := make(chan int)
	for {
		select{
		case <- es.channel.ticker.C:
			es.ProcessBatchException()
		case <- quit:
			es.channel.ticker.Stop()
			return
		}
	}
}

func (es *ExceptionStore) Stop() {
	es.channel.quit <- 0
}

// Add new UnaddedException to channel, process if full
func (es *ExceptionStore) Send(exc UnaddedException) {
	es.channel._queue <- exc
	if len(es.channel._queue) == es.channel.BatchSize {
		go es.ProcessBatchException()
	}
}

// Process Batch from channel and bulk insert into Db
func (es *ExceptionStore) ProcessBatchException() {
	var excsToAdd []UnaddedException
	for length := len(es.channel._queue); length > 0; length-- {
		exc := <-es.channel._queue
		excsToAdd = append(excsToAdd, exc)
	}
	if len(excsToAdd) == 0 { return }

	// Match exceptions with each other to find similar ones

	// Rows to add to Tables
	var exceptionClasses []Exception
	var exceptionClassInstances []ExceptionInstance
	var exceptionClassInstancePeriods []ExceptionInstancePeriod
	var exceptionData []ExceptionData

	// Maps the hash to the index of the associated array
	var exceptionClassesMap = make(map[string]int)
	var exceptionClassInstancesMap = make(map[string]int)
	var exceptionClassInstancePeriodsMap = make(map[KeyExceptionPeriod]int)
	var exceptionDataMap = make(map[string]int)

	for _, exception := range excsToAdd {
		rawStack := GenerateRawStack(exception.StackTrace)
		processedStack := ProcessStack(exception.StackTrace)
		rawData := ExtractDataFromException(exception)
		processedData := ProcessData(rawData)

		rawStackHash := Hash(rawStack)
		processedStackHash := Hash(processedStack)
		processedDataHash := Hash(processedData)

		// Each hash should be unique in the database, and so we make sure
		// they are not repeated in the array by checking the associated map.
		if _, ok := exceptionClassesMap[processedStackHash]; !ok {
			exceptionClasses = append(exceptionClasses, Exception{
				ServiceId:          0, // TODO: add proper id
				ServiceVersion:     exception.ServerName,
				Name:               exception.Message,
				ProcessedStack:     processedStack,
				ProcessedStackHash: processedStackHash,
			})
			exceptionClassesMap[processedStackHash] = len(exceptionClasses) - 1
		}

		if _, ok := exceptionClassInstancesMap[rawStackHash]; !ok {
			exceptionClassInstances = append(exceptionClassInstances, ExceptionInstance{
				ProcessedStackHash: processedStackHash, // Used to reference exception_class_id later
				ProcessedDataHash:  processedDataHash,  // Used to reference exception_data_id later
				RawStack:           rawStack,
				RawStackHash:       rawStackHash,
			})
			exceptionClassInstancesMap[rawStackHash] = len(exceptionClassInstances) - 1
		}

		// The unique key should be the raw stack, the processed stack, and the time period,
		// since the count should keep track of an exception instance in a certain time frame.
		t := PythonUnixToGoUnix(exception.Timestamp).UTC()
		key := KeyExceptionPeriod{
			rawStackHash,
			processedDataHash,
			FindBoundingTime(t),
		}
		if _, ok := exceptionClassInstancePeriodsMap[key]; !ok {
			exceptionClassInstancePeriods = append(exceptionClassInstancePeriods, ExceptionInstancePeriod{
				CreatedAt:         key.TimePeriod,
				UpdatedAt:         t,
				RawStackHash:      rawStackHash,      // Used to reference exception_class_instance_id later
				ProcessedDataHash: processedDataHash, // Used to reference exception_data_id later
				Count:             1,
			})
			exceptionClassInstancePeriodsMap[key] = len(exceptionClassInstancePeriods) - 1
		} else {
			exceptionClassInstancePeriods[exceptionClassInstancePeriodsMap[key]].Count++
		}

		if _, ok := exceptionDataMap[processedDataHash]; !ok {
			exceptionData = append(exceptionData, ExceptionData{
				RawData:           processedData,
				ProcessedData:     processedData,
				ProcessedDataHash: processedDataHash,
			})
			exceptionDataMap[processedDataHash] = len(exceptionData) - 1
		}
	}

	if _, err := es.ds.AddExceptions(exceptionClasses); err != nil {
		es.log.Print("Error while inserting exceptions")
	}

	if _, err := es.ds.AddExceptionData(exceptionData); err != nil {
		es.log.Print("Error while inserting exception data")
	}
	// Query since upsert does not return ids
	if err := es.ds.QueryExceptions(exceptionClasses); err != nil {
		es.log.Print("Error while querying exception class")
	}
	// Query since upsert does not return ids
	if err := es.ds.QueryExceptionData(exceptionData); err != nil {
		es.log.Print("Error while querying exception data")
	}

	// Add the ids generated from above
	for _, idx := range exceptionClassInstancesMap {
		stackHash := exceptionClassInstances[idx].ProcessedStackHash
		dataHash := exceptionClassInstances[idx].ProcessedDataHash
		exceptionClassInstances[idx].ExceptionId =
			exceptionClasses[exceptionClassesMap[stackHash]].Id
		exceptionClassInstances[idx].ExceptionDataId =
			exceptionData[exceptionDataMap[dataHash]].Id
	}

	if _, err := es.ds.AddExceptionInstances(exceptionClassInstances); err != nil {
		es.log.Print("Error while inserting exception instances")
	}

	if err := es.ds.QueryExceptionInstances(exceptionClassInstances); err != nil {
		es.log.Print("Error while querying exception instances")
	}
	// Add the ids generated from above
	for _, idx := range exceptionClassInstancePeriodsMap {
		stackHash := exceptionClassInstancePeriods[idx].RawStackHash
		dataHash := exceptionClassInstancePeriods[idx].ProcessedDataHash
		exceptionClassInstancePeriods[idx].ExceptionInstanceId =
			exceptionClassInstances[exceptionClassInstancesMap[stackHash]].Id
		exceptionClassInstancePeriods[idx].ExceptionDataId =
			exceptionData[exceptionDataMap[dataHash]].Id
	}

	if _, err := es.ds.AddExceptioninstancePeriods(exceptionClassInstancePeriods); err != nil {
		es.log.Print("Error while inserting exception time periods")
	}
}
