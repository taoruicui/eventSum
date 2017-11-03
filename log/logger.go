package log

import (
	log "github.com/sirupsen/logrus"
	"github.com/ContextLogic/eventsum/rules"
	. "github.com/ContextLogic/eventsum/models"
	"encoding/json"
	"os"
	"reflect"
	"time"
	"bufio"
	"github.com/ContextLogic/eventsum/datastore"
)

var GlobalRule *rules.Rule

type config struct {
	TimePeriod int `json:"time_period"`
	Environment string `json:"environment"`
	AppLogging string `json:"app_logging"`
	DataLogging string `json:"data_logging"`
	DataSourceInstance string `json:"data_source_instance"`
	DataSourceSchema string `json:"data_source_schema"`
}

// There are two types of logging in this service: App logginng and Data logging.
// App logging: Debug, info, System calls, etc.
// Data logging: real data, events that failed to be added to DB, etc.
type Logger struct {
	App *log.Logger
	Data *log.Logger
	EventLog failedEventsLog

	ticker *time.Ticker
}

type failedEventsLog struct {
	Bases []EventBase `json:"event_base"`
	Instances []EventInstance `json:"event_instance"`
	Periods []EventInstancePeriod `json:"event_instance_period"`
	Details []EventDetail `json:"event_detail"`

	BaseMap map[string]int `json:"event_base_map"`
	InstanceMap map[string]int `json:"event_instance_map"`
	PeriodMap map[KeyEventPeriod]int `json:"event_instance_period_map"`
	DetailMap map[string]int `json:"event_detail_map"`
	// TODO: Add mutex?
}

func (l *Logger) Start() {
	for {
		select {
		case <- l.ticker.C:
			l.App.Info("Saving failed events into the log file!")
			l.SaveEventsToLogFile()
		}
	}
}

// Move all the data saved in the logs to a logfile
func (l *Logger) SaveEventsToLogFile() {
	// Check if there is anything to save
	if 	len(l.EventLog.Bases) == 0 && len(l.EventLog.Details) == 0 &&
		len(l.EventLog.Instances) == 0 && len(l.EventLog.Periods) == 0 {
		return
	}

	l.Data.WithFields(log.Fields{
		"event_base": l.EventLog.Bases,
		"event_base_map": l.EventLog.BaseMap,
		"event_instance": l.EventLog.Instances,
		"event_instance_map": l.EventLog.InstanceMap,
		"event_instance_period": l.EventLog.Periods,
		//"event_instance_period_map": l.EventLog.PeriodMap,
		"event_detail": l.EventLog.Details,
		"event_detail_map": l.EventLog.DetailMap,
	}).Info("Dumping event data now")

	// Clear the EventLog data after saving successfully
	l.EventLog.Bases = []EventBase{}
	l.EventLog.BaseMap = make(map[string]int)
	l.EventLog.Instances = []EventInstance{}
	l.EventLog.InstanceMap = make(map[string]int)
	l.EventLog.Periods = []EventInstancePeriod{}
	l.EventLog.PeriodMap = make(map[KeyEventPeriod]int)
	l.EventLog.Details = []EventDetail{}
	l.EventLog.DetailMap = make(map[string]int)
}

// Check if there is an open db connection, and anything to put in from the logs
func (l *Logger) PeriodicCheck(filename string, conf config) {
	ds, err := datastore.NewDataStore(conf.DataSourceInstance, conf.DataSourceInstance)
	if err != nil {
		return
	}
	file, err := os.Open(filename)
	if err != nil {
		l.App.WithFields(log.Fields{
			"filename": filename,
		}).Error("Unable to open file")
		return
	}
	stats, err := file.Stat()
	if err != nil {

	}
	if stats.Size() == 0 {

	}
	// read the file line by line
	reader := bufio.NewReader(file)
	var failedEvent failedEventsLog
	for {
		line, err := reader.ReadBytes('\n')
		json.Unmarshal(line, &failedEvent)
		// Add to DB
		if err := ds.AddEvents(failedEvent.Bases); err != nil {
			// TODO
		}
		
		if err := ds.AddEventDetails(failedEvent.Details); err != nil {
			// TODO
		}
		
		for _, idx := range failedEvent.InstanceMap {
			dataHash := failedEvent.Instances[idx].ProcessedDataHash
			detailHash := failedEvent.Instances[idx].ProcessedDetailHash
			failedEvent.Instances[idx].EventBaseId =
				failedEvent.Bases[failedEvent.BaseMap[dataHash]].Id
			failedEvent.Instances[idx].EventDetailId =
				failedEvent.Details[failedEvent.DetailMap[detailHash]].Id
		}
		
		if err := ds.AddEventInstances(failedEvent.Instances); err != nil {
			// TODO
		}

		for _, idx := range failedEvent.PeriodMap {
			dataHash := failedEvent.Periods[idx].RawDataHash
			failedEvent.Periods[idx].EventInstanceId =
				failedEvent.Instances[failedEvent.InstanceMap[dataHash]].Id
		}

		if err := ds.AddEventinstancePeriods(failedEvent.Periods); err != nil {
			// TODO
		}

		if err != nil {
			break
		}
	}
}

// Logs the data into failedEventsLog
func (l *failedEventsLog) LogData(event interface{}) {
	// Check the event type
	t := reflect.TypeOf(event)
	k := t.Kind()
	if k != reflect.Struct {
		return
	}

	switch t.Name(){
	case "EventBase":
		l.logEventBase(event.(EventBase))
	case "EventInstance":
		l.logEventInstance(event.(EventInstance))
	case "EventInstancePeriod":
		l.logEventInstancePeriod(event.(EventInstancePeriod))
	case "EventDetail":
		l.logEventDetail(event.(EventDetail))
	}
}

func (l *failedEventsLog) logEventBase(base EventBase) {
	if _, ok := l.BaseMap[base.ProcessedDataHash]; !ok {
		l.Bases = append(l.Bases, base)
		l.BaseMap[base.ProcessedDataHash] = len(l.Bases) - 1
	}
}

func (l *failedEventsLog) logEventInstance(instance EventInstance) {
	if _, ok := l.InstanceMap[instance.ProcessedDataHash]; !ok {
		l.Instances = append(l.Instances, instance)
		l.InstanceMap[instance.ProcessedDataHash] = len(l.Instances) - 1
	}
}

func (l *failedEventsLog) logEventInstancePeriod(period EventInstancePeriod) {
	key := KeyEventPeriod{
		RawDataHash:period.RawDataHash,
		StartTime: period.StartTime,
	}
	if _, ok := l.PeriodMap[key]; !ok {
		l.Periods = append(l.Periods, period)
		l.PeriodMap[key] = len(l.Periods) - 1
	} else {
		e := &l.Periods[l.PeriodMap[key]]
		e.Count += period.Count
		// TODO: handle error
		e.CounterJson, _ = GlobalRule.Consolidate(period.CounterJson, e.CounterJson)
	}
}

func (l *failedEventsLog) logEventDetail(detail EventDetail) {
	if _, ok := l.DetailMap[detail.ProcessedDetailHash]; !ok {
		l.Details = append(l.Details, detail)
		l.DetailMap[detail.ProcessedDetailHash] = len(l.Details) - 1
	}
}

//func (l *failedEventsLog) HandleLogFmt(key, val []byte) error {
//	return nil
//}

func parseConfig(file string) config {
	c := config{
		5,
		"dev",
		"log/system.log",
		"log/data.log",
		"config/datasourceinstance.yaml",
		"config/schema.json",
	}
	f, err := os.Open(file)
	if err != nil {
		log.Error(err)
		return c
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	err = decoder.Decode(&c)
	if err != nil {
		log.Error(err)
	}
	return c
}

// Return new logger given config file
func NewLogger(configFile string) *Logger {
	config := parseConfig(configFile)

	l := Logger{
		App: log.New(),
		Data: log.New(),
		EventLog: failedEventsLog{
			BaseMap: make(map[string]int),
			InstanceMap: make(map[string]int),
			PeriodMap: make(map[KeyEventPeriod]int),
			DetailMap: make(map[string]int),
		},

		ticker: time.NewTicker(time.Duration(config.TimePeriod) * time.Second),
	}
	l.Data.Formatter = &log.JSONFormatter{}

	// If environment is dev, send output to stdout
	if config.Environment == "dev" {
		l.App.Out = os.Stdout
		l.Data.Out = os.Stdout
		l.App.Level = log.DebugLevel
		l.Data.Level = log.DebugLevel
	} else {
		appFile, err := os.OpenFile(config.AppLogging, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)

		if err != nil {
			log.Warn(err)
		} else {
			l.App.Out = appFile
		}

		dataFile, err := os.OpenFile(config.DataLogging, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)

		if err != nil {
			log.Warn(err)
		} else {
			l.Data.Out = dataFile
		}

	}

	// run log processing in a goroutine
	l.PeriodicCheck("log/data.log", config)
	go l.Start()
	return &l
}