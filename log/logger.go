package log

import (
	"bufio"
	"encoding/json"
	"github.com/ContextLogic/eventsum/datastore"
	. "github.com/ContextLogic/eventsum/models"
	"github.com/ContextLogic/eventsum/rules"
	"github.com/ContextLogic/eventsum/util"
	log "github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"reflect"
	"time"
)

var GlobalRule *rules.Rule

// helper functions
func open(filename string) (*os.File, error) {
	return os.OpenFile(filename+".log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
}

type config struct {
	LogSaveDataInterval        int    `json:"log_save_data_interval"`
	LogDataPeriodCheckInterval int    `json:"log_data_period_check_interval"`
	Environment                string `json:"environment"`
	AppDir                     string `json:"app_logging"`
	DataDir                    string `json:"data_logging"`
	DataSourceInstance         string `json:"data_source_instance"`
	DataSourceSchema           string `json:"data_source_schema"`
}

// There are two types of logging in this service: App logginng and Data logging.
// App logging: Debug, info, System calls, etc.
// Data logging: real data, events that failed to be added to DB, etc.
type Logger struct {
	app      *log.Logger
	data     *log.Logger
	eventLog failedEventsLog
	ds       datastore.DataStore

	appDir  string
	dataDir string

	endOfDay    time.Time    // used for log rotation
	tickerDump  *time.Ticker // used for dumping logs
	tickerCheck *time.Ticker // used for periodically checking logs
}

// This struct is what is being stored into the data log file. This is the summarized
// version of all the unadded events within a given time period
type failedEventsLog struct {
	Bases     []EventBase           `json:"event_base"`
	Instances []EventInstance       `json:"event_instance"`
	Periods   []EventInstancePeriod `json:"event_instance_period"`
	Details   []EventDetail         `json:"event_detail"`

	BaseMap     map[string]int         `json:"event_base_map"`
	InstanceMap map[string]int         `json:"event_instance_map"`
	PeriodMap   map[KeyEventPeriod]int `json:"event_instance_period_map"`
	DetailMap   map[string]int         `json:"event_detail_map"`
	// TODO: Add mutex?
}

// Starts the logging timers
func (l *Logger) Start(c config) {
	for {
		select {
		case <-l.tickerDump.C:
			l.SaveEventsToLogFile()
		case <-l.tickerCheck.C:
			l.PeriodicCheck(c)
		}
	}
}

func (l *Logger) EventLog() *failedEventsLog {
	return &l.eventLog
}

func (l *Logger) App() *log.Logger {
	start := time.Now()
	if l.endOfDay.Before(start) {
		l.endOfDay.Add(24 * time.Hour)
		if file, err := open(filepath.Join(l.appDir, l.endOfDay.Format("2006-01-02"))); err == nil {
			l.app.Out = file
		}
	}
	return l.app
}

func (l *Logger) Data() *log.Logger {
	start := time.Now()
	if l.endOfDay.Before(start) {
		l.endOfDay.Add(24 * time.Hour)
		if file, err := open(filepath.Join(l.dataDir, l.endOfDay.Format("2006-01-02"))); err == nil {
			l.data.Out = file
		}
	}
	return l.data
}

// Move all the data saved in the logs to a logfile
func (l *Logger) SaveEventsToLogFile() {
	// Check if there is anything to save
	if len(l.eventLog.Bases) == 0 && len(l.eventLog.Details) == 0 &&
		len(l.eventLog.Instances) == 0 && len(l.eventLog.Periods) == 0 {
		return
	}
	l.App().Info("Saving failed events into the log file!")

	l.Data().WithFields(log.Fields{
		"event_base":            l.eventLog.Bases,
		"event_base_map":        l.eventLog.BaseMap,
		"event_instance":        l.eventLog.Instances,
		"event_instance_map":    l.eventLog.InstanceMap,
		"event_instance_period": l.eventLog.Periods,
		//"event_instance_period_map": l.eventLog.PeriodMap,
		"event_detail":     l.eventLog.Details,
		"event_detail_map": l.eventLog.DetailMap,
	}).Info("Dumping event data now")

	// Clear the eventLog data after saving successfully
	l.eventLog.Bases = []EventBase{}
	l.eventLog.BaseMap = make(map[string]int)
	l.eventLog.Instances = []EventInstance{}
	l.eventLog.InstanceMap = make(map[string]int)
	l.eventLog.Periods = []EventInstancePeriod{}
	l.eventLog.PeriodMap = make(map[KeyEventPeriod]int)
	l.eventLog.Details = []EventDetail{}
	l.eventLog.DetailMap = make(map[string]int)
}

// Check if there is an open db connection, and anything to put in from the logs
func (l *Logger) PeriodicCheck(conf config) {
	datastore.GlobalRule = GlobalRule
	filename := filepath.Join(l.dataDir, l.endOfDay.Format("2006-01-02"))
	file, err := open(filename)
	if err != nil {
		l.App().WithFields(log.Fields{
			"filename": filename,
		}).Error("Unable to open file")
		return
	}
	defer file.Close()
	stats, err := file.Stat()
	if err != nil {
		return
	}
	if stats.Size() == 0 {
		return
	}
	l.App().Info("Running periodic check of log files")
	// read the file line by line
	reader := bufio.NewReader(file)
	var failedEvent failedEventsLog
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			break
		}
		json.Unmarshal(line, &failedEvent)
		// Add to DB
		if err := l.ds.AddEvents(failedEvent.Bases); len(err) != 0 {
			// TODO
			l.App().Error(err)
			return
		}

		if err := l.ds.AddEventDetails(failedEvent.Details); len(err) != 0 {
			// TODO
			l.App().Error(err)
			return
		}

		for _, idx := range failedEvent.InstanceMap {
			dataHash := failedEvent.Instances[idx].ProcessedDataHash
			detailHash := failedEvent.Instances[idx].ProcessedDetailHash
			failedEvent.Instances[idx].EventBaseId =
				failedEvent.Bases[failedEvent.BaseMap[dataHash]].Id
			failedEvent.Instances[idx].EventDetailId =
				failedEvent.Details[failedEvent.DetailMap[detailHash]].Id
		}

		if err := l.ds.AddEventInstances(failedEvent.Instances); len(err) != 0 {
			// TODO
			l.App().Error(err)
			return
		}

		for idx := range failedEvent.Periods {
			dataHash := failedEvent.Periods[idx].RawDataHash
			failedEvent.Periods[idx].EventInstanceId =
				failedEvent.Instances[failedEvent.InstanceMap[dataHash]].Id
		}

		if err := l.ds.AddEventinstancePeriods(failedEvent.Periods); len(err) != 0 {
			// TODO
			l.App().Error(err)
			return
		}
	}
	// delete the log file
	if err := os.Truncate(filename+".log", 0); err != nil {
		l.App().Error(err)
	}
}

// Logs the data into failedEventsLog. Checks the type of the summarized event,
// and calls the respective function
func (l *failedEventsLog) LogData(event interface{}) {
	// Check the event type
	t := reflect.TypeOf(event)
	k := t.Kind()
	if k != reflect.Struct {
		return
	}

	switch t.Name() {
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
		RawDataHash: period.RawDataHash,
		StartTime:   period.StartTime,
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

func parseConfig(file string) config {
	c := config{
		5,
		10,
		"dev",
		"log",
		"log",
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
func NewLogger(configFile string, ds datastore.DataStore) *Logger {
	config := parseConfig(configFile)
	_, endOfDay := util.FindBoundingTime(time.Now(), 1440)

	l := Logger{
		app:  log.New(),
		data: log.New(),
		ds:   ds,
		eventLog: failedEventsLog{
			BaseMap:     make(map[string]int),
			InstanceMap: make(map[string]int),
			PeriodMap:   make(map[KeyEventPeriod]int),
			DetailMap:   make(map[string]int),
		},
		appDir:      config.AppDir,
		dataDir:     config.DataDir,
		endOfDay:    endOfDay,
		tickerDump:  time.NewTicker(time.Duration(config.LogSaveDataInterval) * time.Second),
		tickerCheck: time.NewTicker(time.Duration(config.LogDataPeriodCheckInterval) * time.Second),
	}
	l.data.Formatter = &log.JSONFormatter{}

	// If environment is dev, send output to stdout
	if config.Environment == "dev" {
		l.app.Out = os.Stdout
		l.data.Out = os.Stdout
		l.app.Level = log.DebugLevel
		l.data.Level = log.DebugLevel
	} else {
		appFile, err := open(filepath.Join(l.appDir, l.endOfDay.Format("2006-01-02")))

		if err != nil {
			log.Warn(err)
		} else {
			l.app.Out = appFile
		}

		dataFile, err := open(filepath.Join(l.dataDir, l.endOfDay.Format("2006-01-02")))

		if err != nil {
			log.Warn(err)
		} else {
			l.data.Out = dataFile
		}

	}

	// run log processing in a goroutine
	go l.Start(config)
	return &l
}
