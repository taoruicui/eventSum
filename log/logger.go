package log

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/ContextLogic/eventsum/datastore"
	. "github.com/ContextLogic/eventsum/models"
	"github.com/ContextLogic/eventsum/rules"
	"github.com/ContextLogic/eventsum/util"
	log "github.com/sirupsen/logrus"
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
	EventDir                   string `json:"event_logging"`
}

// There are two types of logging in this service: App logginng and Data logging.
// App logging: Debug, info, System calls, etc.
// Data logging: real data, events that failed to be added to DB, etc.
type Logger struct {
	app      *log.Logger
	data     *log.Logger
	eventLog failedEventsLog
	ds       datastore.DataStore

	appDir   string
	dataDir  string
	eventDir string

	endOfDay    time.Time    // used for log rotation
	tickerDump  *time.Ticker // used for dumping logs
	tickerCheck *time.Ticker // used for periodically checking logs
}

// This struct is what is being stored into the data log file. This is the summarized
// version of all the unadded events within a given time period
type failedEventsLog struct {
	Bases     map[string]EventBase           `json:"event_base"`
	Instances map[string]EventInstance       `json:"event_instance"`
	Periods   map[string]EventInstancePeriod `json:"event_instance_period"`
	Details   map[string]EventDetail         `json:"event_detail"`
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

func (l *Logger) DropEventToDiskLog(evts []UnaddedEvent) {

	filename := fmt.Sprintf("%d-events-buffers", time.Now().Unix())

	evtFile, err := open(filepath.Join(l.eventDir, filename))
	defer evtFile.Close()

	if err != nil {
		//drop the event directly
		return
	}
	for _, evt := range evts {
		jsonData, err := json.Marshal(evt)
		if err != nil {
			//drop the event directly
			continue
		}

		evtFile.WriteString(string(jsonData))
		evtFile.WriteString("\n")
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
		"event_instance":        l.eventLog.Instances,
		"event_instance_period": l.eventLog.Periods,
		//"event_instance_period_map": l.eventLog.PeriodMap,
		"event_detail": l.eventLog.Details,
	}).Info("Dumping event data now")

	// Clear the eventLog data after saving successfully
	l.eventLog.Bases = make(map[string]EventBase)
	l.eventLog.Instances = make(map[string]EventInstance)
	l.eventLog.Periods = make(map[string]EventInstancePeriod)
	l.eventLog.Details = make(map[string]EventDetail)
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
		for hash, base := range failedEvent.Bases {
			if err := l.ds.AddEventBase(&base); err != nil {
				// TODO
				l.App().Error(err)
				return
			}
			failedEvent.Bases[hash] = base
		}

		for hash, detail := range failedEvent.Details {
			if err := l.ds.AddEventDetail(&detail); err != nil {
				// TODO
				l.App().Error(err)
				return
			}
			failedEvent.Details[hash] = detail
		}

		for hash, instance := range failedEvent.Instances {
			instance.EventBaseId = failedEvent.Bases[instance.ProcessedDataHash].Id
			instance.EventDetailId = failedEvent.Details[instance.ProcessedDetailHash].Id
			failedEvent.Instances[hash] = instance
		}

		for hash, instance := range failedEvent.Instances {
			if err := l.ds.AddEventInstance(&instance); err != nil {
				// TODO
				l.App().Error(err)
				return
			}
			failedEvent.Instances[hash] = instance
		}

		for key, period := range failedEvent.Periods {
			period.EventInstanceId = failedEvent.Instances[period.RawDataHash].Id
			failedEvent.Periods[key] = period
		}

		for key, period := range failedEvent.Periods {
			if err := l.ds.AddEventInstancePeriod(&period); err != nil {
				// TODO
				l.App().Error(err)
				return
			}
			failedEvent.Periods[key] = period
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
	if _, ok := l.Bases[base.ProcessedDataHash]; !ok {
		l.Bases[base.ProcessedDataHash] = base
	}
}

func (l *failedEventsLog) logEventInstance(instance EventInstance) {
	if _, ok := l.Instances[instance.ProcessedDataHash]; !ok {
		l.Instances[instance.ProcessedDataHash] = instance
	}
}

func (l *failedEventsLog) logEventInstancePeriod(period EventInstancePeriod) {
	str := fmt.Sprintf("%v%v", period.RawDataHash, period.StartTime.Format(time.RFC3339))
	if _, ok := l.Periods[str]; !ok {
		l.Periods[str] = period
	} else {
		e := l.Periods[str]
		e.Count += period.Count
		// TODO: handle error
		e.CounterJson, _ = GlobalRule.Consolidate(period.CounterJson, e.CounterJson)
		l.Periods[str] = e
	}
}

func (l *failedEventsLog) logEventDetail(detail EventDetail) {
	if _, ok := l.Details[detail.ProcessedDetailHash]; !ok {
		l.Details[detail.ProcessedDetailHash] = detail
	}
}

func parseConfig(file string) config {
	c := config{
		5,
		10,
		"dev",
		"log",
		"log",
		"log",
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
			Bases:     make(map[string]EventBase),
			Instances: make(map[string]EventInstance),
			Periods:   make(map[string]EventInstancePeriod),
			Details:   make(map[string]EventDetail),
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
