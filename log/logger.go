package log

import (
	log "github.com/sirupsen/logrus"
	"github.com/ContextLogic/eventsum/rules"
	. "github.com/ContextLogic/eventsum/models"
	"encoding/json"
	"os"
	"reflect"
)

var GlobalRule *rules.Rule

type config struct {
	TimePeriod int `json:"time_period"`
	Environment string `json:"environment"`
	AppLogging string `json:"app_logging"`
	DataLogging string `json:"data_logging"`
}

// There are two types of logging in this service: App logginng and Data logging.
// App logging: Debug, info, System calls, etc.
// Data logging: real data, events that failed to be added to DB, etc.
type Logger struct {
	App *log.Logger
	Data *log.Logger
	EventLog failedEventsLog
}

type failedEventsLog struct {
	Base []EventBase `json:"base"`
	Instance []EventInstance `json:"instance"`
	Period []EventInstancePeriod `json:"period"`
	Detail []EventDetail `json:"detail"`

	BaseMap map[string]int `json:"base_map"`
	InstanceMap map[string]int `json:"instance_map"`
	PeriodMap map[KeyEventPeriod]int `json:"period_map"`
	DetailsMap map[string]int `json:"details_map"`
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
		l.Base = append(l.Base, base)
		l.BaseMap[base.ProcessedDataHash] = len(l.Base) - 1
	}
}

func (l *failedEventsLog) logEventInstance(instance EventInstance) {
	if _, ok := l.InstanceMap[instance.ProcessedDataHash]; !ok {
		l.Instance = append(l.Instance, instance)
		l.InstanceMap[instance.ProcessedDataHash] = len(l.Instance) - 1
	}
}

func (l *failedEventsLog) logEventInstancePeriod(period EventInstancePeriod) {
	key := KeyEventPeriod{
		RawDataHash:period.RawDataHash,
		StartTime: period.StartTime,
	}
	if _, ok := l.PeriodMap[key]; !ok {
		l.Period = append(l.Period, period)
		l.PeriodMap[key] = len(l.Period) - 1
	} else {
		e := &l.Period[l.PeriodMap[key]]
		e.Count += period.Count
		// TODO: handle error
		e.CounterJson, _ = GlobalRule.Consolidate(period.CounterJson, e.CounterJson)
	}
}

func (l *failedEventsLog) logEventDetail(detail EventDetail) {
	if _, ok := l.DetailsMap[detail.ProcessedDetailHash]; !ok {
		l.Detail = append(l.Detail, detail)
		l.DetailsMap[detail.ProcessedDetailHash] = len(l.Detail) - 1
	}
}

func parseConfig(file string) config {
	c := config{
		5,
		"dev",
		"log/system.log",
		"log/data.log",
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
	var appFile *os.File
	var dataFile *os.File
	var err error

	// If environment is dev, send output to stdout
	if config.Environment == "dev" {
		appFile = os.Stdout
		dataFile = os.Stdout
	} else {
		appFile, err = os.OpenFile(config.AppLogging, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
		if err != nil {
			log.Warn(err)
		}
		dataFile, err = os.OpenFile(config.DataLogging, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
		if err != nil {
			log.Warn(err)
		}
	}

	l := Logger{
		App: log.New(),
		Data: log.New(),
		EventLog: failedEventsLog{
			BaseMap: make(map[string]int),
			InstanceMap: make(map[string]int),
			PeriodMap: make(map[KeyEventPeriod]int),
			DetailsMap: make(map[string]int),
		},
	}
	l.App.Out = appFile
	l.Data.Out = dataFile
	return &l
}