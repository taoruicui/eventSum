package log

import (
	log "github.com/sirupsen/logrus"
	"encoding/json"
	"os"
)

type config struct {
	TimePeriod int `json:"time_period"`
	AppLogging string `json:"app_logging"`
	DataLogging string `json:"data_logging"`
}

type Logger struct {
	App *log.Logger
	Data *log.Logger
}

//func (l *Logger) Errorf(format string, args...interface{}) {
//	l.App.Errorf(format, args...)
//}




func parseConfig(file string) config {
	c := config{
		5,
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
	appFile, err := os.OpenFile(config.AppLogging, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil {
		log.Warn(err)
		appFile = os.Stderr
	}
	dataFile, err := os.OpenFile(config.DataLogging, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil {
		log.Warn(err)
		dataFile = os.Stderr
	}
	l := Logger{
		App: log.New(),
		Data: log.New(),
	}
	l.App.Out = appFile
	l.Data.Out = dataFile
	return &l
}