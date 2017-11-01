package log

import (
	log "github.com/sirupsen/logrus"
	"encoding/json"
	"os"
)

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
	}
	l.App.Out = appFile
	l.Data.Out = dataFile
	return &l
}