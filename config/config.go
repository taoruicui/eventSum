package config

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

type Flags struct {
	ConfigFile string `short:"c" long:"config" description:"location of configuration file"`
	Region     string `short:"r" long:"region" description:"the region server locate at"`
}

// config are settings used ... XXX
type EventsumConfig struct {
	DataSourceInstance string                    `json:"data_source_instance"`
	DataSourceSchema   string                    `json:"data_source_schema"`
	DatabaseName       string                    `json:"database_name"`
	LogConfigFile      string                    `json:"log_config_file"`
	BatchSize          int                       `json:"event_batch_limit"`
	TimeLimit          int                       `json:"event_time_limit"` // in seconds
	ServerPort         int                       `json:"server_port"`
	TimeInterval       int                       `json:"time_interval"` // in minutes
	TimeFormat         string                    `json:"time_format"`
	Services           map[string]map[string]int `json:"services"`
	Environments       map[string]map[string]int `json:"environments"`
	RegionsMap         map[string]int            `json:"regions_map"`
	Region             string                    `json:"region"`
	DrainSecond        int                       `json:"drain_second"` // in seconds
}

func DefaultConfig() EventsumConfig {
	return EventsumConfig{
		DataSourceInstance: "config/datasourceinstance.yaml",
		DataSourceSchema:   "config/schema.json",
		DatabaseName:       "eventsum",
		LogConfigFile:      "config/logconfig.json",
		BatchSize:          5,
		TimeLimit:          5,
		ServerPort:         8080,
		TimeInterval:       15,
		TimeFormat:         "2006-01-02 15:04:05",
		Services:           map[string]map[string]int{},
		Environments:       map[string]map[string]int{},
		RegionsMap:         map[string]int{},
		Region:             "default",
		DrainSecond:        0,
	}
}

// ParseEMConfig parses configuration out of a json file
func ParseEventsumConfig(file string, region string) (EventsumConfig, error) {

	configuration := DefaultConfig()
	f, err := os.Open(file)
	if err != nil {
		return configuration, fmt.Errorf("Error", err)
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	decodeErr := decoder.Decode(&configuration)
	if decodeErr != nil {
		return configuration, fmt.Errorf("error: %s", decodeErr)
	}

	// override the region passed from flags
	if region != "" {
		configuration.Region = region
	}

	return configuration, nil
}

func ParseDataSourceInstanceConfig(c string) (string, error) {
	f, err := os.Open(c)
	if err != nil {
		return "", err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	var line string
	for scanner.Scan() {
		line = strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "pg_string: ") {
			return strings.Replace(line, "pg_string:", "", 1), nil
		}
	}
	return "", errors.New("no config found error")
}
