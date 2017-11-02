package config

import (
	"encoding/json"
	"fmt"
	"os"
)

// config are settings used ... XXX
type EventsumConfig struct {
	DataSourceInstance string                 `json:"data_source_instance"`
	DataSourceSchema   string                 `json:"data_source_schema"`
	LogConfigFile      string                 `json:"log_config_file"`
	BatchSize          int                    `json:"event_batch_limit"`
	TimeLimit          int                    `json:"event_time_limit"` // in seconds
	ServerPort         int                    `json:"server_port"`
	TimeInterval       int                    `json:"time_interval"` // in minutes
	Args               map[string]interface{} `json:"args"`
}

func DefaultConfig() EventsumConfig {
	return EventsumConfig{
		DataSourceInstance: "config/datasourceinstance.yaml",
		DataSourceSchema:   "config/schema.json",
		LogConfigFile:      "config/logconfig.json",
		BatchSize:          5,
		TimeLimit:          5,
		ServerPort:         8080,
		TimeInterval:       15,
		Args:               make(map[string]interface{}),
	}
}

// ParseEMConfig parses configuration out of a json file
func ParseEventsumConfig(file string) (EventsumConfig, error) {

	configuration := DefaultConfig()
	f, err := os.Open(file)
	if err != nil {
		return configuration, fmt.Errorf("Error", err)
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	decodeErr := decoder.Decode(&configuration)
	if decodeErr != nil {
		return configuration, fmt.Errorf("Error: ", decodeErr)
	}
	return configuration, nil
}
