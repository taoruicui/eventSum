package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// EMConfig are settings used ... XXX
type EMConfig struct {
	PgAddress  string        `json:"postgres_address"`
	PgUsername string        `json:"postgres_user"`
	PgPassword string        `json:"postgres_pass"`
	PgDatabase string        `json:"postgres_database"`
	BatchSize  int           `json:"exception_batch_limit"`
	TimeLimit  time.Duration `json:"exception_time_limit"`
	ServerPort int           `json:"server_port"`
	TimeInterval int `json:"time_interval"`
	Args       map[string]interface{}   `json:"args"`
}

// ParseEMConfig parses configuration out of a json file
func ParseEMConfig(file string) (EMConfig, error) {

	configuration := EMConfig{}
	f, err := os.Open(file)
	if err != nil {
		return configuration, fmt.Errorf("Error", err)
	}
	decoder := json.NewDecoder(f)
	decodeErr := decoder.Decode(&configuration)
	if decodeErr != nil {
		return configuration, fmt.Errorf("Error: ", decodeErr)
	}
	return configuration, nil
}
