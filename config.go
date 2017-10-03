package main

import (
	"encoding/json"
	"fmt"
	"os"
)

// EMConfig are settings used ... XXX
type EMConfig struct {
	PgAddress  string        `json:"postgres_address"`
	PgUsername string        `json:"postgres_user"`
	PgPassword string        `json:"postgres_pass"`
	PgDatabase string        `json:"postgres_database"`
	BatchSize  int           `json:"exception_batch_limit"`
	TimeLimit  int `json:"exception_time_limit"` // in seconds
	ServerPort int           `json:"server_port"`
	TimeInterval int `json:"time_interval"` // in minutes
	Args       map[string]interface{}   `json:"args"`
}

func DefaultConfig() (EMConfig) {
	return EMConfig{
		PgAddress: "localhost:5432",
		PgUsername: "username",
		PgPassword: "password",
		PgDatabase: "exception_master",
		BatchSize: 5,
		TimeLimit: 5,
		ServerPort: 8080,
		TimeInterval: 15,
		Args: make(map[string]interface{}),
	}
}

// ParseEMConfig parses configuration out of a json file
func ParseEMConfig(file string) (EMConfig, error) {

	configuration := DefaultConfig()
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