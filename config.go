package main

import (
	"encoding/json"
	"fmt"
	"os"
)

// EMConfig are settings used ... XXX
type EMConfig struct {
	Address  string `json:"postgres_address"`
	User     string `json:"postgres_user"`
	Pass     string `json:"postgres_pass"`
	Database string `json:"postgres_database"`
	BatchSize int `json:"exception_batch_limit"`
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
