package main

import (
	"crypto/sha256"
	"encoding/base64"
)

func Hash(s string) string {
	hasher := sha256.New()
	hasher.Write([]byte(s))
	return base64.URLEncoding.EncodeToString(hasher.Sum(nil))
}

func ProcessException(exception *UnaddedException) string {
	// Strip the line numbers
	return ""
}

func ProcessData(data map[string]interface{}) string {
	return ""
}