package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math"
	"time"
)

/* TIME FUNCTIONS */

var TIME_INTERVAL = 15 * time.Minute

// returns the start time of the interval bounding time t
func FindBoundingTime(t time.Time) time.Time {
	return t.Truncate(time.Duration(TIME_INTERVAL))
}

// convert Python unix time.time to Go unix time.Time
func PythonUnixToGoUnix(t float64) time.Time {
	seconds := int64(t)
	nanoseconds := int64(t-math.Floor(t)) * int64(time.Second)
	return time.Unix(seconds, nanoseconds)
}

func ValidateUnaddedException(e UnaddedException) error {
	return nil
}

func Hash(s string) string {
	hasher := sha256.New()
	hasher.Write([]byte(s))
	return base64.URLEncoding.EncodeToString(hasher.Sum(nil))
}

// Turns the raw stack into a string file
func GenerateFullStack(st StackTrace) string {
	// buffer to resolve string concat issues
	var buffer bytes.Buffer
	for _, frame := range st.Frames {
		// Loop through precontext
		for _, pre := range frame.PreContext {
			buffer.WriteString(pre)
			buffer.WriteString("\n")
		}
		buffer.WriteString(frame.ContextLine)
		buffer.WriteString("\n")
		// Loop through postcontext
		for _, post := range frame.PreContext {
			buffer.WriteString(post)
			buffer.WriteString("\n")
		}
	}
	return buffer.String()
}

func ProcessStack(st string) string {
	// Strip the line numbers
	return st
}

func ExtractDataFromException(e UnaddedException) map[string]interface{} {
	data := make(map[string]interface{})

	// add system arguments
	sysArgs := make(map[string]interface{})
	for key, val := range e.Extra { sysArgs[key] = val }
	data["system_args"] = sysArgs

	// add stack variables
	stackVars := make(map[int]interface{})
	for idx, frame := range e.StackTrace.Frames { stackVars[idx] = frame.Vars }
	data["stack_vars"] = stackVars
	return data
}

func ProcessData(data map[string]interface{}) string {
	res, _ := json.Marshal(data)
	return string(res)
}
