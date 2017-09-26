package main

import (
	"crypto/sha256"
	"encoding/base64"
	"bytes"
	"time"
	"math"
	"encoding/json"
)

/* TIME FUNCTIONS */

var TIME_INTERVAL = 15 * time.Minute

// returns the start time of the interval bounding time t
func FindBoundingTime(t time.Time) (time.Time) {
	return t.Truncate(time.Duration(TIME_INTERVAL))
}

// convert Python unix time.time to Go unix time.Time
func PythonUnixToGoUnix(t float64) (time.Time) {
	seconds := int64(t)
	nanoseconds := int64(t - math.Floor(t)) * int64(time.Second)
	return time.Unix(seconds, nanoseconds)
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

func ProcessStack(st StackTrace) string {
	// Strip the line numbers
	return GenerateFullStack(st)
}

func ProcessData(data map[string]interface{}) string {
	res, _ := json.Marshal(data)
	return string(res)
}

