package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math"
	"time"
	"fmt"
	"strings"
)

// returns the start time of the interval bounding time t,
// interval specific as minutes
func FindBoundingTime(t time.Time, interval int) time.Time {
	duration := time.Duration(interval) * time.Minute
	return t.Truncate(duration)
}

// convert Python unix time.time to Go unix time.Time
func PythonUnixToGoUnix(t float64) time.Time {
	seconds := int64(t)
	nanoseconds := int64(t-math.Floor(t)) * int64(time.Second)
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
	buffer.WriteString(fmt.Sprintf("%s: %s\n", st.Type, st.Value))
	for _, frame := range st.Frames {
		str := fmt.Sprintf("File\"%s\", line %d, in %v\n  %s\n",
			frame.Filename,
			frame.LineNo,
			frame.Function,
			strings.TrimSpace(frame.ContextLine),
		)
		buffer.WriteString(str)
	}
	fmt.Print(buffer.String())
	return buffer.String()
}

func GenerateRawStack(st StackTrace) string {
	data := st
	for i := range data.Frames {
		data.Frames[i].Vars = nil
	}
	res, _ := json.Marshal(data)
	return string(res)
}

func ProcessStack(st StackTrace) string {
	// Strip the line numbers
	data := st
	for i := range data.Frames {
		data.Frames[i].Vars = nil
		data.Frames[i].LineNo = 0
	}
	res, _:= json.Marshal(data)
	return string(res)
}

//func ExtractDataFromEvent(e UnaddedEvent) map[string]interface{} {
//	data := make(map[string]interface{})
//
//	// add system arguments
//	sysArgs := make(map[string]interface{})
//	for key, val := range e.Extra { sysArgs[key] = val }
//	data["system_args"] = sysArgs
//
//	// add stack variables
//	stackVars := make(map[int]interface{})
//	for idx, frame := range e.StackTrace.Frames { stackVars[idx] = frame.Vars }
//	data["stack_vars"] = stackVars
//	return data
//}

func ToJson(data interface{}) string {
	res, _ := json.Marshal(data)
	return string(res)
}
