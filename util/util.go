package util

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/jacksontj/dataman/src/datamantype"
	"github.com/mitchellh/mapstructure"
	"math"
	"reflect"
	"time"
)

// returns the start and end times of the interval bounding time t,
// interval specific as minutes
func FindBoundingTime(t time.Time, interval int) (time.Time, time.Time) {
	duration := time.Duration(interval) * time.Minute
	s := t.Truncate(duration)
	return s, s.Add(duration)
}

// convert Python unix time.time to Go unix time.Time
func PythonUnixToGoUnix(t float64) time.Time {
	seconds := int64(t)
	nanoseconds := int64(t-math.Floor(t)) * int64(time.Second)
	return time.Unix(seconds, nanoseconds)
}

func Hash(i interface{}) string {
	b, err := json.Marshal(i)
	if err != nil {
		fmt.Println("Error", err)
	}
	hasher := sha256.New()
	hasher.Write(b)
	return base64.URLEncoding.EncodeToString(hasher.Sum(nil))
}

// Converts string to time, used for mapstructure.NewDecoder()
func stringToDateTimeHook(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {

	if t == reflect.TypeOf(time.Time{}) && f == reflect.TypeOf("") {
		return time.Parse(datamantype.DateTimeFormatStr, data.(string))
	}
	return data, nil
}

// mapstructure decode
func MapDecode(source, target interface{}) error {
	config := mapstructure.DecoderConfig{
		DecodeHook: stringToDateTimeHook,
		Result:     target,
	}
	decoder, err := mapstructure.NewDecoder(&config)
	if err != nil {
		return err
	}
	return decoder.Decode(source)
}

// Turns the raw stack into a string file
//func generateFullStack(st StackTrace) string {
//	// buffer to resolve string concat issues
//	var buffer bytes.Buffer
//	buffer.WriteString(fmt.Sprintf("%s: %s\n", st.Type, st.Value))
//	for _, frame := range st.Frames {
//		str := fmt.Sprintf("File\"%s\", line %d, in %v\n  %s\n",
//			frame.Filename,
//			frame.LineNo,
//			frame.Function,
//			strings.TrimSpace(frame.ContextLine),
//		)
//		buffer.WriteString(str)
//	}
//	fmt.Print(buffer.String())
//	return buffer.String()
//}

//func extractDataFromEvent(e UnaddedEvent) map[string]interface{} {
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

//func toJson(data interface{}) string {
//	res, _ := json.Marshal(data)
//	return string(res)
//}
