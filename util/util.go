package util

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/jacksontj/dataman/src/datamantype"
	"github.com/mitchellh/mapstructure"
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

// Calculates the average between all times
func AvgTime(times ...time.Time) time.Time {
	//convert all to unix time
	var sum int64 = 0
	var count int64 = 0
	for _, t := range times {
		sum += t.Unix()
		count += 1
	}
	avg := sum / count
	return time.Unix(avg, 0)
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
