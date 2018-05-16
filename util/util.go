package util

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"runtime/pprof"
	"time"

	"database/sql"

	"bytes"

	"strings"

	"strconv"

	"github.com/ContextLogic/eventsum/models"
	"github.com/jacksontj/dataman/src/datamantype"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
)

// returns the start and end times of the interval bounding time t,
// interval specific as minutes
func FindBoundingTime(t time.Time, interval int) (time.Time, time.Time) {
	duration := time.Duration(interval) * time.Minute
	s := t.Truncate(duration)
	return s, s.Add(duration)
}

type ByTime []map[string]interface{}

func (t ByTime) Len() int {
	return len(t)
}

func (t ByTime) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func (t ByTime) Less(i, j int) bool {
	iEndTime := t[i]["end_time"].(string)
	jEndTime := t[j]["end_time"].(string)
	layout := "2006-01-02 04:05:00"
	iTime, _ := time.Parse(layout, iEndTime)
	jTime, _ := time.Parse(layout, jEndTime)
	return iTime.After(jTime)
}

// Calculates the average
func Avg(vals ...int) int {
	//convert all to unix time
	var sum int = 0
	var count int = 0
	for _, t := range vals {
		sum += t
		count += 1
	}
	avg := sum / count
	return avg
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

// MapDecode uses mapstructure to decode a source to a target struct.
// If zero is set to true, then target will be zeroed before writing.
func MapDecode(source, target interface{}, zero bool) error {
	config := mapstructure.DecoderConfig{
		DecodeHook: stringToDateTimeHook,
		Result:     target,
		ZeroFields: zero,
	}
	decoder, err := mapstructure.NewDecoder(&config)
	if err != nil {
		return err
	}
	return decoder.Decode(source)
}

func DBHealthCheck(pgString string) error {
	db, err := sql.Open("postgres", pgString)
	defer db.Close()
	if err != nil {
		return err
	}
	err = db.Ping()
	return err
}

func ServiceHealthCheck() error {
	dump := bytes.NewBufferString("")
	pprof.Lookup("goroutine").WriteTo(dump, 1)
	if !strings.Contains(dump.String(), "eventsum/server.go") {
		return errors.New("go routine: eventsumServer abnormal")
	}
	if !strings.Contains(dump.String(), "eventsum/event_store") {
		return errors.New("go routine: event_store abnormal")
	}
	if !strings.Contains(dump.String(), "eventsum/log/logger") {
		return errors.New("go routine: logger abnormal")
	}
	return nil
}

func GetExptPerMinIncrease(mostRecent map[string]interface{}, secondRecent map[string]interface{}) float64 {
	layout := "2006-01-02 15:04:05"
	mostRecentStart, _ := time.Parse(layout, mostRecent["start_time"].(string))
	mostRecentEnd, _ := time.Parse(layout, mostRecent["end_time"].(string))
	duration := mostRecentEnd.Sub(mostRecentStart).Minutes()
	mostRecentIncre := float64(mostRecent["count"].(int64)) / duration
	if secondRecent == nil {
		return mostRecentIncre
	} else {
		secondRecentStart, _ := time.Parse(layout, secondRecent["start_time"].(string))
		secondRecentEnd, _ := time.Parse(layout, secondRecent["end_time"].(string))
		duration = secondRecentEnd.Sub(secondRecentStart).Minutes()
		secondRecentIncre := float64(secondRecent["count"].(int64)) / duration
		return mostRecentIncre - secondRecentIncre
	}
}

func IsInList(intls []int, intv int, stringls []string, stringv string) bool {
	if intls != nil {
		for _, l := range intls {
			if l == intv {
				return true
			}
		}
		return false
	} else {
		for _, l := range stringls {
			if l == stringv {
				return true
			}
		}
		return false
	}
}

func GroupNameMapToId(name string, groups []models.EventGroup) int {
	for _, g := range groups {
		if name == g.Name {
			return g.Id
		}
	}
	return -1
}

func EpochToTime(epoch string) (time.Time, error) {
	t, err := strconv.ParseInt(epoch, 10, 64)
	if err != nil {
		return time.Now(), err
	}
	tm := time.Unix(t, 0)
	return tm.UTC(), nil
}

func EpochToTime2(epoch string) (string, error) {
	t, err := strconv.ParseInt(epoch, 10, 64)
	if err != nil {
		t2, err := strconv.ParseFloat(epoch, 64)
		if err != nil {
			return time.Now().Format("2006-01-02 15:04:05"), err
		}
		tm := time.Unix(int64(t2), 0)
		return tm.UTC().Format("2006-01-02 15:04:05"), nil
	}
	tm := time.Unix(t, 0)
	return tm.UTC().Format("2006-01-02 15:04:05"), nil
}
