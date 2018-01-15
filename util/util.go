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

func DBHealthCheck(host string, port int, user string, password string, dbname string) error {
	psql := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	db, err := sql.Open("postgres", psql)
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
