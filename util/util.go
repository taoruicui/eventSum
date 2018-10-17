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

func EncodeToJsonRawMsg(data interface{}) []byte {
	jsonString, _ := json.Marshal(data)
	return jsonString
}

func ProcessEventRawMessage(evt *models.UnaddedEvent) {
	switch evt.Data.RawMessage.(type) {
	case string:
		evt.Data.Message = fmt.Sprintf("%s", evt.Data.RawMessage)
	case int:
		evt.Data.Message = fmt.Sprintf("%d", evt.Data.RawMessage)
	case float64:
		evt.Data.Message = fmt.Sprintf("%f", evt.Data.RawMessage)
	case map[string]interface{}:
		if m, err := json.Marshal(evt.Data.RawMessage); err != nil {
			evt.Data.Message = ""
		} else {
			evt.Data.Message = string(m)
		}
	default:
		evt.Data.Message = ""
	}
}

func ProcessGenericData(event *models.UnaddedEvent) error {
	event.Data.Message = ""
	event.Data.RawMessage = nil
	return nil
}

func CompileDataPoints(start string, end string, record models.OpsdbResult, tInterval int) ([][]int, error) {

	var res [][]int

	startTime, err := time.Parse("2006-01-02 15:04:05", start)
	if err != nil {
		return res, err
	}

	//pad zero at head if needed
	head, err := time.Parse("2006-01-02 15:04:05", record.TimeStamp[0])
	if err != nil {
		return res, err
	}
	if head.Add(7*time.Hour).Sub(startTime) > time.Duration(tInterval)*time.Minute {
		res = append(res, []int{0, int(head.Add(-1*(time.Duration(tInterval))*time.Minute).Unix() * 1000)})
	}

	// pad zeros between datapoints
	if len(record.TimeStamp) > 1 {
		for i := range record.TimeStamp[:len(record.TimeStamp)-1] {
			prev, err := time.Parse("2006-01-02 15:04:05", record.TimeStamp[i])
			if err != nil {
				return res, err
			}
			next, err := time.Parse("2006-01-02 15:04:05", record.TimeStamp[i+1])
			if err != nil {
				return res, err
			}
			res = append(res, []int{record.Count[i], int(prev.Unix() * 1000)})
			if next.Sub(prev).Seconds() > float64(tInterval*60*2) {
				res = append(res, []int{0, int(prev.Add((time.Duration(tInterval))*time.Minute).Unix() * 1000)})
				res = append(res, []int{0, int(next.Add(-1*(time.Duration(tInterval))*time.Minute).Unix() * 1000)})

			} else if next.Sub(prev).Seconds() > float64(tInterval*60) && next.Sub(prev).Seconds() <= float64(tInterval*60*2) {
				res = append(res, []int{0, int((prev.Unix() + next.Unix()) / 2 * 1000)})
			}
		}
	}

	//add last datapoint
	lastTimeStamp, err := time.Parse("2006-01-02 15:04:05", record.TimeStamp[len(record.TimeStamp)-1])
	res = append(res, []int{record.Count[len(record.Count)-1], int(lastTimeStamp.Unix() * 1000)})

	endTime, err := time.Parse("2006-01-02 15:04:05", end)
	if err != nil {
		return res, err
	}

	//pad zero at tail if needed
	tail, err := time.Parse("2006-01-02 15:04:05", record.TimeStamp[len(record.TimeStamp)-1])
	if err != nil {
		return res, err
	}
	if endTime.Sub(tail.Add(7*time.Hour)) > time.Duration(tInterval)*time.Minute {
		res = append(res, []int{0, int(tail.Add((time.Duration(tInterval))*time.Minute).Unix() * 1000)})
	}

	for i := range res {
		res[i][1] = res[i][1] + 7*60*60*1000
	}

	//startUnix := startTime.Unix() * 1000
	//endUnix := endTime.Unix() * 1000
	//
	//var interval int64
	//var length int
	//
	//if endUnix-startUnix < int64(200*tInterval*60*1000) {
	//	// means the minimum interval would be less than minimum interval we set in the config.
	//	// we should try to keep the interval as the pre-set one.
	//
	//	interval = int64(tInterval * 60 * 1000)
	//	length = int((endUnix - startUnix) / interval)
	//
	//} else {
	//	// increase the time interval to keep the datapoints ~200 per target.
	//
	//	interval = (endUnix - startUnix) / 200
	//	length = 200
	//}
	//
	//var datapoints = make([][]int, length)
	//for i := range datapoints {
	//	datapoints[i] = make([]int, 2)
	//}
	//
	//starting := int(startUnix + interval/2)
	//for i := range datapoints {
	//	datapoints[i][1] = starting
	//	starting += int(interval)
	//}
	//
	//for i, tStr := range record.TimeStamp {
	//	t, _ := time.Parse("2006-01-02 15:04:05", tStr)
	//	tUnix := t.Add(7*time.Duration(time.Hour)).Unix() * 1000
	//	index := int((tUnix - startUnix) / interval)
	//	if index < 0 || index > 199 {
	//		continue
	//	}
	//	datapoints[index][0] += record.Count[i]
	//}
	//
	return res, nil
}
