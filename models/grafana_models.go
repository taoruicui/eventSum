package models

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

/////////////////////////////////////
/* MODELS CORRESPONDING TO GRAFANA */
/////////////////////////////////////

// request struct for grafana searches
type GrafanaSearchReq struct {
	Target string `json:"target"`
}

// request struct for grafana queries
type GrafanaQueryReq struct {
	PanelId       int              `json:"panel_id"`
	Range         TimeRange        `json:"range"`
	Interval      int              `json:"intervalMs"`
	Targets       []GrafanaTargets `json:"targets"`
	MaxDataPoints int              `json:"maxDataPoints"`
}

// Query response
type GrafanaQueryResp struct {
	Target     string  `json:"target"`
	Datapoints [][]int `json:"datapoints"`
}

// Range specifies the time range the request is valid for.
type TimeRange struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

type GrafanaTargets struct {
	Target GrafanaTargetParam `json:"target"`
	RefId  string             `json:"refId"`
}

// Query target params
type GrafanaTargetParam struct {
	ServiceName     []string `json:"service_name"`
	EnvironmentName []string `json:"environment_name"`
	GroupName       []string `json:"group_name"`
	EventBaseId     []int    `json:"event_base_id"`
	Sort            string   `json:"sort"`
	Limit           int      `json:"limit"`
	EventType       []string `json:"event_type"`
	EventName       []string `json:"event_name"`
}

// Since grafana sends a special data format, we need a custom
// JSON unmarshal function
//
// ex: "service_id=(1|2)&event_group=(test)&limit=5"
func (t *GrafanaTargetParam) UnmarshalJSON(b []byte) error {

	if string(b) == "\"\"" {
		return nil
	}

	s := strings.Trim(string(b), "\"")

	groups := strings.Split(s, "&")

	for _, group := range groups {
		arr := strings.Split(group, "=")

		if len(arr) < 2 {
			return errors.New("Delimiter missing: \"=\"")
		}

		param := arr[0]
		values := arr[1]

		// parse values, values passed in must be regexp, eg. (1|2)
		if _, err := regexp.Compile(values); err != nil {
			return err
		}

		values = strings.TrimLeft(values, "(")
		values = strings.TrimRight(values, ")")
		split := strings.Split(values, "|")

		// ensure param is valid
		switch param {
		case "service_name":
			t.ServiceName = split
		case "environment":
			t.EnvironmentName = split
		case "group_name":
			t.GroupName = split
		case "event_base_id":
			for _, elem := range split {
				if i, err := strconv.Atoi(elem); err != nil {
					return err
				} else {
					t.EventBaseId = append(t.EventBaseId, i)
				}
			}
		case "sort":
			t.Sort = split[0]
		case "limit":
			i, err := strconv.Atoi(split[0])
			if err != nil {
				return err
			}
			t.Limit = i
		case "event_name":
			t.EventName = split
		case "event_type":
			t.EventType = split
		default:
			return errors.New(fmt.Sprintf("No param named %v", param))
		}
	}
	return nil
}

// Represents a bin in a histogram
type Bin struct {
	Count int `json:"count"`
	Start int `json:"start"` // unix time in milliseconds
}

// Represents a map of bins, where the key is the start time
type EventBins map[int]*Bin

// return the bins sorted in chronological order. If the number
// of bins is above n, bins will be merge equally of fixed length.
func (e EventBins) ToSlice(n int) []Bin {
	if len(e) > n {
		res := make([]Bin, n)
		min := 0
		max := 0

		for k := range e {
			if k < min {
				min = k
			}
			if k > max {
				max = k
			}
		}
		l := float64(max - min)

		// group following the formula: floor((x-min)*n/l)
		for _, bin := range e {
			idx := int(math.Floor(float64(bin.Start-min) * float64(n) / l))
			if idx >= n {
				idx = n - 1
			}
			res[idx].Count += bin.Count
			res[idx].Start = bin.Start
		}
		return res
	} else {
		keys := []int{}
		res := []Bin{}
		for i := range e {
			keys = append(keys, i)
		}
		sort.Ints(keys)
		for _, k := range keys {
			res = append(res, *e[k])
		}
		return res
	}
}

// Recent events
type EventResults []EventResult

// used for sort function
func (e EventResults) Len() int {
	return len(e)
}

// used for sort function
func (e EventResults) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

// used for sort function
func (e EventResults) Less(i, j int) bool {
	return e[i].TotalCount > e[j].TotalCount
}

// sort from greatest to smallest
func (e EventResults) SortRecent() EventResults {
	sort.Sort(e)
	return e
}

// sort by events that have recently been increasing
func (e EventResults) SortIncreased() EventResults {
	evts := EventResults{}

	for _, evt := range e {
		if len(evt.Datapoints) <= 2 {
			continue
		}
		countBegin := 0
		countEnd := 0
		mid := len(evt.Datapoints) / 2
		// loop through all datapoints. Add to countBegin
		// if before, else add to countEnd
		for i, point := range evt.Datapoints {
			if i <= mid {
				countBegin += point.Count
			} else {
				countEnd += point.Count
			}
		}
		if countEnd > countBegin {
			evts = append(evts, evt)
		}
	}
	evts = evts.SortRecent()
	return evts
}

// Simple filtering for any events that contain str
func (e EventResults) FilterBy(str string) EventResults {
	evts := EventResults{}

	for _, evt := range e {
		if strings.Contains(evt.EventType, str) {
			evts = append(evts, evt)
		}

		if strings.Contains(evt.ProcessedData.Message, str) {
			evts = append(evts, evt)
		}
	}
	return evts
}

// Base event with histogram of occurrences
type EventResult struct {
	Id                 int       `json:"id"`
	EventType          string    `json:"event_type"`
	EventName          string    `json:"event_name"`
	EventGroupId       int       `json:"event_group_id"`
	EventEnvironmentId int       `json:"event_environment_id"`
	TotalCount         int       `json:"total_count"`
	ProcessedData      EventData `json:"processed_data"`
	InstanceIds        []int     `json:"instance_ids"`
	Datapoints         []Bin     `json:"datapoints"`
}

// returns formatted name of event
func (e EventResult) FormatName() string {
	return fmt.Sprintf("%v: %v", e.EventName, e.ProcessedData.Message)
}
