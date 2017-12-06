package models

import (
	"sort"
	"fmt"
	"time"
	"strings"
	"github.com/pkg/errors"
	"regexp"
	"strconv"
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
	PanelId       int       `json:"panel_id"`
	Range         TimeRange `json:"range"`
	Interval      int       `json:"intervalMs"`
	Targets       []GrafanaTargets `json:"targets"`
	MaxDataPoints int       `json:"maxDataPoints"`
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
	RefId  string `json:"refId"`
}

// Query target params
type GrafanaTargetParam struct {
	ServiceId []int `json:"service_id"`
	EventGroup []int `json:"event_group"`
	EventBase []int `json:"event_base"`
	Sort string `json:"sort"`
	Limit int `json:"limit"`
}

// Since grafana sends a special data format, we need a custom
// JSON unmarshal function
//
// ex: "service_id=(1|2)&event_group=(test)&limit=5"
func (t *GrafanaTargetParam) UnmarshalJSON(b []byte) error {
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
		case "service_id":
			if i, err := toInt(split); err != nil {
				return err
			} else {
				t.ServiceId = i
			}
		case "event_group":
			if i, err := toInt(split); err != nil {
				return err
			} else {
				t.EventBase = i
			}
		case "event_base":
			if i, err := toInt(split); err != nil {
				return err
			} else {
				t.EventBase = i
			}
		case "sort":
			t.Sort = split[0]
		case "limit":
			if i, err := toInt(split); err != nil {
				return err
			} else {
				t.Limit = i[0]
			}
		default:
			return errors.New(fmt.Sprintf("No param named %v", param))
		}
	}
	return nil
}

func toInt(arr []string) ([]int, error) {
	ints := []int{}
	for _, elem := range arr {
		if i, err := strconv.Atoi(elem); err != nil {
			return ints, err
		} else {
			ints = append(ints, i)
		}
	}
	return ints, nil
}

// Represents a bin in a histogram
type Bin struct {
	Count int `json:"count"`
	Start int `json:"start"`
}

// Represents a map of bins, where the key is the start time
type EventBins map[int]*Bin

// return the bins sorted in chronological order
func (e EventBins) ToSlice() []Bin {
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

// sort from greatest to smallest
func (e EventResults) Less(i, j int) bool {
	return e[i].TotalCount > e[j].TotalCount
}

// Base event with histogram of occurrences
type EventResult struct {
	Id            int64       `json:"id"`
	EventType     string      `json:"event_type"`
	EventName     string      `json:"event_name"`
	TotalCount    int         `json:"total_count"`
	ProcessedData EventData   `json:"processed_data"`
	InstanceIds   []int64     `json:"instance_ids"`
	Datapoints    EventBins   `json:"datapoints"`
}

// returns formatted name of event
func (e EventResult) FormatName() string {
	return fmt.Sprintf("%v: %v", e.EventName, e.ProcessedData.Message)
}


