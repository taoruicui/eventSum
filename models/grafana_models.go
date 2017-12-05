package models

import (
	"sort"
	"fmt"
	"time"
)

/////////////////////////////////////
/* MODELS CORRESPONDING TO GRAFANA */
/////////////////////////////////////

// request struct for grafana searches
type SearchReq struct {
	Target string `json:"target"`
}

// request struct for grafana queries
type QueryReq struct {
	PanelId       int       `json:"panel_id"`
	Range         TimeRange `json:"range"`
	Interval      int       `json:"intervalMs"`
	Targets       []Targets `json:"targets"`
	MaxDataPoints int       `json:"maxDataPoints"`
}

// Query response
type QueryResp struct {
	Target     string  `json:"target"`
	Datapoints [][]int `json:"datapoints"`
}

// Range specifies the time range the request is valid for.
type TimeRange struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

type Targets struct {
	Target string `json:"target"`
	RefId  string `json:"refId"`
}

type TargetParam struct {
	ServiceId []int `json:"service_id"`
	EventGroup []int `json:"event_group"`
	EventBase []int `json:"event_base"`
	Sort string `json:"sort"`
	Limit int `json:"limit"`
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


