package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"
	"github.com/julienschmidt/httprouter"
)

// Base class for handling HTTP Requests
type httpHandler struct {
	es  *EventStore
	log *log.Logger
}

type EventIncreasedResult struct {
	Id int `json:"id"`
	EventType string `json:"event_type"`
	EventName string `json:"event_name"`
	Count int `json:"count"`
	LastUpdated time.Time `json:"last_updated"`
	ProcessedData interface{} `json:"processed_data"`
}

type EventHistogramResult struct {
	StartTime time.Time `json:"start_time"`
	EndTime time.Time `json:"end_time"`
	Count int `json:"count"`
	CounterJson map[string]int `json:"count_json"`
}

type EventDetailsResult struct {
	EventType   string                 `json:"event_type"`
	EventName string                 `json:"event_name"`
	RawData  interface{}              `json:"raw_data"`
	RawDetails        interface{}               `json:"raw_details"`
}

// Writes an error to ResponseWriter
func (h *httpHandler) sendError(w http.ResponseWriter, code int, err error, message string) {
	errMsg := fmt.Sprintf("%s: %s", message, err.Error())
	h.log.Println(errMsg)
	w.WriteHeader(code)
	w.Write([]byte(errMsg))
}

func (h *httpHandler) sendResp(w http.ResponseWriter, key string, val interface{}) {
	var response []byte
	if key == "" {
		response, _ = json.Marshal(val)
	} else {
		resp := make(map[string]interface{})
		resp[key] = val
		var err error
		response, err = json.Marshal(resp)
		if err != nil {
			h.sendError(w, http.StatusInternalServerError, err, "Error marshalling response to JSON")
			return
		}
	}
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	w.Write(response)
}

func (h *httpHandler) recentEventsHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

}

func (h *httpHandler) detailsEventsHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	query := r.URL.Query()
	eventId, err := strconv.Atoi(query.Get("event_id"))
	if err != nil {
		h.sendError(w, http.StatusBadRequest, errors.New("Event ID is missing or not an int"), "Error")
		return
	}
	//eventDataId, err := strconv.Atoi(query.Get("event_data_id"))
	//if err != nil {
	//	h.sendError(w, http.StatusBadRequest, errors.New("Event Data ID is missing or not an int"), "Error")
	//	return
	//}

	instance, _ := h.es.ds.GetInstanceById(int(eventId))
	detail, _ := h.es.ds.GetDetailById(int(instance.EventDetailId))
	event, _ := h.es.ds.GetEventBaseById(int(instance.EventBaseId))

	// Process result
	response := EventDetailsResult{
		EventType:   event.EventType,
		EventName:     event.EventName,
		RawData:  instance.RawData,
		RawDetails:        detail.RawDetail,
	}
	h.sendResp(w, "event_details", response)
}

func (h *httpHandler) histogramEventsHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	query := r.URL.Query()
	serviceId, err := strconv.Atoi(query.Get("service_id"))
	if err != nil {
		h.sendError(w, http.StatusBadRequest, errors.New("Event ID is missing or not an int"), "Error")
		return
	}
	time_period, ok := query["time_period"]
	if !ok {
		h.sendError(w, http.StatusBadRequest, errors.New("time_period is missing or not an int"), "Error")
		return
	}
	if len(time_period) != 2 {
		h.sendError(w, http.StatusBadRequest, errors.New("Event ID is missing or not an int"), "Error")
		return
	}
	startTime, err := time.Parse(time.RFC3339, time_period[0])
	if len(time_period) != 2 {
		h.sendError(w, http.StatusBadRequest, err, "Ensure time format is in RFC339")
		return
	}
	endTime, err := time.Parse(time.RFC3339, time_period[1])
	if len(time_period) != 2 {
		h.sendError(w, http.StatusBadRequest, err, "Ensure time format is in RFC339")
		return
	}
	eventInstanceId, _ := strconv.Atoi(query.Get("event_instance_id"))
	hist, err := h.es.ds.GetEventPeriods(startTime, endTime, eventInstanceId)
	if err != nil {
		h.sendError(w, http.StatusInternalServerError, err, "Cannot query event periods")
		return
	}

}

func (h *httpHandler) captureEventsHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	var exc UnaddedEvent
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&exc); err != nil {
		h.sendError(w, http.StatusBadRequest, err, "Error decoding JSON event")
		return
	}
	// TODO: make sure we validate the unadded event

	// Send to batching channel
	h.es.Send(exc)
}
