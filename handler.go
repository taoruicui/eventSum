package eventsum

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
	es  *eventStore
	log *log.Logger
}

type eventRecentResult struct {
	Id int `json:"id"`
	EventType string `json:"event_type"`
	EventName string `json:"event_name"`
	Count int `json:"count"`
	LastUpdated time.Time `json:"last_updated"`
	ProcessedData interface{} `json:"processed_data"`
}

type eventHistogramResult struct {
	StartTime time.Time `json:"start_time"`
	EndTime time.Time `json:"end_time"`
	Count int `json:"count"`
	CounterJson map[string]interface{} `json:"count_json"`
}

type eventDetailsResult struct {
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
	query := r.URL.Query()
	serviceId, err := strconv.Atoi(query.Get("service_id"))
	if err != nil {
		h.sendError(w, http.StatusBadRequest, errors.New("service ID is missing or not an int"), "Error")
		return
	}

	endTime, err := time.Parse("2006-01-02 15:04:05", query.Get("end_time"))
	if err != nil {
		h.log.Printf("Error decoding end time, using default: %v", err)
		endTime = time.Now()
	}

	startTime, err := time.Parse("2006-01-02 15:04:05", query.Get("start_time"))
	if err != nil {
		h.log.Printf("Error decoding start time, using default: %v", err)
		startTime = endTime.Add(-1 * time.Hour)
	}

	limit, err := strconv.Atoi(query.Get("limit"))
	if err != nil {
		limit = 100
	}

	events, err := h.es.ds.GetRecentEvents(startTime, endTime, serviceId, limit)
	if err != nil {
		h.sendError(w, http.StatusInternalServerError, err, "Cannot query event periods")
		return
	}
	var response []eventRecentResult
	for _, e := range events {
		response = append(response, eventRecentResult{
			Id: int(e.Id),
			EventType: e.EventType,
			EventName: e.EventName,
			ProcessedData: e.ProcessedData,
		})
	}
	h.sendResp(w, "recent_events", response)
}

func (h *httpHandler) detailsEventsHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	query := r.URL.Query()
	eventId, err := strconv.Atoi(query.Get("event_id"))
	if err != nil {
		h.sendError(w, http.StatusBadRequest, errors.New("event ID is missing or not an int"), "Error")
		return
	}

	instance, _ := h.es.ds.GetInstanceById(int(eventId))
	detail, _ := h.es.ds.GetDetailById(int(instance.EventDetailId))
	event, _ := h.es.ds.GetEventBaseById(int(instance.EventBaseId))

	// Process result
	response := eventDetailsResult{
		EventType:   event.EventType,
		EventName:     event.EventName,
		RawData:  instance.RawData,
		RawDetails:        detail.RawDetail,
	}
	h.sendResp(w, "event_details", response)
}

func (h *httpHandler) histogramEventsHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	query := r.URL.Query()
	eventInstanceId, err := strconv.Atoi(query.Get("event_instance_id"))
	if err != nil {
		h.sendError(w, http.StatusBadRequest, errors.New("event instance ID is missing or not an int"), "Error")
		return
	}

	endTime, err := time.Parse("2006-01-02 15:04:05", query.Get("end_time"))
	if err != nil {
		h.log.Printf("Error decoding end time, using default: %v", err)
		endTime = time.Now()
	}

	startTime, err := time.Parse("2006-01-02 15:04:05", query.Get("start_time"))
	if err != nil {
		h.log.Printf("Error decoding start time, using default: %v", err)
		startTime = endTime.Add(-1 * time.Hour)
	}

	hist, err := h.es.ds.GetEventPeriods(startTime, endTime, eventInstanceId)
	if err != nil {
		h.sendError(w, http.StatusInternalServerError, err, "Cannot query event periods")
		return
	}
	var response []eventHistogramResult
	for _, h := range hist {
		response = append(response, eventHistogramResult{
			StartTime: h.StartTime,
			EndTime: h.EndTime,
			Count: h.Count,
			CounterJson: h.CounterJson,
		})
	}
	h.sendResp(w, "histogram", response)
}

func (h *httpHandler) captureEventsHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	var exc unaddedEvent
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
