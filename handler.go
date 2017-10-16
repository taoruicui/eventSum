package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"
)

// Base class for handling HTTP Requests
type httpHandler struct {
	es  *EventStore
	log *log.Logger
}

type EventDetailsResult struct {
	DateCreated time.Time              `json:"date_created"`
	DateUpdated time.Time              `json:"date_updated"`
	EventType   string                 `json:"event_type"`
	Message     string                 `json:"message"`
	Function    string                 `json:"function"`
	Path        string                 `json:"path"`
	Stacktrace  string                 `json:"stacktrace"`
	Data        string                 `json:"data"`
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

func (h *httpHandler) recentEventsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Invalid request type", 405)
	}
}

func (h *httpHandler) detailsEventsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Invalid request type", 405)
	}
	query := r.URL.Query()
	eventId, err := strconv.Atoi(query.Get("event_id"))
	if err != nil {
		h.sendError(w, http.StatusBadRequest, errors.New("Event ID is missing or not an int"), "Error")
		return
	}
	eventDataId, err := strconv.Atoi(query.Get("event_data_id"))
	if err != nil {
		h.sendError(w, http.StatusBadRequest, errors.New("Event Data ID is missing or not an int"), "Error")
		return
	}

	// Query the DB
	//var stack StackTrace
	//var args map[string]interface{}

	instance, _ := h.es.ds.GetInstanceById(int(eventId))
	detail, _ := h.es.ds.GetDetailById(int(eventDataId))
	event, _ := h.es.ds.GetEventBaseById(int(instance.EventBaseId))
	//json.Unmarshal([]byte(instance.RawData), &stack)
	//json.Unmarshal([]byte(detail.RawDetail), &args)

	// Process result
	response := EventDetailsResult{
		DateCreated: time.Now(),
		EventType:   event.EventType,
		Message:     event.EventName,
		Function:    "",
		Path:        "",
		Stacktrace:  ToJson(instance.RawData),
		Data:        ToJson(detail.RawDetail),
	}
	h.sendResp(w, "event_details", response)
}

func (h *httpHandler) histogramEventsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Invalid request type", 405)
	}
}

func (h *httpHandler) captureEventsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Invalid request type", 405)
	}

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
