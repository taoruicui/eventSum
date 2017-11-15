package eventsum

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/julienschmidt/httprouter"
	. "github.com/ContextLogic/eventsum/models"
	"github.com/ContextLogic/eventsum/log"
	"github.com/ContextLogic/eventsum/metrics"
	"net/http"
	"strconv"
	"time"
	"github.com/ContextLogic/eventsum/util"
)

// Base class for handling HTTP Requests
type httpHandler struct {
	es  *eventStore
	log *log.Logger
}

// statusRecorder is a simple http status recorder
type statusRecorder struct {
	http.ResponseWriter

	status int
}

// NewStatusRecorder returns an initialized statusRecorder, with 200 as the
// default status.
func newStatusRecorder(w http.ResponseWriter) *statusRecorder {
	return &statusRecorder{ResponseWriter: w, status: http.StatusOK}
}

// Status returns the cached http status value.
func (sr *statusRecorder) Status() int {
	return sr.status
}

// WriteHeader caches the status, then calls the underlying ResponseWriter.
func (sr *statusRecorder) WriteHeader(status int) {
	sr.status = status
	sr.ResponseWriter.WriteHeader(status)
}

// http wrapper to track latency by method calls
func latency(prefix string, h httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
		start := time.Now()
		defer func() {
			metrics.HTTPLatency(prefix, start)
		}()
		
		lw := newStatusRecorder(w)
		h(lw, req, ps)
		metrics.HTTPStatus(prefix, lw.Status())
	}
}

// Writes an error to ResponseWriter
func (h *httpHandler) sendError(w http.ResponseWriter, code int, err error, message string) {
	errMsg := fmt.Sprintf("%s: %s", message, err.Error())
	h.log.App.Info(errMsg)
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
	endTime := time.Now()
	startTime := endTime.Add(-1 * time.Hour)
	serviceId, err := strconv.Atoi(query.Get("service_id"))
	if err != nil {
		h.sendError(w, http.StatusBadRequest, errors.New("service ID is missing or not an int"), "Error")
		return
	}

	if str := query.Get("end_time"); str != "" {
		endTime, err = time.Parse(util.TimeFormat, str)
		if err != nil {
			h.sendError(w, http.StatusBadRequest, err, fmt.Sprintf("Ensure end time is in correct format: %v", util.TimeFormat))
			return
		}
	}

	if str := query.Get("start_time"); str != "" {
		startTime, err = time.Parse(util.TimeFormat, str)
		if err != nil {
			h.sendError(w, http.StatusBadRequest, err, fmt.Sprintf("Ensure start time is in correct format: %v", util.TimeFormat))
			return
		}
	}

	limit, err := strconv.Atoi(query.Get("limit"))
	if err != nil {
		limit = 100
	}

	response, err := h.es.GetRecentEvents(startTime, endTime, serviceId, limit)
	if err != nil {
		h.sendError(w, http.StatusInternalServerError, err, "Cannot query event periods")
		return
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

	response, err := h.es.GetEventDetailsbyId(int(eventId))
	if err != nil {
		h.sendError(w, http.StatusInternalServerError, err, "Could not get event details")
		return
	}
	h.sendResp(w, "event_details", response)
}

func (h *httpHandler) histogramEventsHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	query := r.URL.Query()
	endTime := time.Now()
	startTime := endTime.Add(-1 * time.Hour)
	eventInstanceId, err := strconv.Atoi(query.Get("event_instance_id"))
	if err != nil {
		h.sendError(w, http.StatusBadRequest, errors.New("event instance ID is missing or not an int"), "Error")
		return
	}

	if str := query.Get("end_time"); str != "" {
		endTime, err = time.Parse(util.TimeFormat, str)
		if err != nil {
			h.sendError(w, http.StatusBadRequest, err, fmt.Sprintf("Ensure end time is in correct format: %v", util.TimeFormat))
			return
		}
	}

	if str := query.Get("start_time"); str != "" {
		startTime, err = time.Parse(util.TimeFormat, str)
		if err != nil {
			h.sendError(w, http.StatusBadRequest, err, fmt.Sprintf("Ensure start time is in correct format: %v", util.TimeFormat))
			return
		}
	}

	response, err := h.es.GetEventHistogram(startTime, endTime, eventInstanceId)
	if err != nil {
		h.sendError(w, http.StatusInternalServerError, err, "Cannot query event periods")
		return
	}
	h.sendResp(w, "histogram", response)
}

func (h *httpHandler) captureEventsHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	var evt UnaddedEvent
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&evt); err != nil {
		h.sendError(w, http.StatusBadRequest, err, "Error decoding JSON event")
		return
	}
	// Validate the unadded event
	if _, err := time.Parse(util.TimeFormat, evt.Timestamp); err != nil {
		h.sendError(w, http.StatusBadRequest, err, fmt.Sprintf("Ensure timestamp is in correct format: '%v'", util.TimeFormat))
		return
	}

	if evt.Name == "" || evt.Type == "" {
		h.sendError(w, http.StatusBadRequest, errors.New("event_name and event_type cannot be empty"), "")
		return
	}

	// Send to batching channel
	h.es.Send(evt)
}
