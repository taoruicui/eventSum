package eventsum

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ContextLogic/eventsum/log"
	"github.com/ContextLogic/eventsum/metrics"
	. "github.com/ContextLogic/eventsum/models"
	"github.com/julienschmidt/httprouter"
	"net/http"
	"strconv"
	"time"
)

// Base class for handling HTTP Requests
type httpHandler struct {
	es  *eventStore
	log *log.Logger

	timeFormat string
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

// Initializes a new httpHandler given configs (currently timeFormat)
func newHTTPHandler(es *eventStore, logger *log.Logger, timeFormat string) httpHandler {
	return httpHandler{
		es:         es,
		log:        logger,
		timeFormat: timeFormat,
	}
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
	h.log.App().Info(errMsg)
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
		endTime, err = time.Parse(h.timeFormat, str)
		if err != nil {
			h.sendError(w, http.StatusBadRequest, err, fmt.Sprintf("Ensure end time is in correct format: %v", h.timeFormat))
			return
		}
	}

	if str := query.Get("start_time"); str != "" {
		startTime, err = time.Parse(h.timeFormat, str)
		if err != nil {
			h.sendError(w, http.StatusBadRequest, err, fmt.Sprintf("Ensure start time is in correct format: %v", h.timeFormat))
			return
		}
	}

	limit, err := strconv.Atoi(query.Get("limit"))
	if err != nil {
		limit = 100
	}

	serviceIdMap := map[int]bool{serviceId: true}
	response, err := h.es.GeneralQuery(startTime, endTime, map[int]bool{}, map[int]bool{}, serviceIdMap)
	response.SortRecent()

	if len(response) > limit {
		response = response[:limit]
	}

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
	eventBaseId, err := strconv.Atoi(query.Get("event_base_id"))
	if err != nil {
		h.sendError(w, http.StatusBadRequest, errors.New("event base ID is missing or not an int"), "Error")
		return
	}

	if str := query.Get("end_time"); str != "" {
		endTime, err = time.Parse(h.timeFormat, str)
		if err != nil {
			h.sendError(w, http.StatusBadRequest, err, fmt.Sprintf("Ensure end time is in correct format: %v", h.timeFormat))
			return
		}
	}

	if str := query.Get("start_time"); str != "" {
		startTime, err = time.Parse(h.timeFormat, str)
		if err != nil {
			h.sendError(w, http.StatusBadRequest, err, fmt.Sprintf("Ensure start time is in correct format: %v", h.timeFormat))
			return
		}
	}

	eventMap := map[int]bool{eventBaseId: true}
	response, err := h.es.GeneralQuery(startTime, endTime, map[int]bool{}, eventMap, map[int]bool{})
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
	if _, err := time.Parse(h.timeFormat, evt.Timestamp); err != nil {
		h.sendError(w, http.StatusBadRequest, err, fmt.Sprintf("Ensure timestamp is in correct format: '%v'", h.timeFormat))
		return
	}

	if evt.Name == "" || evt.Type == "" {
		h.sendError(w, http.StatusBadRequest, errors.New("event_name and event_type cannot be empty"), "")
		return
	}

	// Send to batching channel
	h.es.Send(evt)
}

func (h *httpHandler) healthCheck(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// TODO: Make this useful
	h.sendResp(w, "", "")
}
