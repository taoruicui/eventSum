package eventsum

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jacksontj/dataman/src/datamantype"
	"github.com/julienschmidt/httprouter"
	log "github.com/ContextLogic/eventsum/log"
	"net/http"
	"strconv"
	"time"
)

// Base class for handling HTTP Requests
type httpHandler struct {
	es  *eventStore
	log *log.Logger
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
		endTime, err = time.Parse(datamantype.DateTimeFormatStr, str)
		if err != nil {
			h.sendError(w, http.StatusBadRequest, err, fmt.Sprintf("Ensure end time is in correct format: %v", datamantype.DateTimeFormatStr))
			return
		}
	}

	if str := query.Get("start_time"); str != "" {
		startTime, err = time.Parse(datamantype.DateTimeFormatStr, str)
		if err != nil {
			h.sendError(w, http.StatusBadRequest, err, fmt.Sprintf("Ensure start time is in correct format: %v", datamantype.DateTimeFormatStr))
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
		endTime, err = time.Parse(datamantype.DateTimeFormatStr, str)
		if err != nil {
			h.sendError(w, http.StatusBadRequest, err, fmt.Sprintf("Ensure end time is in correct format: %v", datamantype.DateTimeFormatStr))
			return
		}
	}

	if str := query.Get("start_time"); str != "" {
		startTime, err = time.Parse(datamantype.DateTimeFormatStr, str)
		if err != nil {
			h.sendError(w, http.StatusBadRequest, err, fmt.Sprintf("Ensure start time is in correct format: %v", datamantype.DateTimeFormatStr))
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
