package eventsum

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/ContextLogic/eventsum/log"
	"github.com/ContextLogic/eventsum/metrics"
	. "github.com/ContextLogic/eventsum/models"
	"github.com/ContextLogic/eventsum/util"
	"github.com/julienschmidt/httprouter"
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
	sr.Header().Set("Content-Type", "application/json")
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

func (h *httpHandler) searchEventsHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	var err error
	query := r.URL.Query()
	serviceId := 1
	endTime := time.Now()
	startTime := endTime.Add(-1 * time.Hour)
	serviceIdMap := make(map[int]bool)
	baseIdMap := make(map[int]bool)
	groupIdMap := make(map[int]bool)
	envIdMap := make(map[int]bool)
	keywords := ""
	sort := ""

	if str := query.Get("service_id"); str != "" {
		id, err := strconv.Atoi(str)
		if err != nil {
			h.sendError(w, http.StatusBadRequest, errors.New("service ID is not an int"), "Error")
			return
		}
		serviceId = id
	}
	serviceIdMap[serviceId] = true

	if str := query.Get("end_time"); str != "" {
		endTime, err = time.Parse(h.timeFormat, str)
		if err != nil {
			h.sendError(w, http.StatusBadRequest, err, fmt.Sprintf("Ensure end time is in correct format: %v", h.timeFormat))
			return
		}
	}

	if str := query.Get("hash"); str != "" {
		if base, err := h.es.GetEventByHash(str); err != nil {
			h.sendError(w, http.StatusBadRequest, err, "")
		} else {
			baseIdMap[base.Id] = true
		}
	}

	if str := query.Get("group_id"); str != "" {
		if id, err := strconv.Atoi(str); err != nil {
			h.sendError(w, http.StatusBadRequest, err, "group_id must be an int")
		} else {
			groupIdMap[id] = true
		}
	}

	if str := query.Get("env_id"); str != "" {
		if id, err := strconv.Atoi(str); err != nil {
			h.sendError(w, http.StatusBadRequest, err, "env_id must be an int")
		} else {
			envIdMap[id] = true
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

	if str := query.Get("keywords"); str != "" {
		keywords = str
	}

	if str := query.Get("sort"); str != "" {
		sort = str
	}

	response, err := h.es.GeneralQuery(startTime, endTime, groupIdMap, baseIdMap, serviceIdMap, envIdMap)

	if keywords != "" {
		response = response.FilterBy(keywords)
	}

	if sort == "recent" {
		response = response.SortRecent()
	} else if sort == "increased" {
		response = response.SortIncreased()
	}

	if len(response) > limit {
		response = response[:limit]
	}

	if err != nil {
		h.sendError(w, http.StatusInternalServerError, err, "Cannot query event periods")
		return
	}
	h.sendResp(w, "events", response)
}

func (h *httpHandler) detailsEventsHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	query := r.URL.Query()
	eventId, err := strconv.ParseInt(query.Get("event_id"), 10, 64)
	if err != nil {
		h.sendError(w, http.StatusBadRequest, errors.New("event ID is missing or could not be parsed"), "Error")
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

	evtId, err := strconv.Atoi(query.Get("evt_id"))
	if err != nil {
		h.sendError(w, http.StatusBadRequest, errors.New("event base ID is missing or not an int"), "Error")
		return
	}

	from := query.Get("start_time")
	startTime, err := util.EpochToTime2(from)
	if err != nil {
		h.sendError(w, http.StatusBadRequest, errors.New("from is missing or not an epoch time"), "Error")
		return
	}

	to := query.Get("end_time")
	endTime, err := util.EpochToTime2(to)
	if err != nil {
		h.sendError(w, http.StatusBadRequest, errors.New("to is missing or not an epoch time"), "Error")
		return
	}

	response, err := h.es.ds.Test(startTime, endTime, evtId)
	h.sendResp(w, "histogram", response)
}

func (h *httpHandler) assignGroupHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	var evts []UnaddedEventGroup
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&evts); err != nil {
		h.sendError(w, http.StatusBadRequest, err, "Error decoding JSON event")
		return
	}
	for _, evt := range evts {
		if _, err := h.es.SetGroupId(evt.EventId, evt.GroupId); err != nil {
			h.sendError(w, http.StatusInternalServerError, err, "Error setting group id")
			return
		}
	}
}

func (h *httpHandler) createGroupHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	var groups []EventGroup
	if err := decoder.Decode(&groups); err != nil {
		h.sendError(w, http.StatusBadRequest, err, "Error decoding JSON group")
		return
	}
	for _, group := range groups {
		if _, err := h.es.AddEventGroup(group); err != nil {
			h.sendError(w, http.StatusInternalServerError, err, "Error creating a group")
			return
		}
	}
}

func (h *httpHandler) captureEventsHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	var evt UnaddedEvent
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&evt); err != nil {
		h.sendError(w, http.StatusBadRequest, err, "Error decoding JSON event")
		return
	}

	util.ProcessEventRawMessage(&evt)

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

func (h *httpHandler) searchGroupHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	defer r.Body.Close()
	query := r.URL.Query()
	id := query.Get("id")
	if id == "" {
		id = "0"
	}
	idInt, err := strconv.Atoi(id)
	if err != nil {
		h.sendError(w, http.StatusBadRequest, err, "Group id must be a valid int")
	}
	name := query.Get("name")

	var evts []EventBase

	if evts, err = h.es.GetEventsByGroup(idInt, name); err != nil {
		h.sendError(w, http.StatusBadRequest, err, "Cannot get events by group")
	}

	h.sendResp(w, "events", evts)

}

func (h *httpHandler) modifyGroupHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	defer r.Body.Close()
	var groups []map[string]string
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&groups); err != nil {
		h.sendError(w, http.StatusBadRequest, err, "Error decoding JSON event")
		return
	}
	for _, g := range groups {
		if err := h.es.ModifyEventGroup(g["name"], g["info"], g["new_name"]); err != nil {
			h.sendError(w, http.StatusInternalServerError, err, "Error setting group id")
			return
		}
	}
}

func (h *httpHandler) deleteGroupHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	defer r.Body.Close()
	query := r.URL.Query()
	id := query.Get("id")
	if id == "" {
		id = "-1"
	}
	idInt, err := strconv.Atoi(id)
	if err != nil {
		h.sendError(w, http.StatusBadRequest, err, "Group id must be a valid int")
	}
	name := query.Get("name")

	if err := h.es.DeleteEventGroup(idInt, name); err != nil {
		h.sendError(w, http.StatusBadRequest, err, "Error deleting groups")
		return
	}

}

func (h *httpHandler) countEventsHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	defer r.Body.Close()
	query := r.URL.Query()

	filter := make(map[string]string)

	id := query.Get("event_instance_id")
	if id == "" {
		h.sendError(w, http.StatusBadRequest, errors.New("event instance ID is missing"), "Error")
		return
	}
	_, err := strconv.Atoi(id)
	if err != nil {
		h.sendError(w, http.StatusBadRequest, errors.New("event instance ID is not an int"), "Error")
		return
	}
	filter["id"] = id
	filter["start_time"] = query.Get("start_time")
	filter["end_time"] = query.Get("end_time")

	if count, err := h.es.CountEvents(filter); err != nil {
		h.sendError(w, http.StatusBadRequest, err, "Error counting events")
		return
	} else {
		h.sendResp(w, "count", count)
	}

}

func (h *httpHandler) opsdbEventsHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	defer r.Body.Close()
	query := r.URL.Query()

	service := query.Get("service")
	if service == "" {
		h.sendError(w, http.StatusBadRequest, errors.New("service is missing"), "Error")
		return
	}
	if _, ok := h.es.ds.GetServicesMap()[service]; !ok {
		h.sendError(w, http.StatusBadRequest, errors.New("wrong service query"), "Error")
		return
	}
	env := query.Get("environment")
	if env == "" {
		h.sendError(w, http.StatusBadRequest, errors.New("environment is missing"), "Error")
		return
	}
	if _, ok := h.es.ds.GetEnvironmentsMap()[env]; !ok {
		h.sendError(w, http.StatusBadRequest, errors.New("wrong environment query"), "Error")
		return
	}

	group := query.Get("group")
	if group == "" {
		h.sendError(w, http.StatusBadRequest, errors.New("group is missing"), "Error")
		return
	}
	groupId, err := h.es.ds.GetGroupIdByGroupName(group)
	if err != nil {
		h.sendError(w, http.StatusBadRequest, errors.New("wrong group query"), "Error")
		return
	}

	region := query.Get("region")
	regionID := 1
	if region != "" {
		regionID, _ = h.es.ds.GetRegionsMap()[region]
	}

	serviceString := strconv.Itoa(h.es.ds.GetServicesMap()[service].Id)
	envString := strconv.Itoa(h.es.ds.GetEnvironmentsMap()[env].Id)
	groupString := groupId
	from := query.Get("start_time")
	to := query.Get("end_time")

	start, _ := util.EpochToTime2(from)
	end, _ := util.EpochToTime2(to)

	if res, err := h.es.OpsdbQuery(start, end, envString, serviceString, groupString, regionID); err != nil {
		h.sendError(w, http.StatusBadRequest, err, "Error counting events")
		return
	} else {
		h.sendResp(w, "opsdbQuery", res)
	}

}

func (h *httpHandler) test(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	fmt.Println(r.Header)
}

func (h *httpHandler) envTypesHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	h.sendResp(w, "envs", h.es.EnvsQuery())
}

func (h *httpHandler) tierTypesHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	h.sendResp(w, "tiers", h.es.TiersQuery())
}

func (h *httpHandler) groupTypesHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	groups, err := h.es.GroupsQuery()
	if err != nil {
		h.sendError(w, http.StatusBadRequest, err, "Error getting all group types")
	} else {
		h.sendResp(w, "groups", groups)
	}
}

func (h *httpHandler) cpuAlertHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	type Switch struct {
		S string `json:"s"`
	}
	var s Switch
	err := decoder.Decode(&s)
	if err != nil {
		h.log.App().Error("Failed parse cpu alert body")
		return
	}
	if s.S == "on" {
		h.log.App().Info("DB CPU alert triggered, start dropping to disk")
		h.es.dropToDisk.TurnOn()
	} else if s.S == "off" {
		h.log.App().Info("DB CPU alert resolved, stop dropping to disk")
		h.es.dropToDisk.TurnOff()
	} else {
		h.log.App().Info("the switch should either be turned on or off.")
	}
}

func (h *httpHandler) diskAlertHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	type Switch struct {
		S int
	}
	var s Switch
	err := decoder.Decode(&s)
	if err != nil {
		h.log.App().Error("Failed parse disk alert body")
		return
	}
	if s.S < 0 || s.S > 100 {
		h.log.App().Error("throttling value should be in range of 0 to 100")
	} else {
		h.log.App().Infof("throttle the write traffic to %d%%", s.S)
		h.es.dropEvent.Throttle(s.S)
	}
}

func (h *httpHandler) recentExceptionHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	//defer r.Body().Close()
	//query := r.URL.Query()
	//
	//service := query.Get("service")
}
