package eventsum

import (
	"encoding/json"
	"fmt"
	"github.com/julienschmidt/httprouter"
	. "github.com/ContextLogic/eventsum/models"
	"net/http"
	"strconv"
	"sort"
)


// cors adds headers that Grafana requires to work as a direct access data
// source.
//
// forgetting to add these manifests itself as an unintellible error when
// adding a datasource.
//
// These are not required if using "proxy" access.
func cors(h httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		w.Header().Set("Access-Control-Allow-Headers", "accept, content-type")
		w.Header().Set("Access-Control-Allow-Methods", "POST")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		h(w, r, p)
	}
}

func (h *httpHandler) grafanaOk(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

}

// grafanaQuery handles all grafana metric requests. This means it handles 3 main
// request types:
//
// 1) Individual Event metrics
// 2) Top n recent events and their metrics
// 3) Top n new/increased events and their metrics
func (h *httpHandler) grafanaQuery(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	var query GrafanaQueryReq
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)

	if err := decoder.Decode(&query); err != nil {
		h.sendError(w, http.StatusBadRequest, err, "Incorrect request format")
	}

	result := []GrafanaQueryResp{}
	for _, target := range query.Targets {
		// create the maps that are needed later
		eventBaseMap :=  make(map[int]bool)
		eventGroupMap := make(map[int]bool)
		serviceIdMap := make(map[int]bool)

		for _, v := range target.Target.EventBase {
			eventBaseMap[v] = true
		}

		for _, v := range target.Target.EventGroup {
			eventGroupMap[v] = true
		}

		for _, v := range target.Target.ServiceId {
			serviceIdMap[v] = true
		}

		evts, err := h.es.GrafanaQuery(query.Range.From, query.Range.To, eventGroupMap, eventBaseMap, serviceIdMap)

		if err != nil {
			h.sendError(w, http.StatusInternalServerError, err, "query error")
			return
		}

		// Sort the events
		if target.Target.Sort == "recent" {
			sort.Sort(evts)
		} else if target.Target.Sort == "increased" {

		}

		if len(evts) > target.Target.Limit && target.Target.Limit > 0 {
			evts = evts[:target.Target.Limit]
		}

		for _, evt := range evts {
			datapoints := [][]int{}
			bins := evt.Datapoints.ToSlice()
			for _, bin := range bins {
				datapoints = append(datapoints, []int{bin.Count, bin.Start})
			}
			result = append(result, GrafanaQueryResp{
				Target:     evt.FormatName(),
				Datapoints: datapoints,
			})
		}
	}

	if err := json.NewEncoder(w).Encode(result); err != nil {
		fmt.Printf("json encode failure: %+v", err)
	}
}

// grafanaSearch handles the searching of service ids and event ids for grafana template variables
func (h *httpHandler) grafanaSearch(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	var search GrafanaSearchReq
	var result interface{}
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&search)

	if err != nil {
		h.sendError(w, http.StatusBadRequest, err, "Incorrect request format")
	}

	switch search.Target {
	// Get all service ids
	case "service_id":
		result, err = h.es.GetServiceIds()

		if err != nil {
			h.sendError(w, http.StatusInternalServerError, err, "eventstore error")
		}

	case "event_group":
		groups, err := h.es.GetAllGroups()
		ids := []int{}

		if err != nil {
			h.sendError(w, http.StatusInternalServerError, err, "eventstore error")
		}

		for _, group := range groups {
			ids = append(ids, int(group.Id))
		}
		result = ids

	// Get all event ids that correspond to service_id
	default:
		service_id, err := strconv.Atoi(search.Target)
		if err != nil {
			h.sendError(w, http.StatusBadRequest, err, "target not an int")
		}

		result, err = h.es.GetBaseIds(service_id)

		if err != nil {
			h.sendError(w, http.StatusInternalServerError, err, "eventstore error")
		}
	}

	if err := json.NewEncoder(w).Encode(result); err != nil {
		fmt.Printf("json encode failure: %+v", err)
	}
}
