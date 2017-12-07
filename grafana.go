package eventsum

import (
	"encoding/json"
	"fmt"
	"github.com/julienschmidt/httprouter"
	. "github.com/ContextLogic/eventsum/models"
	"net/http"
	"strconv"
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
	// maps name to id
	serviceNameMap := make(map[string]int)
	groupNameMap := make(map[string]int)
	services, _ := h.es.GetAllServices()
	for _, service := range services {
		serviceNameMap[service.Name] = service.Id
	}
	groups, _ := h.es.GetAllGroups()
	for _, group := range groups {
		groupNameMap[group.Name] = group.Id
	}

	for _, target := range query.Targets {
		// create the maps that are needed later
		eventBaseMap :=  make(map[int]bool)
		groupIdMap := make(map[int]bool)
		serviceIdMap := make(map[int]bool)

		for _, v := range target.Target.EventBaseId {
			eventBaseMap[v] = true
		}

		for _, v := range target.Target.GroupName {
			groupIdMap[groupNameMap[v]] = true
		}

		for _, v := range target.Target.ServiceName {
			serviceIdMap[serviceNameMap[v]] = true
		}

		evts, err := h.es.GeneralQuery(query.Range.From, query.Range.To, groupIdMap, eventBaseMap, serviceIdMap)

		if err != nil {
			h.sendError(w, http.StatusInternalServerError, err, "query error")
			return
		}

		// Sort the events
		if target.Target.Sort == "recent" {
			evts = evts.SortRecent()
		} else if target.Target.Sort == "increased" {
			// TODO: implement increased
			mid := int(1000 * (query.Range.From.Unix() + query.Range.To.Unix()) / 2)
			evts = evts.SortIncreased(mid)
			evts = evts.SortRecent()
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
	case "service_name":
		services, err := h.es.GetAllServices()
		names := []string{}

		if err != nil {
			h.sendError(w, http.StatusInternalServerError, err, "eventstore error")
		}

		for _, service := range services {
			names = append(names, service.Name)
		}
		result = names

	case "group_name":
		groups, err := h.es.GetAllGroups()
		names := []string{}

		if err != nil {
			h.sendError(w, http.StatusInternalServerError, err, "eventstore error")
		}

		for _, group := range groups {
			names = append(names, group.Name)
		}
		result = names

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
