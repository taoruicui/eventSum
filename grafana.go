package eventsum

import (
	"encoding/json"
	"fmt"
	. "github.com/ContextLogic/eventsum/models"
	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"
	"net/http"
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
	groupNameMap := make(map[string]int)
	serviceNameMap := h.es.ds.GetServicesMap()
	environmentNameMap := h.es.ds.GetEnvironmentsMap()
	groups, _ := h.es.ds.GetGroups()
	for _, group := range groups {
		groupNameMap[group.Name] = group.Id
	}

	for _, target := range query.Targets {
		// create the maps that are needed later
		eventBaseMap := make(map[int]bool)
		groupIdMap := make(map[int]bool)
		serviceIdMap := make(map[int]bool)
		envIdMap := make(map[int]bool)

		for _, v := range target.Target.EventBaseId {
			eventBaseMap[v] = true
		}

		for _, v := range target.Target.GroupName {
			groupIdMap[groupNameMap[v]] = true
		}

		for _, v := range target.Target.ServiceName {
			serviceIdMap[serviceNameMap[v].Id] = true
		}

		for _, v := range target.Target.EnvironmentName {
			envIdMap[environmentNameMap[v].Id] = true
		}

		evts, err := h.es.GeneralQuery(
			query.Range.From,
			query.Range.To,
			groupIdMap,
			eventBaseMap,
			serviceIdMap,
			envIdMap)

		if err != nil {
			h.sendError(w, http.StatusInternalServerError, err, "query error")
			return
		}

		// Sort the events
		if target.Target.Sort == "recent" {
			evts = evts.SortRecent()
		} else if target.Target.Sort == "increased" {
			// TODO: implement increased
			evts = evts.SortIncreased()
			evts = evts.SortRecent()
		}

		if len(evts) > target.Target.Limit && target.Target.Limit > 0 {
			evts = evts[:target.Target.Limit]
		}

		for _, evt := range evts {
			datapoints := [][]int{}
			for _, bin := range evt.Datapoints {
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
		services := h.es.ds.GetServices()
		names := []string{}

		for _, service := range services {
			names = append(names, service.Name)
		}
		result = names

	case "environment":
		environments := h.es.ds.GetEnvironments()
		names := []string{}

		for _, service := range environments {
			names = append(names, service.Name)
		}
		result = names

	case "group_name":
		groups, err := h.es.ds.GetGroups()
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
		services := h.es.ds.GetServicesMap()
		service, ok := services[search.Target]

		if !ok {
			h.sendError(w, http.StatusBadRequest, errors.New("service name does not exist"), "")
		}

		events, err := h.es.ds.GetEvents()
		ids := []int{}

		if err != nil {
			h.sendError(w, http.StatusInternalServerError, err, "eventstore error")
		}

		for _, base := range events {
			if base.ServiceId == service.Id {
				ids = append(ids, base.Id)
			}
		}
		result = ids
	}

	if err := json.NewEncoder(w).Encode(result); err != nil {
		fmt.Printf("json encode failure: %+v", err)
	}
}
