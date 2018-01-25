package eventsum

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	_ "strings"

	"strconv"

	. "github.com/ContextLogic/eventsum/models"
	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"
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

	groups, err := h.es.ds.GetGroups()
	if err != nil {
		h.sendError(w, http.StatusInternalServerError, err, "Error in Querying Groups")
		return
	}
	for _, group := range groups {
		groupNameMap[group.Name] = group.Id
	}

	for _, target := range query.Targets {

		var groupIds, serviceIds, envIds []int

		for _, g := range target.Target.GroupName {
			groupIds = append(groupIds, groupNameMap[g])
		}
		for _, env := range target.Target.EnvironmentName {
			envIds = append(envIds, environmentNameMap[env].Id)
		}
		for _, serviceName := range target.Target.ServiceName {
			serviceIds = append(serviceIds, serviceNameMap[serviceName].Id)
		}

		evts, err := h.es.ds.MyGeneralQuery(query.Range.From, query.Range.To, groupIds,
			target.Target.EventBaseId, serviceIds, envIds, target.Target.EventName, target.Target.EventType)

		if err != nil {
			h.sendError(w, http.StatusInternalServerError, err, "query error")
			return
		}

		// Sort the events
		if target.Target.Sort == "recent" {
			evts = evts.SortRecent()
		} else if target.Target.Sort == "increased" {
			evts = evts.SortIncreased()
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
	var idFilterMatch = regexp.MustCompile("event_id\\([^,;]*,[^,;]*,[^,;]*,[^,;]*\\)")
	var eventTypeFilterMatch = regexp.MustCompile("(event_type=.*|event_type\\scontains\\s.*)")
	var eventNameFilterMatch = regexp.MustCompile("(event_name=.*|event_name\\scontains\\s.*)")

	if err != nil {
		h.sendError(w, http.StatusBadRequest, err, "Incorrect request format")
	}

	switch {
	// Get all service ids
	case search.Target == "service_name":
		services := h.es.ds.GetServices()
		names := []string{}

		for _, service := range services {
			names = append(names, service.Name)
		}
		result = names

	case search.Target == "environment":
		environments := h.es.ds.GetEnvironments()
		names := []string{}

		for _, service := range environments {
			names = append(names, service.Name)
		}
		result = names

	case search.Target == "group_name":
		groups, err := h.es.ds.GetGroups()
		names := []string{}

		if err != nil {
			h.sendError(w, http.StatusInternalServerError, err, "eventstore error")
		}

		for _, group := range groups {
			names = append(names, group.Name)
		}
		result = names

	case eventTypeFilterMatch.MatchString(search.Target):
		var statement = search.Target
		names, err := h.es.ds.GetEventTypes(statement)
		if err != nil {
			h.sendError(w, http.StatusInternalServerError, err, "event_type error")
		}
		result = names

	case eventNameFilterMatch.MatchString(search.Target):
		var statement = search.Target
		names, err := h.es.ds.GetEventNames(statement)
		if err != nil {
			h.sendError(w, http.StatusInternalServerError, err, "event_name error")
		}
		result = names

	case idFilterMatch.MatchString(search.Target):
		statement := strings.TrimPrefix(search.Target, "event_id(")
		statement = strings.TrimSuffix(statement, ")")
		criteria := strings.Split(statement, ",")
		var service, eventType, eventName, environment string
		var serviceId, environmentId string
		if criteria[0] != "" {
			service = criteria[0]
			services := h.es.ds.GetServicesMap()
			serviceIdMap, ok := services[service]
			serviceId = strconv.Itoa(serviceIdMap.Id)
			if !ok {
				h.sendError(w, http.StatusBadRequest, errors.New("service name does not exist"), "")
				return
			}
		}
		if criteria[1] != "" {
			eventType = criteria[1]
		}
		if criteria[2] != "" {
			eventName = criteria[2]
		}
		if criteria[3] != "" {
			environment = criteria[3]
			environments := h.es.ds.GetEnvironmentsMap()
			environmentIdMap, ok := environments[environment]
			environmentId = strconv.Itoa(environmentIdMap.Id)
			if !ok {
				h.sendError(w, http.StatusBadRequest, errors.New("service name does not exist"), "")
				return
			}
		}

		events, err := h.es.ds.GetEventsByCriteria(serviceId, eventType, eventName, environmentId)
		if err != nil {
			h.sendError(w, http.StatusBadRequest, errors.New("eventstore error"), "")
			return
		} else if len(events) == 0 {
			return
		}

		var ids []int

		for _, base := range events {
			ids = append(ids, base.Id)
		}
		result = ids

	default:
		return
	}

	if err := json.NewEncoder(w).Encode(result); err != nil {
		fmt.Printf("json encode failure: %+v", err)
	}

}
