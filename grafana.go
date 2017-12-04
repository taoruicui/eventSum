package eventsum

import (
	"github.com/julienschmidt/httprouter"
	"net/http"
	"encoding/json"
	"time"
	"fmt"
	"strconv"
	"strings"
)

// request struct for grafana searches
type searchReq struct {
	Target string `json:"target"`
}

// request struct for grafana queries
type queryReq struct {
	PanelId int `json:"panel_id"`
	Range timeRange `json:"range"`
	Interval int `json:"intervalMs"`
	Targets []targets `json:"targets"`
	MaxDataPoints int `json:"maxDataPoints"`
}

type queryResp struct {
	Target string `json:"target"`
	Datapoints [][]int `json:"datapoints"`
}

// Range specifies the time range the request is valid for.
type timeRange struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

type targets struct {
	Target string `json:"target"`
	RefId string `json:"refId"`
}

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
// 1) Individual Event metrics
// 2) Top n recent events and their metrics
// 3) Top n new/increased events and their metrics
func (h *httpHandler) grafanaQuery(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	var query queryReq
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)

	if err := decoder.Decode(&query); err != nil {
		h.sendError(w, http.StatusBadRequest, err, "Incorrect request format")
	}

	var result []queryResp
	for _, target := range query.Targets {
		base_id, err := strconv.Atoi(target.Target)
		// target is base_id
		if err == nil {
			// TODO: limit datapoints to maxDataPoints
			evt, err := h.es.GetEventHistogram(query.Range.From, query.Range.To, base_id)
			if err != nil {
				h.sendError(w, http.StatusInternalServerError, err, "query error")
			}

			datapoints := [][]int{}
			for _, bin := range evt.Datapoints {
				datapoints = append(datapoints, []int{bin.Count, bin.Start})
			}

			result = append(result, queryResp{
				Target: evt.FormatName(),
				Datapoints: datapoints,
			})
		// target is "recent.<service_id>"
		} else if strings.Contains(target.Target, "recent") {
			// TODO: limit datapoints to maxDataPoints
			service_id, err := strconv.Atoi(strings.Split(target.Target,".")[1])
			evts, err := h.es.GetRecentEvents(query.Range.From, query.Range.To, service_id, 5)
			if err != nil {
				h.sendError(w, http.StatusInternalServerError, err, "query error")
			}

			for _, evt := range evts {
				datapoints := [][]int{}
				bins := evt.Datapoints.ToSlice()
				for _, bin := range bins {
					datapoints = append(datapoints, []int{bin.Count, bin.Start})
				}
				result = append(result, queryResp{
					Target: evt.FormatName(),
					Datapoints: datapoints,
				})
			}
		// target is "increased.<service_id>"
		} else if strings.Contains(target.Target, "increased") {
			// TODO: limit datapoints to maxDataPoints
			// TODO: panic on strings.split
			service_id, err := strconv.Atoi(strings.Split(target.Target,".")[1])
			evts, err := h.es.GetRecentEvents(query.Range.From, query.Range.To, service_id, 5)
			if err != nil {
				h.sendError(w, http.StatusInternalServerError, err, "query error")
			}

			for _, evt := range evts {
				datapoints := [][]int{}
				bins := evt.Datapoints.ToSlice()
				for _, bin := range bins {
					datapoints = append(datapoints, []int{bin.Count, bin.Start})
				}
				result = append(result, queryResp{
					Target: evt.FormatName(),
					Datapoints: datapoints,
				})
			}
		} else {
			h.sendError(w, http.StatusBadRequest, err, "Invalid request")
		}
	}

	if err := json.NewEncoder(w).Encode(result); err != nil {
		fmt.Printf("json encode failure: %+v", err)
	}
}

// grafanaSearch handles the searching of service ids and event ids for grafana template variables
func (h *httpHandler) grafanaSearch(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	var search searchReq
	var result []int
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