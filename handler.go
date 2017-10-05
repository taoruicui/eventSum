package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"errors"
	"log"
	"strconv"
	"time"
)

// Base class for handling HTTP Requests
type httpHandler struct {
	es *ExceptionStore
	log *log.Logger
}

type ExceptionDetailsResult struct {
	dateCreated time.Time
	dateUpdated time.Time
	exceptionType string
	message string
	count int
	function string
	path string
	stacktrace StackTrace
	data map[string]interface{}
}

// Writes an error to ResponseWriter
func (h *httpHandler) sendError(w http.ResponseWriter, code int, err error, message string) {
	errMsg := fmt.Sprintf("%s: %s", message, err.Error())
	h.log.Println(errMsg)
	w.WriteHeader(code)
	w.Write([]byte(errMsg))
}

func (h *httpHandler) sendResp(w http.ResponseWriter, key string, val string) {

}

func (h *httpHandler) recentExceptionsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Invalid request type", 405)
	}
}

func (h *httpHandler) detailsExceptionsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Invalid request type", 405)
	}
	query := r.URL.Query()
	exceptionId, err := strconv.Atoi(query.Get("exception_id"))
	if err != nil {
		h.sendError(w, http.StatusBadRequest, errors.New("Exception ID is missing or not an int"), "Error")
		return
	}
	exceptionDataId, err := strconv.Atoi(query.Get("exception_data_id"))
	if err != nil {
		h.sendError(w, http.StatusBadRequest, errors.New("Exception Data ID is missing or not an int"), "Error")
		return
	}

	// Query the DB
	var exceptionInstance []ExceptionInstance
	var exceptionData []ExceptionData
	exceptionInstance = append(exceptionInstance, ExceptionInstance{})
	exceptionData = append(exceptionData, ExceptionData{})
	exceptionInstance[0].Id = int64(exceptionId)
	exceptionData[0].Id = int64(exceptionDataId)

	h.es.ds.QueryExceptionInstances(exceptionInstance)
	h.es.ds.QueryExceptionData(exceptionData)
	h.es.ds.Query(ExceptionInstance{})
	exceptionPeriod, _ := h.es.ds.FindPeriods(exceptionInstance[0].Id, exceptionData[0].Id)
	h.log.Println(exceptionPeriod)
	// Process result
	response := ExceptionDetailsResult {}
	//	dateCreated: time.Now(),
	//	exceptionType: ,
	//	message: ,
	//	count: ,
	//	function: ,
	//	path: ,
	//	stacktrace: ,
	//	data: ,
	//}

	fmt.Println(exceptionInstance)
	js, _ := json.Marshal(response)
	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

func (h *httpHandler) histogramExceptionsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Invalid request type", 405)
	}
}

func (h *httpHandler) captureExceptionsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Invalid request type", 405)
	}

	var exc UnaddedException
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&exc); err != nil {
		h.sendError(w, http.StatusBadRequest, err, "Error decoding JSON event")
		return
	}
	// TODO: make sure we validate the unadded exception

	// Send to batching channel
	h.es.Send(exc)
}
