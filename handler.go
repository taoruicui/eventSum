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
	DateCreated time.Time `json:"date_created"`
	DateUpdated time.Time`json:"date_updated"`
	ExceptionType string `json:"exception_type"`
	Message string `json:"message"`
	Function string `json:"function"`
	Path string `json:"path"`
	Stacktrace StackTrace `json:"stacktrace"`
	Data map[string]interface{} `json:"data"`
}

// Writes an error to ResponseWriter
func (h *httpHandler) sendError(w http.ResponseWriter, code int, err error, message string) {
	errMsg := fmt.Sprintf("%s: %s", message, err.Error())
	h.log.Println(errMsg)
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
	instance := ExceptionInstance{}
	data := ExceptionData{}
	instance.Id = int64(exceptionId)
	data.Id = int64(exceptionDataId)
	var stack StackTrace
	var args map[string]interface{}

	instance, _ = h.es.ds.QueryExceptionInstances(instance)
	data, _ = h.es.ds.QueryExceptionData(data)
	json.Unmarshal([]byte(instance.RawStack), &stack)
	json.Unmarshal([]byte(data.RawData), &args)

	// Process result
	response := ExceptionDetailsResult {
		DateCreated: time.Now(),
		ExceptionType: stack.Type,
		Message: GenerateFullStack(stack),
		Function: stack.Value,
		Path: stack.Module,
		Stacktrace: stack,
		Data: args,
	}
	h.sendResp(w, "exception_details", response)
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
