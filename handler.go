package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"log"
)

// Base class for handling HTTP Requests
type httpHandler struct {
	es *ExceptionStore
	log *log.Logger
}

// Writes an error to ResponseWriter
func (h *httpHandler) sendError(w http.ResponseWriter, code int, err error, message string) {
	errMsg := fmt.Sprintf("%s: %s", message, err.Error())
	h.log.Println(errMsg)
	w.WriteHeader(code)
	w.Write([]byte(errMsg))
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

	if str := query.Get("exception_id"); str == "" {
		//h.sendError(w, http.StatusBadRequest, error("Exception ID is missing"), "")
		return
	}
	fmt.Println(query)
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
