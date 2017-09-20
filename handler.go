package main

import (
	"encoding/json"
	"fmt"
	"github.com/go-pg/pg"
	"net/http"
)

// Base class for handling HTTP Requests
type httpHandler struct {
	Db *pg.DB
	ExceptionChannel chan UnaddedException
	BatchSize int 
}

// Writes an error to ResponseWriter
func (h *httpHandler) sendError(w http.ResponseWriter, code int, err error, message string, path string) {
	errMsg := fmt.Sprintf("%s: %s", message, err.Error())
	fmt.Println(errMsg)
	w.WriteHeader(code)
	w.Write([]byte(errMsg))
}

func (h httpHandler) recentExceptionsHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hi there, I love %s!", r.URL.Path[1:])
	var exceptions []Exception
	err := h.Db.Model(&exceptions).Select()
	if err != nil {
		panic(err)
	}
	fmt.Println(exceptions)
}

func (h httpHandler) detailsExceptionsHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hi there, I love %s!", r.URL.Path[1:])

}

func (h httpHandler) histogramExceptionsHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hi there, I love %s!", r.URL.Path[1:])

}

func (h httpHandler) captureExceptionsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Invalid request type", 405)
	}
	var exc UnaddedException
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&exc); err != nil {
		h.sendError(w, http.StatusBadRequest, err, "Error decoding JSON event", r.URL.Path)
		return
	}

	h.ExceptionChannel <- exc
	if len(h.ExceptionChannel) == h.BatchSize {
		close(h.ExceptionChannel)
		go ProcessBatchException(h.ExceptionChannel, h.BatchSize)
	}
	// Add to database
	//err := h.Db.Insert(exc)
	//if err != nil {
	//	h.sendError(w, http.StatusBadRequest, err, "Error Inserting into database", r.URL.Path)
	//	return
	//}
}
