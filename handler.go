package main

import (
	"fmt"
	"net/http"
	"github.com/go-pg/pg"
)

// Base class for handling HTTP Requests
type httpHandler struct {
	db *pg.DB
}

func (h httpHandler) recentExceptionsHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hi there, I love %s!", r.URL.Path[1:])
	var exceptions []Exception
	err := h.db.Model(&exceptions).Select()
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
