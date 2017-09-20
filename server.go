package main

import (
	"github.com/go-pg/pg"
	"net/http"
)

type Config struct {
	port string
}

// Create a connection to Postgres Database
func connectDB(conf EMConfig) *pg.DB {

	db := pg.Connect(&pg.Options{
		Addr:     conf.Address,
		User:     conf.User,
		Password: conf.Pass,
		Database: conf.Database,
	})
	return db
}

func main() {
	// Get configurations
	config, err := ParseEMConfig("default.json")
	if err != nil {
		panic(err)
	}

	db := connectDB(config)
	h := httpHandler{
		db,
		make(chan UnaddedException, config.BatchSize),
		config.BatchSize,
	}

	/* ROUTING */
	// GET requests
	http.HandleFunc("/", h.recentExceptionsHandler)
	http.HandleFunc("/api/exceptions/recent", h.recentExceptionsHandler)
	http.HandleFunc("api/exceptions/details", h.detailsExceptionsHandler)
	http.HandleFunc("api/exceptions/histogram", h.histogramExceptionsHandler)

	// POST requests
	http.HandleFunc("/api/exceptions/capture", h.captureExceptionsHandler)

	http.ListenAndServe(":8080", nil)
}
