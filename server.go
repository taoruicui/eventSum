package main

import (
	"net/http"
	"github.com/go-pg/pg"
)

type Config struct {
	port string
}

// Create a connection to Postgres Database
func connectDB() *pg.DB {

	EMConfig, err := ParseEMConfig("default.json")
	if err != nil {
		panic(err)
	}

	db := pg.Connect(&pg.Options{
		Addr: EMConfig.Address,
		User: EMConfig.User,
		Password: EMConfig.Pass,
		Database: EMConfig.Database,
	})
	return db
}

func main() {
	db := connectDB()
	h := httpHandler{db: db}

	// Routing
	http.HandleFunc("/", h.recentExceptionsHandler)
	http.HandleFunc("/api/exceptions/recent", h.recentExceptionsHandler)
	http.HandleFunc("api/exceptions/details", h.detailsExceptionsHandler)
	http.HandleFunc("api/exceptions/histogram", h.histogramExceptionsHandler)
	http.ListenAndServe(":8080", nil)
}