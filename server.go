package main

import (
	"net/http"
	"github.com/go-pg/pg"
)

type Config struct {
	port string
}

func connectDB() *pg.DB {
	db := pg.Connect(&pg.Options{
		Addr: ,
		User: ,
		Password: ,
		Database: ,
	})
	return db
}

func main() {
	db := connectDB()
	h := httpHandler{db: db}

	http.HandleFunc("/", h.recentExceptionsHandler)
	http.HandleFunc("/api/exceptions/recent", h.recentExceptionsHandler)
	http.HandleFunc("api/exceptions/details", h.detailsExceptionsHandler)
	http.HandleFunc("api/exceptions/histogram", h.histogramExceptionsHandler)
	http.ListenAndServe(":8080", nil)
}