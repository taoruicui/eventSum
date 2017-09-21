package main

import (
	"github.com/go-pg/pg"
	"net/http"
	"time"
	"log"
	"os"
	"strconv"
	"os/signal"
	"syscall"
	"context"
)

type ExceptionServer struct {
	logger *log.Logger
	route *http.ServeMux
	httpHandler httpHandler
	port string
}

func (s *ExceptionServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "example Go server")
	s.route.ServeHTTP(w, r)
}

// Create a connection to Postgres Database
func connectDB(conf EMConfig) *pg.DB {

	db := pg.Connect(&pg.Options{
		Addr:     conf.PgAddress,
		User:     conf.PgUsername,
		Password: conf.PgPassword,
		Database: conf.PgDatabase,
	})
	return db
}

// Creates new HTTP Server given options.
// Options is a function which will be applied to the new ExceptionServer
// Returns a pointer to ExceptionServer
func newExceptionServer(options func(server *ExceptionServer)) *ExceptionServer {
	s := &ExceptionServer{route: http.NewServeMux()}
	options(s)

	if s.logger == nil {
		s.logger = log.New(os.Stdout, "", 0)
	}

	/* ROUTING */
	// GET requests
	s.route.HandleFunc("/", s.httpHandler.recentExceptionsHandler)
	s.route.HandleFunc("/api/exceptions/recent", s.httpHandler.recentExceptionsHandler)
	s.route.HandleFunc("api/exceptions/details", s.httpHandler.detailsExceptionsHandler)
	s.route.HandleFunc("api/exceptions/histogram", s.httpHandler.histogramExceptionsHandler)

	// POST requests
	s.route.HandleFunc("/api/exceptions/capture", s.httpHandler.captureExceptionsHandler)

	return s
}

// Graceful shutdown of the server
func graceful(hs *http.Server, es *ExceptionServer, logger *log.Logger, timeout time.Duration) {
	// listen for termination signal
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// make sure we process exceptions inside the queue
	logger.Printf("\nShutdown with timeout: %s\n", timeout)
	logger.Printf("\nProcessing exceptions still left in the queue")
	close(es.httpHandler.Channel._queue)
	es.httpHandler.Channel.ProcessBatchException()

	if err := hs.Shutdown(ctx); err != nil {
		logger.Printf("Error: %v\n", err)
	} else {
		logger.Println("Server stopped")
	}
}

func main() {
	// Get configurations
	config, err := ParseEMConfig("default.json")
	if err != nil {
		panic(err)
	}

	logger := log.New(os.Stdout, "", 0)
	db := connectDB(config)

	// create new http server
	exceptionServer := newExceptionServer(func(s *ExceptionServer) {
		s.logger = logger
		s.httpHandler = httpHandler{
			db,
			&ExceptionChannel{
				make(chan UnaddedException, config.BatchSize),
				config.BatchSize,
				config.TimeLimit,
				time.Now(),
			},
		}
		s.port = ":" + strconv.Itoa(config.ServerPort)
	})
	httpServer := &http.Server{
		Addr: exceptionServer.port,
		Handler: exceptionServer,
	}

	// run the server in a goroutine
	go func() {
		logger.Printf("Listening on http://0.0.0.0%s\n", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			logger.Fatal(err)
		}
	}()

	graceful(httpServer, exceptionServer, logger, 5*time.Second)
}
