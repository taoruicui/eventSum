package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/julienschmidt/httprouter"
)

type DigestServer struct {
	logger      *log.Logger
	route       *httprouter.Router
	httpHandler httpHandler
	port        string
}

func (s *DigestServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "example Go server")
	s.route.ServeHTTP(w, r)
}

// Creates new HTTP Server given options.
// Options is a function which will be applied to the new DigestServer
// Returns a pointer to DigestServer
func newDigestServer(options func(server *DigestServer)) *DigestServer {
	s := &DigestServer{route: httprouter.New()}
	options(s)

	if s.logger == nil {
		s.logger = log.New(os.Stdout, "", log.Lshortfile)
	}

	/* ROUTING */
	// GET requests
	s.route.GET("/", s.httpHandler.recentEventsHandler)
	s.route.GET("/api/event/recent", s.httpHandler.recentEventsHandler)
	s.route.GET("/api/event/detail", s.httpHandler.detailsEventsHandler)
	s.route.GET("/api/event/histogram", s.httpHandler.histogramEventsHandler)

	// POST requests
	s.route.POST("/api/event/capture", s.httpHandler.captureEventsHandler)

	return s
}

// Graceful shutdown of the server
func graceful(hs *http.Server, es *DigestServer, logger *log.Logger, timeout time.Duration) {
	// listen for termination signal
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// make sure we process events inside the queue
	es.httpHandler.es.Stop()
	logger.Printf("\nShutdown with timeout: %s\n", timeout)
	logger.Printf("\nProcessing events still left in the queue")
	close(es.httpHandler.es.channel._queue)
	es.httpHandler.es.SummarizeBatchEvents()

	if err := hs.Shutdown(ctx); err != nil {
		logger.Printf("Error: %v\n", err)
	} else {
		logger.Println("Server stopped")
	}
}

func main() {
	// Get configurations
	config, err := ParseEMConfig("config/default.json")
	if err != nil {
		panic(err)
	}

	logger := log.New(os.Stdout, "", log.Lshortfile)
	ds := newDataStore(config, logger)
	es := newEventStore(ds, config, logger)

	// create new http server
	digestServer := newDigestServer(func(s *DigestServer) {
		s.logger = logger
		s.httpHandler = httpHandler{
			es,
			logger,
		}
		s.port = ":" + strconv.Itoa(config.ServerPort)
	})

	httpServer := &http.Server{
		Addr:    digestServer.port,
		Handler: digestServer,
	}

	// run queue in a goroutine
	go es.Start()

	// run the server in a goroutine
	go func() {
		logger.Printf("Listening on http://0.0.0.0%s\n", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			logger.Fatal(err)
		}
	}()

	graceful(httpServer, digestServer, logger, 5*time.Second)
}
