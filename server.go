package eventsum

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
	"github.com/pkg/errors"
)

type EventSumServer struct {
	logger      *log.Logger
	route       *httprouter.Router
	httpHandler httpHandler
	port        string
}

func (s *EventSumServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "example Go store")
	s.route.ServeHTTP(w, r)
}

// Starts the evensum server
func (s *EventSumServer) Start() {
	httpServer := &http.Server{
		Addr:    s.port,
		Handler: s,
	}
	// run queue in a goroutine
	go s.httpHandler.es.Start()

	// run the store in a goroutine
	go func() {
		s.logger.Printf("Listening on http://0.0.0.0%s\n", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			s.logger.Fatal(err)
		}
	}()

	s.Stop(httpServer, 5*time.Second)
}

// Graceful shutdown of the store
func (s *EventSumServer) Stop(hs *http.Server, timeout time.Duration) {
	// listen for termination signal
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// make sure we process events inside the queue
	s.httpHandler.es.Stop()
	s.logger.Printf("\nShutdown with timeout: %s\n", timeout)
	s.logger.Printf("\nProcessing events still left in the queue")
	close(s.httpHandler.es.channel._queue)
	s.httpHandler.es.SummarizeBatchEvents()

	if err := hs.Shutdown(ctx); err != nil {
		s.logger.Printf("Error: %v\n", err)
	} else {
		s.logger.Println("Server stopped")
	}
}

// User Defined configurable filters
func (s *EventSumServer) AddFilter(name string, filter func(EventData) (EventData, error)) error {
	if name == "" {
		return errors.New("Name must be a valid string")
	}
	return s.httpHandler.es.rule.addFilter(name, filter)
}

// User Defined configurable groupings
func (s *EventSumServer) AddGrouping(name string, grouping func(EventData, map[string]interface{}) map[string]interface{}) error {
	if name == "" {
		return errors.New("Name must be a valid string")
	}
	return s.httpHandler.es.rule.addGrouping(name, grouping)
}

// Creates new HTTP Server given options.
// Options is a function which will be applied to the new Server
// Returns a pointer to Server
func newServer(options func(server *EventSumServer)) *EventSumServer {
	s := &EventSumServer{route: httprouter.New()}
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

func New(configFilename string) *EventSumServer {
	// Get configurations
	config, err := parseEventsumConfig(configFilename)
	if err != nil {
		panic(err)
	}

	logger := log.New(os.Stdout, "", log.Lshortfile)
	ds := newDataStore(config, logger)
	es := newEventStore(ds, config, logger)

	// create new http store
	return newServer(func(s *EventSumServer) {
		s.logger = logger
		s.httpHandler = httpHandler{
			es,
			logger,
		}
		s.port = ":" + strconv.Itoa(config.ServerPort)
	})
}