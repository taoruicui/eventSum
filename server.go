package eventsum

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"
	"github.com/ContextLogic/eventsum/log"
	"github.com/ContextLogic/eventsum/rules"
	conf "github.com/ContextLogic/eventsum/config"
	. "github.com/ContextLogic/eventsum/models"
	"github.com/ContextLogic/eventsum/metrics"
	"github.com/ContextLogic/eventsum/datastore"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

/* GLOBAL VARIABLES */
var globalRule rules.Rule

type EventSumServer struct {
	logger      *log.Logger
	route       *httprouter.Router
	httpHandler httpHandler
	port        string
}

func (s *EventSumServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
		s.logger.App.Printf("Listening on http://0.0.0.0%s", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			s.logger.App.Fatal(err)
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
	s.logger.App.Printf("Shutdown with timeout: %s", timeout)
	s.logger.App.Printf("Processing events still left in the queue")
	close(s.httpHandler.es.channel._queue)
	s.httpHandler.es.SummarizeBatchEvents()

	if err := hs.Shutdown(ctx); err != nil {
		s.logger.App.Errorf("Error: %v", err)
	} else {
		s.logger.App.Println("Server stopped")
	}
}

// User Defined configurable filters
func (s *EventSumServer) AddFilter(name string, filter func(EventData) (EventData, error)) error {
	if name == "" {
		return errors.New("Name must be a valid string")
	}
	return globalRule.AddFilter(name, filter)
}

// User Defined configurable groupings
func (s *EventSumServer) AddGrouping(name string, grouping func(EventData, map[string]interface{}) (map[string]interface{}, error)) error {
	if name == "" {
		return errors.New("Name must be a valid string")
	}
	return globalRule.AddGrouping(name, grouping)
}

// User Defined configurable groupings
func (s *EventSumServer) AddConsolidation(f func(map[string]interface{}, map[string]interface{}) (map[string]interface{}, error)) error {
	return globalRule.AddConsolidateFunc(f)
}

// Creates new HTTP Server given options.
// Options is a function which will be applied to the new Server
// Returns a pointer to Server
func newServer(options func(server *EventSumServer)) *EventSumServer {
	s := &EventSumServer{route: httprouter.New()}
	options(s)

	if s.logger == nil {
		s.logger = log.NewLogger("")
	}

	/* ROUTING */
	// GET requests
	s.route.GET("/", latency("/api/event/recent", s.httpHandler.recentEventsHandler))
	s.route.GET("/api/event/recent", s.httpHandler.recentEventsHandler)
	s.route.GET("/api/event/detail", s.httpHandler.detailsEventsHandler)
	s.route.GET("/api/event/histogram", s.httpHandler.histogramEventsHandler)
	s.route.Handler("GET", "/metrics", promhttp.Handler())

	// POST requests
	s.route.POST("/api/event/capture", s.httpHandler.captureEventsHandler)

	return s
}

func New(configFilename string) *EventSumServer {
	// Get configurations
	config, err := conf.ParseEventsumConfig(configFilename)
	if err != nil {
		panic(err)
	}

	globalRule = rules.NewRule()
	// global var globalRule should be available in packages as well
	log.GlobalRule = &globalRule
	datastore.GlobalRule = &globalRule

	logger := log.NewLogger(config.LogConfigFile)

	if err := metrics.RegisterPromMetrics(); err != nil {
		logger.App.Fatalf("Unable to register prometheus metrics: %v", err)
	}

	ds, err := datastore.NewDataStore(config.DataSourceInstance, config.DataSourceSchema)
	if err != nil {
		logger.App.Fatal(err)
	}
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
