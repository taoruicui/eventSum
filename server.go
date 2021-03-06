package eventsum

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"fmt"

	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	conf "github.com/ContextLogic/eventsum/config"
	"github.com/ContextLogic/eventsum/datastore"
	"github.com/ContextLogic/eventsum/log"
	"github.com/ContextLogic/eventsum/metrics"
	. "github.com/ContextLogic/eventsum/models"
	"github.com/ContextLogic/eventsum/rules"
	"github.com/ContextLogic/eventsum/util"
)

/* GLOBAL VARIABLES */
var (
	globalRule rules.Rule
)

type EventsumServer struct {
	logger      *log.Logger
	route       *httprouter.Router
	httpHandler httpHandler
	port        string
	config      conf.EventsumConfig
	stopped     IsStopped
}

type IsStopped struct {
	sync.Mutex
	value bool
}

func (is *IsStopped) stop() {
	is.Lock()
	defer is.Unlock()

	is.value = true
}

func (is *IsStopped) start() {
	is.Lock()
	defer is.Unlock()

	is.value = false
}

func (s *EventsumServer) isStopped() bool {
	s.stopped.Lock()
	defer s.stopped.Unlock()
	return s.stopped.value
}

func (s *EventsumServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.route.ServeHTTP(w, r)
}

// Starts the evensum server
func (s *EventsumServer) Start() {

	httpServer := &http.Server{
		Addr:    s.port,
		Handler: s,
	}

	s.stopped.start()

	// run queue in a goroutine
	go s.httpHandler.es.Start()

	// run the store in a goroutine
	go func() {
		s.logger.App().Printf("Listening on http://0.0.0.0%s", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			s.logger.App().Fatal(err)
		}
	}()

	s.Stop(httpServer, 5*time.Second)
}

// Graceful shutdown of the store
func (s *EventsumServer) Stop(hs *http.Server, timeout time.Duration) {
	// listen for termination signal
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	s.stopped.stop()

	if s.config.DrainSecond > 0 {
		s.logger.App().Println("Failing the LB health check...")
		timer := time.NewTimer(time.Second * time.Duration(s.config.DrainSecond))
		<-timer.C
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// make sure we process events inside the queue
	s.httpHandler.es.Stop()
	s.logger.App().Printf("Shutdown with timeout: %s", timeout)
	s.logger.App().Printf("Processing events still left in the queue")
	close(s.httpHandler.es.channel.queue)
	s.httpHandler.es.SummarizeBatchEvents()

	if err := hs.Shutdown(ctx); err != nil {
		s.logger.App().Errorf("Error: %v", err)
	} else {
		s.logger.App().Println("Server stopped")
	}
}

// User Defined configurable filters
func (s *EventsumServer) AddFilter(name string, filter func(EventData) (EventData, error)) error {
	if name == "" {
		return errors.New("Name must be a valid string")
	}
	return globalRule.AddFilter(name, filter)
}

// User Defined configurable groupings
func (s *EventsumServer) AddGrouping(name string, grouping func(EventData, map[string]interface{}) (map[string]interface{}, error)) error {
	if name == "" {
		return errors.New("Name must be a valid string")
	}
	return globalRule.AddGrouping(name, grouping)
}

// User Defined configurable groupings
func (s *EventsumServer) AddConsolidation(f func(map[string]interface{}, map[string]interface{}) (map[string]interface{}, error)) error {
	return globalRule.AddConsolidateFunc(f)
}

// Creates new HTTP Server given options.
// Options is a function which will be applied to the new Server
// Returns a pointer to Server
func newServer(options func(server *EventsumServer)) *EventsumServer {
	s := &EventsumServer{route: httprouter.New()}
	options(s)

	/* ROUTING */
	// GET requests
	s.route.GET("/", latency("/", func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

		s.httpHandler.sendResp(w, "message", "To visit EventSum UI, please go to https://opsdb.prod.wish.com/eventsum")

	}))
	s.route.GET("/search", latency("/search", s.httpHandler.searchEventsHandler))
	s.route.GET("/detail", latency("/detail", s.httpHandler.detailsEventsHandler))
	s.route.GET("/histogram", latency("/histogram", s.httpHandler.histogramEventsHandler))
	s.route.GET("/test", latency("/test", s.httpHandler.test))
	s.route.GET("/health", latency("/health", func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

		if s.isStopped() {
			s.httpHandler.sendError(w, 400, errors.New("Service shutting down"), "pod is shutting down")
			return
		}

		if errMap := s.healthCheck(); errMap != nil {
			s.httpHandler.sendError(w, 500, errors.New("Internal Service Error"), fmt.Sprintf("%v", errMap))
		} else {
			s.httpHandler.sendResp(w, "errors", errMap)
		}

	}))
	s.route.GET("/status", latency("/status", func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

		if s.isStopped() {
			s.httpHandler.sendError(w, 400, errors.New("Service shutting down"), "pod is shutting down")
			return
		}

		if errMap := s.healthCheck(); errMap != nil {
			s.httpHandler.sendError(w, 500, errors.New("Internal Service Error"), fmt.Sprintf("%v", errMap))
		} else {
			s.httpHandler.sendResp(w, "errors", errMap)
		}

	}))
	s.route.GET("/group", latency("/group", s.httpHandler.searchGroupHandler))
	s.route.GET("/count", latency("/count", s.httpHandler.countEventsHandler))
	s.route.GET("/opsdb", latency("/opsdb", s.httpHandler.opsdbEventsHandler))
	s.route.Handler("GET", "/metrics", promhttp.Handler())

	s.route.GET("/types/env", latency("/types/env", s.httpHandler.envTypesHandler))
	s.route.GET("/types/tier", latency("/types/tier", s.httpHandler.tierTypesHandler))
	s.route.GET("/types/group", latency("/types/group", s.httpHandler.groupTypesHandler))
	s.route.GET("/types/region", latency("/types/region", s.httpHandler.regionTypesHandler))

	s.route.GET("/recent_exceptions", latency("/recent_exceptions", s.httpHandler.recentExceptionHandler))

	// PUT requests
	s.route.PUT("/group", latency("/group", s.httpHandler.modifyGroupHandler))

	// POST requests
	s.route.POST("/capture", latency("/capture", s.httpHandler.captureEventsHandler))
	s.route.POST("/assign_group", latency("/assign_group", s.httpHandler.assignGroupHandler))
	s.route.POST("/group", latency("/group", s.httpHandler.createGroupHandler))
	s.route.POST("/db_cpu_alert", latency("/db_cpu_alert", s.httpHandler.cpuAlertHandler))
	s.route.POST("/server_cpu_alert", latency("/server_cpu_alert", s.httpHandler.diskAlertHandler))

	// DELETE requests
	s.route.DELETE("/group", latency("/group", s.httpHandler.deleteGroupHandler))

	// Grafana endpoints
	s.route.GET("/grafana", latency("/grafana", cors(s.httpHandler.grafanaOk)))
	s.route.GET("/grafana/", latency("/grafana/", cors(s.httpHandler.grafanaOk)))
	s.route.OPTIONS("/grafana/:route", latency("/grafana", cors(s.httpHandler.grafanaOk)))
	s.route.POST("/grafana/query", latency("/grafana/query", cors(s.httpHandler.grafanaTest)))
	s.route.POST("/grafana/search", latency("/grafana/search", cors(s.httpHandler.grafanaSearch)))

	return s
}

func New(flag conf.Flags) *EventsumServer {
	// Get configurations
	config, err := conf.ParseEventsumConfig(flag.ConfigFile, flag.Region)
	if err != nil {
		panic(err)
	}

	globalRule = rules.NewRule()
	// global var globalRule should be available in packages as well
	log.GlobalRule = &globalRule
	datastore.GlobalRule = &globalRule

	ds, err := datastore.NewDataStore(config)

	if err != nil {
		panic(err)
	}

	logger := log.NewLogger(config.LogConfigFile, ds)

	if err := metrics.RegisterPromMetrics(config.DatabaseName); err != nil {
		logger.App().Fatalf("Unable to register prometheus metrics: %v", err)
	}

	es := newEventStore(ds, config, logger)

	// create new http store
	return newServer(func(s *EventsumServer) {
		s.logger = logger
		s.httpHandler = newHTTPHandler(es, logger, config.TimeFormat)
		s.port = ":" + strconv.Itoa(config.ServerPort)
		s.config = config
	})
}

func (s *EventsumServer) healthCheck() map[string]interface{} {

	pgString := fmt.Sprintf("%s dbname=%s", s.httpHandler.es.ds.GetDBConfig().StorageConfig["pg_string"], s.config.DatabaseName)
	//errs := make(map[string]interface{})

	dbErr := util.DBHealthCheck(pgString)

	serviceErr := util.ServiceHealthCheck()

	if dbErr != nil || serviceErr != nil {
		return map[string]interface{}{"DBError": dbErr, "ServiceError": serviceErr}
	} else {
		return nil
	}
}
