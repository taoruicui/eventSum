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
)

type Server struct {
	logger      *log.Logger
	route       *httprouter.Router
	httpHandler httpHandler
	port        string
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "example Go server")
	s.route.ServeHTTP(w, r)
}

func (s *Server) Start() {
	httpServer := &http.Server{
		Addr:    s.port,
		Handler: s,
	}
	// run queue in a goroutine
	go s.httpHandler.es.Start()

	// run the server in a goroutine
	go func() {
		s.logger.Printf("Listening on http://0.0.0.0%s\n", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			s.logger.Fatal(err)
		}
	}()

	s.Graceful(httpServer, 5*time.Second)
}

func (s *Server) Stop() {

}

func (s *Server) AddFilter(name string, filter func(data EventData) EventData) {

}

// Creates new HTTP Server given options.
// Options is a function which will be applied to the new Server
// Returns a pointer to Server
func newServer(options func(server *Server)) *Server {
	s := &Server{route: httprouter.New()}
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
func (s *Server) Graceful(hs *http.Server, timeout time.Duration) {
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

func New(configFilename string) *Server {
	// Get configurations
	config, err := ParseEMConfig(configFilename)
	if err != nil {
		panic(err)
	}

	logger := log.New(os.Stdout, "", log.Lshortfile)
	ds := newDataStore(config, logger)
	es := newEventStore(ds, config, logger)

	// create new http server
	return newServer(func(s *Server) {
		s.logger = logger
		s.httpHandler = httpHandler{
			es,
			logger,
		}
		s.port = ":" + strconv.Itoa(config.ServerPort)
	})
}