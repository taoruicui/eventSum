package tests

import (
	"testing"
	"fmt"
	"time"
	"github.com/ContextLogic/eventsum"
)


func TestMain(m *testing.M){
	var e *eventsum.EventsumServer
	setup(e)
	m.Run()
	teardown(e)
}

func TestPostCapture(t *testing.T){

}

func TestPostCreateGroup(t *testing.T){

}

func TestPostAssignGroup(t *testing.T){

}

func TestGetSearch(t *testing.T) {

}

func TestGetDetail(t *testing.T){

}

func TestGetHistogram(t *testing.T){

}

func TestGetMetrics(t *testing.T){

}

func setup(e *eventsum.EventsumServer){
	fmt.Println("this is a setup")
	e = eventsum.New("./config/config.json")
	e.Start()

}

func teardown(e *eventsum.EventsumServer){
	fmt.Println("this is a tear down")
	e.Stop(5 * time.Second)
}


