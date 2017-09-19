package worker

import (
	"fmt"
	"time"
	td_client "github.com/treasure-data/td-client-go"
)

// TD Base Class
type TDBaseHandler struct {
	db *td_client.TDClient
	database string
}

// Runs query on TD and returns result
func (td TDBaseHandler) NewTdWorker(query string, startTime, endTime time.Time) {
	fmt.Println(query)
	query = fmt.Sprintf(query, startTime.Unix(), endTime.Unix())
	jobId, err := td.db.SubmitQuery(td.database, td_client.Query{
		Type:        "presto",
		Query:       query,
		ResultUrl:   "",
		Priority:    0,
		RetryLimit:  0,
	})

	if err != nil {
		fmt.Println("There was an error with the query: ", err)
	}
	for {
		status, err := td.db.JobStatus(jobId)
		if err != nil {
			fmt.Println("There was an error processing the query:", err)
		}
		if status != "queued" && status != "running" { break }
		time.Sleep(time.Second)
	}
	err = td.db.JobResultEach(jobId, func(v interface{}) error {
		fmt.Printf("Result:%v\n", v)
		return nil
	})
}

// Main function
func main() {
	fmt.Println("Running TD Worker...")
	client, err := td_client.NewTDClient(td_client.Settings {
		ApiKey: "13/0698382685dac718e336015b9e17b0f38e90176a",
	})

	if err != nil {
		panic(err)
	}
	td := TDBaseHandler{client, "sweeper"}
	endTime := time.Now()
	startTime := endTime.Add(-time.Hour)

	// create goroutine for each different query type
	for _, query := range QueryMap {
		go td.NewTdWorker(query, startTime, endTime)
	}
}
